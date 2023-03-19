import copy
import multiprocessing
import os
import threading
import time
import random
import json

from kubernetes.client.rest import ApiException
from kubernetes import client, config
from utils_images import init_system_metadata, record_service_pull_time, delete_k8s_service, layer_list_get, \
    apply_k8s_service, shutil_remove_dirs
from queue import Queue as thr_queue



def update_msg_encapsulate(local_to_del_dict={}, hub_to_get_dict={},
                           bro_to_send_dict={}, bro_get_check={}):
    update_msg = {"local_to_del": local_to_del_dict,
                  "hub_to_get": hub_to_get_dict,
                  "bro_to_send": bro_to_send_dict,
                  "bro_get_check": bro_get_check,
                  }
    return update_msg


def changes_to_dict(my_dict, sync_dict, node_name, granularity, operation, basic_path):
    dir_path = os.path.join(basic_path, "distribution", node_name, granularity)
    if operation == "add":  # "add" only used in master which aims to change "freq"
        for item in sync_dict[granularity][operation]:
            if item in my_dict[node_name][granularity]:
                my_dict[node_name][granularity][item]["frq"] += 1
            else:
                my_dict[node_name][granularity][item] = {"frq": 1, "exit": 0, "size": -1}
    elif operation == "rm":  # "rm" is used to change "exit"
        for item in sync_dict[granularity][operation]:
            try:
                my_dict[node_name][granularity][item]["exit"] = 0
                os.rmdir(os.path.join(dir_path, item))
            except Exception as e:
                print("changes_to_dict miss key or ...", e)
    elif operation == "size_update":  # "rm" is used to change "exit" and "size"
        print(f"changes_to_dict | operation == 'size_update', sync_dict -> f{sync_dict}")
        for item in sync_dict[granularity][operation]:
            if item in my_dict[node_name][granularity]:
                my_dict[node_name][granularity][item]["exit"] = 1
                my_dict[node_name][granularity][item]["size"] = sync_dict[granularity][operation][item]
                try:
                    target_path = os.path.join(dir_path, item)
                    if not os.path.exists(target_path):
                        os.mkdir(target_path, mode=0o777)
                except Exception as e:
                    print("changes_to_dict with wrong:", e)
            else:
                raise

    return my_dict

def thread_pool_callback(worker):
    worker_exception = worker.exception()
    if worker_exception:
        print("error: {}".format(worker_exception))


class BoosterOrchestrationModule(object):

    def __init__(self, orchestrator_name="booster-orchestrator", host_master=os.getlogin(),
                 orchestrator_layer_cache=500):
        # TODO：read factor from YAML.
        self.load_config()
        self.v1 = client.CoreV1Api()
        self.cluster_images_name = None
        self.orchestrator_name = orchestrator_name
        self.orchestrator_layer_cache = orchestrator_layer_cache
        self.__host_node_name = host_master
        self.now_strategy = 1
        self.can_next_deploy = True
        self.__menu_basic_path = "/home/" + os.getlogin() + "/cluster_menu/"
        self.share_sync_lock = threading.Lock()  # TODO: share_sync_lock用于同步部署与编排
        self._dist_sync_lock = threading.Lock()
        self._changes_sync_queue = thr_queue(maxsize=100)
        self.service_delete_queue = thr_queue(maxsize=20)
        self.workload_queue = thr_queue(maxsize=100)
        self.__cluster_dist_dict = self.init_dist_in_memo()
        threading.Thread(target=self.init_apply_order).start()

    @property
    def host_name(self):
        return self.__host_node_name

    @property
    def menu_basic_path(self):
        return self.__menu_basic_path

    @property
    def cluster_dist_dict(self):
        return self.__cluster_dist_dict

    @property
    def layer_cache_size(self):
        return self.orchestrator_layer_cache

    def init_dist_in_memo(self):
        dist_dict = {}
        node_dir_path = os.path.join(self.__menu_basic_path, "distribution")
        for each_node in os.listdir(node_dir_path):
            dist_dict[each_node] = {"layer": {}, "file": {}, "image": {}}
            for layer in os.listdir(os.path.join(self.__menu_basic_path, "distribution", each_node, "layer")):
                dist_dict[each_node]["layer"][layer] = {"frq": 0, "exit": 1, "size": -1}
            for file in os.listdir(os.path.join(self.__menu_basic_path, "distribution", each_node, "file")):
                dist_dict[each_node]["file"][file] = {"frq": 0, "exit": 1, "size": -1}
            for image in os.listdir(os.path.join(self.__menu_basic_path, "distribution", each_node, "image")):
                dist_dict[each_node]["image"][image] = {"frq": 0, "exit": 1, "size": -1}
        return dist_dict

    @staticmethod
    def load_config():
        try:
            config.load_kube_config()
        except FileNotFoundError as e:
            print("WARNING %s\n" % e)
            config.load_incluster_config()

    def cycle_apply_once(self, apply=True, is_raw=True):
        version_list = {'python': '3.9.3', 'golang': '1.16.2', 'openjdk': '11.0.11-9-jdk', 'alpine': '3.13.4',
                        'ubuntu': 'focal-20210401', 'memcached': '1.6.8', 'nginx': '1.19.10', 'httpd': '2.4.43',
                        'mysql': '8.0.23', 'mariadb': '10.5.8', 'redis': '6.2.1', 'mongo': '4.0.23', 'postgres': '13.1',
                        'rabbitmq': '3.8.13', 'registry': '2.7.0', 'wordpress': 'php7.3-fpm', 'ghost': '3.42.5-alpine',
                        'node': '16-alpine3.11', 'flink': '1.12.3-scala_2.11-java8', 'cassandra': '3.11.9',
                        'eclipse-mosquitto': '2.0.9-openssl'}
        if apply:
            for app in version_list:
                image_name = app + ":" + version_list[app]
                if is_raw:
                    apply_k8s_service(image_name, "raw")
                else:
                    apply_k8s_service(image_name, "mine")
        else:
            for app in version_list:
                image_name = app + ":" + version_list[app]
                delete_k8s_service("", "", image_name)

    def cycle_apply_workload(self, apply=True, is_raw=True):
        version_list = {'python': '3.9.3', 'golang': '1.16.2', 'openjdk': '11.0.11-9-jdk', 'alpine': '3.13.4',
                        'ubuntu': 'focal-20210401', 'memcached': '1.6.8', 'nginx': '1.19.10', 'httpd': '2.4.43',
                        'mysql': '8.0.23', 'mariadb': '10.5.8', 'redis': '6.2.1', 'mongo': '4.0.23', 'postgres': '13.1',
                        'rabbitmq': '3.8.13', 'registry': '2.7.0', 'wordpress': 'php7.3-fpm', 'ghost': '3.42.5-alpine',
                        'node': '16-alpine3.11', 'flink': '1.12.3-scala_2.11-java8', 'cassandra': '3.11.9',
                        'eclipse-mosquitto': '2.0.9-openssl'}
        img_list_once = ['postgres', 'openjdk', 'httpd', 'python', 'golang', 'flink', 'redis', 'memcached']

        img_list = ['postgres', 'postgres', 'python', 'httpd', 'flink', 'openjdk', 'flink', 'openjdk', 'memcached',
                    'httpd', 'redis', 'redis', 'memcached', 'redis', 'golang', 'redis']
        if apply:
            for app in img_list:
                image_name = app + ":" + version_list[app]
                time.sleep(2.5)
                if is_raw:
                    while not self.can_next_deploy:
                        time.sleep(3)
                    apply_k8s_service(image_name, "raw")
                else:
                    while not self.can_next_deploy:
                        time.sleep(3)
                    apply_k8s_service(image_name, "mine")
        else:
            for app in img_list_once:
                image_name = app + ":" + version_list[app]
                delete_k8s_service("", "", image_name)



    def init_metadata_tree(self):
        version_list = {'python': '3.9.3', 'golang': '1.16.2', 'openjdk': '11.0.11-9-jdk', 'alpine': '3.13.4',
                        'ubuntu': 'focal-20210401', 'memcached': '1.6.8', 'nginx': '1.19.10', 'httpd': '2.4.43',
                        'mysql': '8.0.23', 'mariadb': '10.5.8', 'redis': '6.2.1', 'mongo': '4.0.23', 'postgres': '13.1',
                        'rabbitmq': '3.8.13', 'registry': '2.7.0', 'wordpress': 'php7.3-fpm', 'ghost': '3.42.5-alpine',
                        'node': '16-alpine3.11', 'flink': '1.12.3-scala_2.11-java8', 'cassandra': '3.11.9',
                        'eclipse-mosquitto': '2.0.9-openssl'}
        img_list = ['postgres', 'openjdk', 'httpd', 'python', 'golang', 'flink', 'redis', 'memcached']

        resgistry_name = "fengyicheng/"
        # resgistry_name = ""

        self.cluster_images_name = [resgistry_name + app + ":" + version_list[app] for app in img_list]
        # self.cluster_images_name = ["fengyicheng/offload_cloud:latest", "fengyicheng/openjdk:latest"]
        nodes_list = self.nodes_available()
        init_system_metadata(self.cluster_images_name, nodes_list, self.__menu_basic_path)
        # return 0

    def init_random_collocation(self):
        nodes_ip = self.get_nodes_ip_dict()

        nodes_ip = {"node71": "192.168.1.71", "node72": "192.168.1.72"}
        image_menu_path = self.__menu_basic_path + "image_metadata/"
        images_list = os.listdir(image_menu_path)
        alloc_layer_list = []

        tmp_digest_name_dict = {}
        for each_image in images_list:
            manifest_path = image_menu_path + each_image + '/manifest.json'
            with open(manifest_path, 'rb') as f:
                result = json.load(f)
            image_name = result[0].get('RepoTags')  # ['fengyicheng/offload_cloud:latest']
            print("RepoTags show -> ", image_name[0])
            layer_list = [i[:-10] for i in result[0].get('Layers')]
            random_choice_list = random.sample(layer_list, len(layer_list) // 3)
            alloc_layer_list += random_choice_list
            for layer_digest in random_choice_list:
                tmp_digest_name_dict[layer_digest] = image_name[0]
        # print("alloc_layer_list:", alloc_layer_list)
        index = 0
        num_mean = len(alloc_layer_list) // len(nodes_ip)
        from master_proxy_module import proxy_update_notifier
        for node in nodes_ip:
            if index == len(nodes_ip) + 1:
                my_list = alloc_layer_list[
                          index * num_mean:(index + 1) * num_mean + len(alloc_layer_list) % len(nodes_ip)]
            else:
                my_list = alloc_layer_list[index * num_mean:(index + 1) * num_mean]
            # print("mylist:", my_list)
            trans_pull_dict = {}
            # print("tmp_digest_name_dict:", tmp_digest_name_dict)
            for each_digest in my_list:
                trans_pull_dict[each_digest] = tmp_digest_name_dict[each_digest]
            # print("trans_pull_dict:", trans_pull_dict)
            update_msg = update_msg_encapsulate(hub_to_get_dict={"layer": trans_pull_dict, "file": []})
            # print("update_msg:", update_msg)
            proxy_update_notifier(nodes_ip[node], "9000", update_msg, 0)
            index += 1


    def init_daemons(self):
        changes_sync_daemons_thr = threading.Thread(target=self.changes_sync_daemons)
        changes_sync_daemons_thr.start()

        delete_service_daemons_thr = threading.Thread(target=self.delete_service_daemon)
        delete_service_daemons_thr.start()

        orchestration_daemon_thr = threading.Thread(target=self.orchestration_daemon)
        # orchestration_daemon_thr.start()

        # TODO: 补全其他daemons
        return changes_sync_daemons_thr

    def init_workload(self):
        dir_head = "/home/" + os.getlogin() + "/atc/yamls/raw"

        self.workload_queue.put()

    def get_nodes_ip_dict(self):
        nodes_ip_dict = {}
        for n in self.v1.list_node().items:
            if "master" not in n.metadata.name:
                nodes_ip_dict[n.metadata.name] = n.status.addresses[0].address
        return nodes_ip_dict

    def nodes_available(self):
        ready_nodes = []
        for n in self.v1.list_node().items:
            for status in n.status.conditions:
                if status.status == "True" and status.type == "Ready" and "master" not in n.metadata.name:
                    ready_nodes.append(n.metadata.name)
        return ready_nodes

    def get_node_from_name(self, node_name):
        return next(x for x in self.v1.list_node().items if x.metadata.name == node_name)

    def changes_sync_add(self, name_with_dict_list):
        self._changes_sync_queue.put(name_with_dict_list)

    def changes_sync_daemons(self):
        print("master -> changes_sync_daemons are working....")
        while True:
            tmp_obj = self._changes_sync_queue.get()
            node_name, sync_dict = tmp_obj[0], tmp_obj[1]
            for granularity in sync_dict:
                for operation in sync_dict[granularity]:
                    with self._dist_sync_lock:
                        self.__cluster_dist_dict = changes_to_dict(self.__cluster_dist_dict, sync_dict, node_name,
                                                                   granularity, operation, self.__menu_basic_path)
            print("__cluster_dist_dict:", self.__cluster_dist_dict)

    def _get_schedulable_node(self):
        layer_list = layer_list_get(os.path.join(self.__menu_basic_path, "image_metadata",
                                                              "6d891ca36d5f3fdc256f65ae4300195e46b6742e2f859936bfc53562cf3e73a8",
                                                              "manifest.json"))
        image_lack_lf = {"lack_lf": {"layer": [], "file": []},
                         "image_id": "6d891ca36d5f3fdc256f65ae4300195e46b6742e2f859936bfc53562cf3e73a8",
                         "image_name": "fengyicheng/offload_cloud:latest"}

        self.orchestrate_deploy_trigger("node71", image_lack_lf, [])


    def orchestrate_deploy_trigger(self, host_node, image_lack_lf, whole_layer_list_back):
        self.push_lf_local_info(host_node, image_lack_lf["lack_lf"], image_lack_lf["image_id"],
                                image_lack_lf["image_name"], whole_layer_list_back)

    def push_lf_local_info(self, schedule_node, lack_dict, image_id, image_name, whole_layer_list_back):
        from master_proxy_module import proxy_push_schedule_node_notifier
        from master_proxy_module import proxy_push_node_notifier

        nodes_ip_dict = self.get_nodes_ip_dict()
        final_push_dict = {}
        for node in nodes_ip_dict:
            if node != schedule_node:
                final_push_dict[node] = {"layer": [], "file": []}

        pre_layer_node_dict, pre_file_node_dict = {}, {}
        hub_need_lf = copy.deepcopy(lack_dict)

        with self.share_sync_lock:
            for layer in lack_dict["layer"]:
                # print(f"with self.share_sync_lock | layer -> {layer}")
                find_in_cluster = False
                with self._dist_sync_lock:
                    for node in self.__cluster_dist_dict:
                        if node == schedule_node:
                            continue
                        if layer in self.__cluster_dist_dict[node]["layer"]:
                            if self.__cluster_dist_dict[node]["layer"][layer]["exit"] == 1:
                                print(f"layer -> {layer} exit in node {node}")
                                if layer not in pre_layer_node_dict:
                                    pre_layer_node_dict[layer] = {}
                                pre_layer_node_dict[layer][node] = next(
                                    int(self.get_node_from_name(node).metadata.labels[x]) for x in
                                    self.get_node_from_name(node).metadata.labels.keys() if
                                    ('bandwidth_' + schedule_node) in x)
                                find_in_cluster = True
                if find_in_cluster:
                    select_node = max(pre_layer_node_dict[layer], key=pre_layer_node_dict[layer].get)
                    final_push_dict[select_node]["layer"].append(layer)
                    hub_need_lf["layer"].remove(layer)

            for file in lack_dict["file"]:
                find_in_cluster = False
                with self._dist_sync_lock:
                    for node in self.__cluster_dist_dict:
                        if node == schedule_node:
                            continue
                        if file in self.__cluster_dist_dict[node]["file"]:
                            if self.__cluster_dist_dict[node]["file"][file]["exit"] == 1:
                                if file not in pre_file_node_dict:
                                    pre_file_node_dict[file] = {}
                                pre_file_node_dict[file] = next(
                                    int(self.get_node_from_name(node).metadata.labels[x]) for x in
                                    self.get_node_from_name(node).metadata.labels.keys() if
                                    'bandwidth_' + schedule_node in x)
                                find_in_cluster = True

                if find_in_cluster:
                    select_node = max(pre_file_node_dict[file], key=pre_file_node_dict[file].get)
                    final_push_dict[select_node]["file"].append(file)
                    hub_need_lf["file"].remove(file)

            hub_to_get_dict = {"layer": {}, "file": {}}
            for each_layer in hub_need_lf["layer"]:
                sha_each_layer = "sha256:" + each_layer if "sha256:" not in each_layer else each_layer
                hub_to_get_dict["layer"][sha_each_layer] = image_name
            self.update_presence(schedule_node, lack_dict["layer"], image_id)

        metadata_path = self.__menu_basic_path + "image_metadata/" + image_id + "/"
        metadata_list = os.listdir(metadata_path)
        res_schedule = proxy_push_schedule_node_notifier(nodes_ip_dict[schedule_node], "9000", metadata_path,
                                                         metadata_list,
                                                         lack_dict, image_id, image_name, hub_to_get_dict)
        res_dict = {schedule_node: res_schedule}
        for node in final_push_dict:
            if final_push_dict[node]["layer"] or final_push_dict[node]["file"]:
                res_dict[node] = proxy_push_node_notifier(nodes_ip_dict[node], "9000", final_push_dict[node],
                                                          [nodes_ip_dict[schedule_node], "9000"], image_id, "deploy")
        return res_dict


    def update_popularity(self, host_node, whole_layer_list_back, image_id):
        for layer in whole_layer_list_back:
            with self._dist_sync_lock:
                if layer not in self.cluster_dist_dict[host_node]["layer"]:
                    self.cluster_dist_dict[host_node]["layer"][layer] = {"frq": 1, "exit": 0, "size": -1}
                else:
                    self.cluster_dist_dict[host_node]["layer"][layer]["frq"] += 1

        with self._dist_sync_lock:
            if image_id not in self.cluster_dist_dict[host_node]["image"]:
                self.cluster_dist_dict[host_node]["image"][image_id] = {"frq": 1, "exit": 0, "size": -1}
            else:
                self.cluster_dist_dict[host_node]["image"][image_id]["frq"] += 1
        print(f"update_popularity | self.cluster_dist_dict -> {self.cluster_dist_dict}")

    def update_presence(self, host_node, lack_list, image_id):
        sync_dict_layer = {"layer": {"add": lack_list}}
        sync_dict_image = {"image": {"add": [image_id]}}
        with self._dist_sync_lock:
            self.__cluster_dist_dict = changes_to_dict(self.__cluster_dist_dict, sync_dict_layer, host_node,
                                                       "layer", "add", self.__menu_basic_path)
            self.__cluster_dist_dict = changes_to_dict(self.__cluster_dist_dict, sync_dict_image, host_node,
                                                       "image", "add", self.__menu_basic_path)

        print(f"update_presence | self.cluster_dist_dict -> {self.__cluster_dist_dict}")


    def delete_service_add(self, dict_info):
        # {'from_node': from_node, 'image_id': image_id, 'image_name': image_name, 'pull_time': pull_time}
        self.service_delete_queue.put(dict_info)

    def delete_service_daemon(self):
        # {'from_node': from_node, 'image_id': image_id, 'image_name': image_name, 'pull_time': pull_time}
        while True:
            dict_info = self.service_delete_queue.get()
            dir_path = os.path.join(self.__menu_basic_path, "distribution", dict_info['from_node'], 'image')
            # 清除image信息
            with self._dist_sync_lock:
                try:
                    self.__cluster_dist_dict[dict_info['from_node']]["image"][dict_info['image_id']]["exit"] = 0
                    os.rmdir(os.path.join(dir_path, dict_info['image_id']))  # 删除分布数据库中文件夹
                except Exception as e:
                    print("delete_service_daemon | changes_to_dict miss key or ...", e)


            record_service_pull_time(node_name=dict_info['from_node'], image_id=dict_info['image_id'],
                                     image_name=dict_info['image_name'], pull_time=dict_info['pull_time'], hit_rate=dict_info['hit_rate'])
            delete_k8s_service(node_name=dict_info['from_node'], image_id=dict_info['image_id'],
                               image_name=dict_info['image_name'])
            self.can_next_deploy = True
