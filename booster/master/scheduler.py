#!/usr/bin/env python3
import os
import time
import random
import json

from kubernetes.client.rest import ApiException
from kubernetes import client, config


# from placeholder import Placeholder


class BoosterScheduler(object):

    def __init__(self, orchestration_module, scheduler_name="booster-scheduler"):
        self.load_config()
        self.v1 = client.CoreV1Api()
        self.scheduler_name = scheduler_name
        self.placeholders = []
        self.rescedules = dict()
        self.orchestration_module = orchestration_module

        # The two thresholds are used as bounds for the image score range. They correspond to a reasonable size range
        # for container images compressed and stored in registries; 90%ile of images on dockerhub drops into this range.
        self.minThreshold = 23  # mb
        self.maxThreshold = 1000   # mb

    @staticmethod
    def load_config():
        try:
            config.load_kube_config()
        except FileNotFoundError as e:
            print("WARNING %s\n" % e)
            config.load_incluster_config()

    def nodes_available(self):
        ready_nodes = []
        for n in self.v1.list_node().items:
            for status in n.status.conditions:
                if status.status == "True" and status.type == "Ready" and "node-role.kubernetes.io/master" \
                        not in n.metadata.labels:  # also need to filter the master node
                    ready_nodes.append(n.metadata.name)
        return ready_nodes

    def get_all_pods(self, kube_system=False):
        if not kube_system:
            return [x for x in self.v1.list_pod_for_all_namespaces(watch=False).items if
                    x.metadata.namespace != 'kube-system']
        else:
            return self.v1.list_pod_for_all_namespaces(watch=False).items

    def get_pod_by_name(self, name):
        return next(x for x in self.v1.list_pod_for_all_namespaces(watch=False).items if name in x.metadata.name)

    def get_pods_on_node(self, node_name, kube_system=False):
        if not kube_system:
            return [x for x in self.v1.list_pod_for_all_namespaces(watch=False).items if
                    x.metadata.namespace != 'kube-system' and x.spec.node_name == node_name]
        else:
            return [x for x in self.v1.list_pod_for_all_namespaces(watch=False).items if x.spec.node_name == node_name]

    @staticmethod
    def convert_to_int(resource_string, res_type):
        if res_type == 'cpu':
            # 1core = 1000m
            if 'm' in resource_string:
                return int(resource_string.split('m')[0])
            else:
                return int(resource_string) * 1000
        else:
            if 'Ki' in resource_string:
                return int(resource_string.split('K')[0]) * 1024
            elif 'Mi' in resource_string:
                return int(resource_string.split('M')[0]) * (1024 ** 2)
            elif 'Gi' in resource_string:
                return int(resource_string.split('G')[0]) * (1024 ** 3)
            else:
                # 'ephemeral-storage'(available) shows as '70332298329'...
                return int(resource_string) * 1024

    def calculate_available_memory(self, node):
        pods_on_node = self.get_pods_on_node(node.metadata.name)
        sum_reserved_memory = sum([self.get_pod_memory_request(y) for y in pods_on_node])
        allocatable_memory = self.convert_to_int(node.status.allocatable['memory'])
        # try:
        #     sum_reserved_memory += next(x.required_memory for x in self.placeholders if x.node == node.metadata.name)
        # except StopIteration:
        #     pass
        return allocatable_memory - sum_reserved_memory

    def calculate_available_res(self, node, res_type):
        pods_on_node = self.get_pods_on_node(node.metadata.name)
        sum_reserved_res = sum([self.get_pod_res_request(y, res_type) for y in pods_on_node])
        allocatable_res = self.convert_to_int(node.status.allocatable[res_type], res_type)

        return allocatable_res - sum_reserved_res

    def calculate_res_fraction(self, node, res_type):
        pods_on_node = self.get_pods_on_node(node.metadata.name)
        sum_reserved_res = sum([self.get_pod_res_request(y, res_type) for y in pods_on_node])
        capacity_res = self.convert_to_int(node.status.capacity[res_type], res_type)

        return round(sum_reserved_res / capacity_res, 3), sum_reserved_res, capacity_res

    def get_leastrequestedpriority_score(self, sum_reserved_cpu, capacity_cpu, sum_reserved_memory, capacity_memory,
                                         sum_reserved_disk, capacity_disk):
        return ((capacity_cpu - sum_reserved_cpu) * 10 / capacity_cpu) + (
                (capacity_memory - sum_reserved_memory) * 10 / capacity_memory) / 2 + (
                       (capacity_disk - sum_reserved_disk) * 10 / capacity_disk) / 10

    def get_balancedresourceallocation_score(self, cpuFraction, memoryFraction, diskFraction):
        return 10 - (abs(cpuFraction - memoryFraction) + abs(cpuFraction - diskFraction) + abs(
            memoryFraction - diskFraction) / 3) * 10

    def get_layerlocalitypriority_score(self, node, pod_image_name):

        image_menu_path = self.orchestration_module.menu_basic_path + "image_metadata/"
        images_list = os.listdir(image_menu_path)
        # Obtain specific image information from metadata according to the image name
        for each_image in images_list:
            manifest_path = image_menu_path + each_image + '/manifest.json'
            with open(manifest_path, 'rb') as f:
                result = json.load(f)
            # print(f"开始遍历元数据 | pod_image_name = {pod_image_name} | result[0].get('RepoTags') = {result[0].get('RepoTags')[0]}")
            if pod_image_name == result[0].get('RepoTags')[0]:
                print(f"get_layerlocalitypriority_score | match RepoTags -> {pod_image_name}")
                hit_cache_sum_size = 0
                image_id = each_image
                with self.orchestration_module._dist_sync_lock:
                    # if image_id in self.orchestration_module.cluster_dist_dict[node.metadata.name]["image"] and \
                    #         self.orchestration_module.cluster_dist_dict[node.metadata.name]["image"][image_id][
                    #             "exit"] == 1:
                    #     score, whole_layer_list = 10 * 1, None  # 镜像已经加载进入FS的情况
                    # else:
                    layer_list = [i[:-10] for i in result[0].get('Layers')]
                    layer_filter_list = [i[7:] for i in layer_list] if "sha" in layer_list[
                        0] else layer_list
                    # print(f"complete layer_list:{layer_list}, layer_filter_list: {layer_filter_list}")

                    # whole_layer_list = layer_filter_list
                    whole_layer_list_back = layer_filter_list.copy()
                    # tmp_len = len(layer_filter_list)
                    for layer in self.orchestration_module.cluster_dist_dict[node.metadata.name]["layer"]:
                        # 是需要的，且在缓存中
                        if layer in layer_filter_list and self.orchestration_module.cluster_dist_dict[node.metadata.name]["layer"][layer]["exit"] == 1:
                            # TODO: add spread factor
                            hit_cache_sum_size += self.orchestration_module.cluster_dist_dict[node.metadata.name]["layer"][layer]["size"]
                            layer_filter_list.remove(layer)
                    my_node_cache_now = 0
                    for layer in self.orchestration_module.cluster_dist_dict[node.metadata.name]["layer"]:
                        if self.orchestration_module.cluster_dist_dict[node.metadata.name]["layer"][layer]["exit"] == 1:
                            my_node_cache_now += self.orchestration_module.cluster_dist_dict[node.metadata.name]["layer"][layer]["size"]

                    if hit_cache_sum_size < self.minThreshold:
                        hit_cache_sum_size = self.minThreshold
                    elif hit_cache_sum_size > self.maxThreshold:
                        hit_cache_sum_size = self.maxThreshold
                    score = 10 * (hit_cache_sum_size - self.minThreshold) / (self.maxThreshold - self.minThreshold)

                    sum_score = score + 5 * (self.orchestration_module.orchestrator_layer_cache - my_node_cache_now) / self.orchestration_module.orchestrator_layer_cache

                return sum_score, {"image_name": pod_image_name, "image_id": image_id,
                               "lack_lf": {"layer": layer_filter_list, "file": []}}, whole_layer_list_back
        return -1, None, None


    def get_cloud_node(self):
        return next(x for x in self.v1.list_node().items if
                    'kubernetes.io/role' in x.metadata.labels.keys() and
                    x.metadata.labels['kubernetes.io/role'] == 'cloud')

    def get_node_from_name(self, node_name):
        return next(x for x in self.v1.list_node().items if x.metadata.name == node_name)

    def get_node_from_podname_or_nodename(self, previous_element_name):
        if previous_element_name in [x.metadata.name for x in self.v1.list_node().items]:
            return self.get_node_from_name(previous_element_name)
        else:
            return self.get_node_from_name(next(x for x in self.v1.list_pod_for_all_namespaces(watch=False).items if
                                                previous_element_name in x.metadata.name).spec.node_name)

    def get_nodes_in_radius(self, required_delay, required_bandwidth):
        available_nodes = self.nodes_available()
        delay_dict = {node: float(self.get_node_from_name(node).metadata.labels[x]) for node in available_nodes for x in
                      self.get_node_from_name(node).metadata.labels.keys() if 'delay' in x}
        print(f"get_nodes_in_radius | delay_dict = {delay_dict}")
        bandwidth_dict = {}
        for node in available_nodes:
            bandwidth_dict[node] = round(
                sum([int(self.get_node_from_name(node).metadata.labels[x]) for node in available_nodes for x in self.get_node_from_name(node).metadata.labels.keys() if
                     'bandwidth_node' in x]) / (len(available_nodes) - 1), 2)
        print(f"get_nodes_in_radius | bandwidth_dict = {bandwidth_dict}")

        constrained_nodes_list = [node for node in delay_dict if
                                  required_delay > delay_dict[node] and required_bandwidth <= bandwidth_dict[node]]
        delay_bandwidth_dict = {node: {} for node in constrained_nodes_list}
        for node in constrained_nodes_list:
            delay_bandwidth_dict[node]["delay"] = delay_dict[node]
            delay_bandwidth_dict[node]["bandwidth"] = bandwidth_dict[node]

        return constrained_nodes_list, delay_bandwidth_dict

    def reused_placeholder_unused_pod_node(self, placeholder, nodes_enough_resource):
        covered_nodes = [self.get_node_from_podname_or_nodename(x) for x in placeholder.pods]
        covered_node_names = [y.metadata.name for y in covered_nodes]
        if any(x.metadata.name not in covered_node_names + [placeholder.node] for x in nodes_enough_resource):
            return True, covered_node_names + [placeholder.node]
        return False, None

    def get_memory_matrix(self, placeholder, nodes_enough_resource):
        placeholder_memory_matrix = {}
        for n in nodes_enough_resource:
            if n.metadata.name != placeholder.node:
                placeholder_memory_matrix[n.metadata.name] = 0
                for pod in self.get_pods_on_node(n.metadata.name):
                    if pod.metadata.name.split('-')[0] in placeholder.pods:
                        placeholder_memory_matrix[n.metadata.name] += self.get_pod_memory_request(pod)
        return placeholder_memory_matrix

    def reused_placeholder_used_pod_node(self, placeholder, pod, nodes_enough_resource):
        placeholder_memory_matrix = self.get_memory_matrix(placeholder, nodes_enough_resource)
        if any(placeholder_memory_matrix[x] + self.get_pod_memory_request(pod) <= placeholder.required_memory
               for x in placeholder_memory_matrix.keys()):
            return True, next(x for x in nodes_enough_resource if x.metadata.name in placeholder_memory_matrix.keys()
                              and placeholder_memory_matrix[x.metadata.name] +
                              self.get_pod_memory_request(pod) <= placeholder.required_memory)
        return False, None

    def add_pod_to_placeholder(self, pod, placeholder, extra_memory=0):
        placeholder.pods.add(pod.metadata.name.split('-')[0])
        placeholder.required_memory += extra_memory

    def narrow_placeholders_in_rad(self, node_names_in_rad):
        placeholders_in_rad = []
        for placeholder in self.placeholders:
            if placeholder.node in node_names_in_rad:
                placeholders_in_rad.append(placeholder)
        return placeholders_in_rad

    def assign_placeholder(self, pod, nodes_less_resource, nodes_enough_resource):
        # len(nodes_enough_resource) + len(nodes_less_resource) is always greater than 1!
        node_names_in_rad = [x.metadata.name for x in nodes_less_resource]
        node_names_in_rad += [x.metadata.name for x in nodes_enough_resource]
        placeholders_in_rad = self.narrow_placeholders_in_rad(node_names_in_rad)

        for placeholder in placeholders_in_rad:
            is_any_usable, excluded_list = self.reused_placeholder_unused_pod_node(placeholder, nodes_enough_resource)
            if is_any_usable:
                self.add_pod_to_placeholder(pod, placeholder)
                return [x.metadata.name not in excluded_list for x in nodes_enough_resource]

        for placeholder in placeholders_in_rad:
            is_any_usable, chosen_node = self.reused_placeholder_used_pod_node(placeholder, pod, nodes_enough_resource)
            if is_any_usable:
                self.add_pod_to_placeholder(pod, placeholder)
                return [chosen_node]

        # TODO: Another option, when we have to increase the placeholder's size
        #  for assigning the pod somewhere in the radius

        if len(nodes_enough_resource) > 1:
            placeholder = self.create_new_placeholder(nodes_enough_resource)
            self.add_pod_to_placeholder(pod, placeholder, self.get_pod_memory_request(pod))
            return [x for x in nodes_enough_resource if x.metadata.name != placeholder.node]
        else:
            print("WARNING Can not create placeholder for this pod!")
            return nodes_enough_resource

    def pod_has_placeholder(self, pod):
        try:
            # FIXME: '-' character assumed as splitting character
            return True, next(ph for ph in self.placeholders if pod.metadata.name.split('-')[0] in ph.pods)
        except StopIteration:
            return False, None

    def get_pod_res_request(self, pod, res_type):
        """
        res_type includes memory, cpu, ephemeral-storage
        """
        return sum([self.convert_to_int(x.resources.requests[res_type], res_type) for x in pod.spec.containers if
                    x.resources.requests is not None])

    def get_pod_memory_request(self, pod):
        return sum([self.convert_to_int(x.resources.requests['memory']) for x in pod.spec.containers if
                    x.resources.requests is not None])

    def get_pod_cpu_request(self, pod):
        return sum([self.convert_to_int(x.resources.requests['cpu']) for x in pod.spec.containers if
                    x.resources.requests is not None])

    def get_pod_disk_request(self, pod):
        return sum([self.convert_to_int(x.resources.requests['ephemeral-storage']) for x in pod.spec.containers if
                    x.resources.requests is not None])

    def narrow_nodes_by_capacity(self, pod, node_list):
        return_list, res_available_dict = [], {}
        for n in node_list:
            if self.calculate_available_res(n, "memory") <= self.get_pod_res_request(pod, "memory"):
                continue
            if self.calculate_available_res(n, "cpu") <= self.get_pod_res_request(pod, "cpu"):
                continue
            if self.calculate_available_res(n, "ephemeral-storage") <= self.get_pod_res_request(pod,
                                                                                                "ephemeral-storage"):
                continue
            return_list.append(n)
        return return_list  # , res_available_dict

    def get_placeholder_by_pod(self, pod):
        try:
            return next(x for x in self.placeholders if pod.metadata.name.split('-')[0] in x.pods)
        except StopIteration:
            return None

    def get_reschedulable(self, node, new_memory_request):
        pods_on_node = self.get_pods_on_node(node.metadata.name)
        for old_pod in pods_on_node:
            old_memory_request = self.get_pod_memory_request(old_pod)
            if old_memory_request >= new_memory_request:
                old_start_point = next(x for x in old_pod.metadata.labels.keys() if 'delay_' in x).split('_')[1]
                old_required_delay = int(old_pod.metadata.labels['delay_' + old_start_point])
                old_nodes_in_radius = self.narrow_nodes_by_capacity(old_pod,
                                                                    self.get_nodes_in_radius(old_start_point,
                                                                                             old_required_delay))
                old_placeholder = self.get_placeholder_by_pod(old_pod)
                if len([x for x in old_nodes_in_radius if x.metadata.name != old_placeholder.node]) > 0:
                    return True, old_pod, \
                           random.choice([x for x in old_nodes_in_radius if x.metadata.name != old_placeholder.node])
        return False, None, None

    def reschedule_pod(self, new_pod, new_nodes_in_radius):
        new_memory_request = self.get_pod_memory_request(new_pod)
        for n in new_nodes_in_radius:
            any_reschedulable, old_pod, reschedule_node = self.get_reschedulable(n, new_memory_request)
            if any_reschedulable:
                self.do_reschedule(old_pod, reschedule_node)
                return old_pod.metadata.name
        return None

    def update_popularity(self, host_node, whole_layer_list_back, image_id):  # 弃用
        cluster_dist_dict = self.orchestration_module.cluster_dist_dict

        # 更新layer热度
        for layer in whole_layer_list_back:
            with self.orchestration_module._dist_sync_lock:
                if layer not in cluster_dist_dict[host_node]["layer"]:
                    # 此时该layer属于预部署状态，先更新其热度
                    cluster_dist_dict[host_node]["layer"][layer] = {"frq": 1, "exit": 0}
                else:
                    cluster_dist_dict[host_node]["layer"][layer]["frq"] += 1

        # 更新image热度
        with self.orchestration_module._dist_sync_lock:
            if image_id not in cluster_dist_dict[host_node]["image"]:
                cluster_dist_dict[host_node]["image"][image_id] = {"frq": 1, "exit": 0}
            else:
                cluster_dist_dict[host_node]["image"][image_id]["frq"] += 1

    def do_reschedule(self, old_pod, reschedule_node):
        self.patch_pod(old_pod, reschedule_node.metadata.name)

    def do_schedule(self, pod, namespace="default"):
        """
        multi-stage scheduling
        steps: pre_schedule (sorting and filtering) -> priority_schedule (scoring) -> binding

        notify -> orchestration module
        """


        nodes_enough_resource_in_rad = self.pre_schedule(pod)

        if len(nodes_enough_resource_in_rad):
            host_node, image_lack_dict, whole_layer_list_back = self.priority_schedule(pod,
                                                                                       nodes_enough_resource_in_rad)  # merge_res_dict
            print(f"do_schedule | host_node = {host_node}, image_lack_dict = {image_lack_dict}")

            self.bind(pod, host_node, namespace)

            # None means that image is in the fs currently.
            if image_lack_dict["lack_lf"]["layer"] is not None:
                self.orchestration_module.orchestrate_deploy_trigger(host_node, image_lack_dict, whole_layer_list_back)

            # self.update_popularity(host_node, whole_layer_list_back, image_lack_dict["image_id"])
            # print(f"do_schedule | 已完成 update_popularity | update_popularity = {self.orchestration_module.cluster_dist_dict}")

        else:
            raise 0

    def priority_schedule(self, pod, pre_node_list, namespace="default"):
        weighting_factor = {"LeastRequestedPriority": 1, "BalancedResourceAllocation": 1, "LayerLocalityPriority": 1, }

        '''
        from k8s plugin/pkg/scheduler/algorithm/priorities/, booster introduces new features of ephemeral-storage
        
        The calculation reference for LeastRequestedPriority is:
        k8s: cpu((capacity-sum(requested))*10/capacity) + memory((capacity-sum(requested))*10/capacity))/2
        =>
        booster: cpu((capacity-sum(requested))*10/capacity) + memory((capacity-sum(requested))*10/capacity)/2 + 
        disk((capacity-sum(requested))*10/capacity))/10
        
        
        The calculation reference for BalancedResourceAllocation is:
        k8s: 10-(abs(cpuFraction-memoryFraction) + abs(cpuFraction-diskFraction) + abs(memoryFraction-
        diskFraction)/3)*10
        =>
        booster: 10-(abs(cpuFraction-memoryFraction) + abs(cpuFraction-diskFraction) + abs(memoryFraction-
        diskFraction)/3)*10
        
        '''
        highest_score, host_node, image_lack_dict, whole_layer_list_back = 0, None, {}, {}
        for node in pre_node_list:
            cpuFraction, sum_reserved_cpu, capacity_cpu = self.calculate_res_fraction(node, "cpu")
            print(f"node -> {node.metadata.name} | priority_schedule | cpuFraction = {cpuFraction}, sum_reserved_cpu = {sum_reserved_cpu}, capacity_cpu = {capacity_cpu}")

            memoryFraction, sum_reserved_memory, capacity_memory = self.calculate_res_fraction(node, "memory")
            print(f"node -> {node.metadata.name} | priority_schedule | memoryFraction = {memoryFraction}, sum_reserved_memory = {sum_reserved_memory}, capacity_memory = {capacity_memory}")

            diskFraction, sum_reserved_disk, capacity_disk = self.calculate_res_fraction(node, "ephemeral-storage")
            print(f"node -> {node.metadata.name} | priority_schedule | diskFraction = {diskFraction}, sum_reserved_disk = {sum_reserved_disk}, capacity_disk = {capacity_disk}")

            score_LeastRequestedPriority = self.get_leastrequestedpriority_score(sum_reserved_cpu, capacity_cpu,
                                                                                 sum_reserved_memory, capacity_memory,
                                                                                 sum_reserved_disk, capacity_disk)
            print(f"node -> {node.metadata.name} | priority_schedule | score_LeastRequestedPriority = {score_LeastRequestedPriority}")

            score_BalancedResourceAllocation = self.get_balancedresourceallocation_score(cpuFraction, memoryFraction,
                                                                                         diskFraction)
            print(f"node -> {node.metadata.name} | priority_schedule | score_BalancedResourceAllocation = {score_BalancedResourceAllocation}")

            with self.orchestration_module.share_sync_lock:
                score_LayerLocalityPriority, image_lack_lf, whole_layer_list_back = self.get_layerlocalitypriority_score(
                    node, pod.spec.containers[0].image)
            print(f"node -> {node.metadata.name} | priority_schedule | score_LayerLocalityPriority = {score_LayerLocalityPriority}")
            print(f"node -> {node.metadata.name} | priority_schedule | image_lack_lf = {image_lack_lf}")

            node_final_score = weighting_factor["LeastRequestedPriority"] * score_LeastRequestedPriority + \
                               weighting_factor["BalancedResourceAllocation"] * score_BalancedResourceAllocation + \
                               weighting_factor["LayerLocalityPriority"] * score_LayerLocalityPriority

            print(f"node -> {node.metadata.name} | priority_schedule | final score = {node_final_score}")

            if node_final_score > highest_score:
                highest_score, host_node, image_lack_dict = node_final_score, node.metadata.name, image_lack_lf

            elif node_final_score == highest_score and random.random() > 0.5:
                highest_score, host_node, image_lack_dict = node_final_score, node.metadata.name, image_lack_lf

        return host_node, image_lack_dict, whole_layer_list_back

    def pre_schedule(self, pod, namespace="default"):
        # New Pod request
        # Get the delay and bandwidth constraint value from the labels
        required_delay = int(pod.metadata.labels['limit_delay'])  # ms
        required_bandwidth = int(pod.metadata.labels['limit_bandwidth'])  # MBps
        print(f"pre_schedule | service required_delay = {required_delay}, required_bandwidth = {required_bandwidth}")

        # Getting all the nodes inside the delay radius
        all_measure_constrained_nodes, constrained_nodes_measure_dict = self.get_nodes_in_radius(required_delay,
                                                                                                 required_bandwidth)
        print(f"pre_schedule | all_measure_constrained_nodes = {all_measure_constrained_nodes}")
        print(f"pre_schedule | constrained_nodes_measure_dict = {constrained_nodes_measure_dict}")

        nodes_enough_resource_in_rad = self.narrow_nodes_by_capacity(pod, [
            self.get_node_from_name(n) for n in
            all_measure_constrained_nodes])
        print(f"pre_schedule | nodes_enough_resource_in_rad -> {[node.metadata.name for node in nodes_enough_resource_in_rad]}")

        return nodes_enough_resource_in_rad

    def schedule(self, pod, namespace="default"):
        try:
            # FIXME: '-' character assumed as splitting character
            if pod.metadata.name.split('-')[0] in self.rescedules.keys():
                node = self.rescedules[pod.metadata.name.split('-')[0]]
                del self.rescedules[pod.metadata.name.split('-')[0]]
                self.bind(pod, node)
                return
            # Is there any placeholder assigned to this pod name?
            any_assigned_placeholder, placeholder = self.pod_has_placeholder(pod)
            if any_assigned_placeholder:
                # The Pod has already an assigned placeholder so probably a node failure occurred,
                # we need to restart the pod
                self.patch_pod(pod, placeholder.node)
            else:
                # New Pod request
                # Get the previous element name from where the delay constraint defined
                previous_element_name = next(x for x in pod.metadata.labels.keys() if 'delay_' in x).split('_')[1]
                # Get the delay constraint value from the labels
                required_delay = int(pod.metadata.labels['delay_' + previous_element_name])
                # Getting all the nodes inside the delay radius
                all_nodes_in_radius = self.get_nodes_in_radius(previous_element_name, required_delay)
                nodes_enough_resource_in_rad = self.narrow_nodes_by_capacity(pod, all_nodes_in_radius)
                if len(nodes_enough_resource_in_rad) == 0:
                    # There is no node with available resource
                    # Try to reschedule some previously deployed Pod
                    old_pod_name = self.reschedule_pod(pod, all_nodes_in_radius)
                    # We have to wait, while the pod get successfully rescheduled
                    if old_pod_name is not None:
                        # FIXME: '-' character assumed as splitting character
                        # FIXME: We are waiting till only 1 instance remain. There can be more on purpose!
                        time.sleep(3)
                        print("INFO Waiting for rescheduling.")
                        while len([x.metadata.name for x in self.get_all_pods() if
                                   old_pod_name.split('-')[0] in x.metadata.name]) > 1:
                            time.sleep(1)
                            print("INFO Waiting for rescheduling.")
                    # Recalculate the nodes with the computational resources
                    nodes_enough_resource_in_rad = self.narrow_nodes_by_capacity(pod, all_nodes_in_radius)
                if len(all_nodes_in_radius) > 1:
                    nodes_enough_resource_in_rad = self.assign_placeholder(pod, [x for x in all_nodes_in_radius if
                                                                                 x not in nodes_enough_resource_in_rad],
                                                                           nodes_enough_resource_in_rad)
                    for ph in self.placeholders:
                        print("INFO Placeholder on node: %s ;assigned Pods: %s\n" % (ph.node, str(ph.pods)))
                elif len(all_nodes_in_radius) == 1:
                    print("WARNING No placeholder will be assigned to this Pod!")
                node = nodes_enough_resource_in_rad[0]
                self.bind(pod, node.metadata.name, namespace)
        except StopIteration:
            # No delay constraint for the pod
            node = self.get_cloud_node()
            self.bind(pod, node.metadata.name, namespace)

    def bind(self, pod, node, namespace="default"):
        target = client.V1ObjectReference(api_version='v1', kind='Node', name=node)
        meta = client.V1ObjectMeta()
        meta.name = pod.metadata.name

        body = client.V1Binding(target=target, metadata=meta)

        try:
            print("INFO Pod: %s placed on: %s\n" % (pod.metadata.name, node))
            api_response = self.v1.create_namespaced_pod_binding(name=pod.metadata.name, namespace=namespace, body=body)
            print(api_response)
            return api_response
        except Exception as e:
            print("Warning when calling CoreV1Api->create_namespaced_pod_binding: %s\n" % e)

    def patch_deployment(self, pod, node, namespace="default"):
        extensions_v1beta1 = client.ExtensionsV1beta1Api()
        deployment_name = pod.metadata.name.split('-')[0]
        deployment = [x for x in extensions_v1beta1.list_deployment_for_all_namespaces(watch=False).items if
                      x.metadata.namespace == 'default' and x.metadata.name == deployment_name][-1]
        deployment.spec.template.spec.node_name = node
        api_response = extensions_v1beta1.replace_namespaced_deployment(name=deployment_name, namespace=namespace,
                                                                        body=deployment)

    def patch_pod(self, pod, node, namespace="default"):
        # FIXME: '-' character assumed as splitting character
        self.rescedules[pod.metadata.name.split('-')[0]] = node
        self.v1.delete_namespaced_pod(name=pod.metadata.name, namespace=namespace)
