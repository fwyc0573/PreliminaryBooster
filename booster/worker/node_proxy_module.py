import copy
import json
import os
import shutil
import subprocess
import threading
import time

import requests
from flask import Flask, request, jsonify

from my_agent import BoosterAgent
from utils_images import compress_layerFile, remove_layerFile
from concurrent.futures import ThreadPoolExecutor


app = Flask(__name__)
agent = BoosterAgent()


class Proxy(object):
    def __init__(self, agent_instance=None, port="9000", ip="0.0.0.0"):
        self.agent = agent_instance
        self.port = port
        self.ip = ip

    def proxy_start(self, debug=False, threaded=True):
        threading.Thread(
            target=app.run, args=(self.ip, self.port, debug, threaded)
        ).start()


@app.route("/proxy_node/update_receiver", methods=["POST"])
def proxy_update_receiver():
    trans_id = request.headers["trans_id"]
    from_node = request.headers["from_node"]

    global agent
    whole_trans_dict = request.json
    hub_to_get_dict = whole_trans_dict["hub_to_get"]
    print("hub_to_get_dict:", hub_to_get_dict)
    agent.hub_to_get_add(hub_to_get_dict["layer"]) if hub_to_get_dict["layer"] else 0
    # if hub_to_get_dict["layer"] or hub_to_get_dict["file"] else 0

    bro_to_send_dict = whole_trans_dict["bro_to_send"]
    print("bro_to_send_dict:", bro_to_send_dict)

    local_to_del_dict = whole_trans_dict["local_to_del"]
    check_local_del = copy.deepcopy(local_to_del_dict)
    if local_to_del_dict:
        for ip in bro_to_send_dict:
            for each_layer in bro_to_send_dict[ip]["layer"]:
                if each_layer in check_local_del["layer"]:
                    agent.wait_rm_add("layer", each_layer)
                    local_to_del_dict["layer"].remove(each_layer)
            for each_file in bro_to_send_dict[ip]["file"]:
                if each_file in check_local_del["file"]:
                    agent.wait_rm_add("file", each_file)
                    local_to_del_dict["file"].remove(each_file)

    agent.bro_to_send_add(bro_to_send_dict) if bro_to_send_dict else 0

    bro_get_check_dict = whole_trans_dict["bro_get_check"]
    print("bro_get_check_dict:", bro_get_check_dict)
    # agent.bro_get_check_add(bro_get_check_dict) if bro_get_check_dict else 0

    agent.local_to_del_add(local_to_del_dict) if local_to_del_dict[
        "layer"
    ] or local_to_del_dict["file"] else 0
    print("local_to_del_dict:", local_to_del_dict)

    return jsonify(status="success")


def thread_exception(worker):
    worker_exception = worker.exception()
    if worker_exception:
        print("erro: {}".format(worker_exception))


def thr_save_decompress(args):
    print(
        "flask thr_save_decompress | Thread %s, parent process %s"
        % (threading.get_ident(), os.getppid())
    )

    trans_layer, agent, image_id, flow_type = args
    layer_name = trans_layer.filename  # 现在为layer_id+"layer.tar"(本来是带有.tar.gz后缀)
    print(
        f"thr_save_decompress | layer_name:{layer_name}, layer_name[:-9]={layer_name[:-9]}"
    )

    my_path = (
        agent.menu_basic_path + "construct/" + image_id + "/"
        if flow_type == "deploy"
        else agent.menu_basic_path + "layer/"
    )
    if not agent.check_if_local_exit("layer", layer_name[:-9]):  # 不在cache中即可
        # tmp_path = agent.menu_basic_path + "tmp/" + layer_name
        dir_tmp_path = agent.menu_basic_path + "tmp/" + layer_name[:-9]

        try:
            os.mkdir(dir_tmp_path, mode=0o777)  # 创建一个layer_id的文件夹
        except:
            print(f"thr_save_decompress | 文件夹创建失败...")

        pure_tmp_path = agent.menu_basic_path + "tmp/"
        tar_layer_save_path = os.path.join(dir_tmp_path, "layer.tar")
        trans_layer.save(tar_layer_save_path)

        # 检查构建的image_id目录是否存在
        try:
            os.mkdir(my_path, mode=0o777)
        except:
            pass

        shutil.move(dir_tmp_path, my_path)
        agent.coming_soon_update_queue.put(
            {"action": "rm", "granularity": "layer", "my_id": layer_name[:-9]}
        ) if layer_name[:-9] in agent.coming_soon_set["layer"] else 0
    else:
        print("layer_name：", layer_name, " 已在本地存在副本，跳过下载...")


pool = ThreadPoolExecutor()


@app.route("/proxy_node/downloader", methods=["POST"])
def proxy_downloader():
    # trans_metadata
    layer_list = request.headers["trans_layer"]
    file_list = request.headers["trans_file"]
    miss_lf = request.headers["miss_lf"]
    from_node = request.headers["from_node"]
    trans_id = request.headers["trans_id"]
    image_id = request.headers["image_id"]
    timestamp = request.headers["timestamp"]
    flow_type = request.headers["flow_type"]

    print(
        f"proxy_downloader | from_node = {from_node}, layer_list = {layer_list}, time_use = {time.time()-float(timestamp)}"
    )
    # print(f"proxy_downloader | from_node = {from_node}, layer_list = {layer_list}, timestamp = {timestamp}")

    global agent
    downloaded_layers = request.files.getlist("my_layer")
    # downloaded_files = request.files.getlist("my_file")
    result_list = []

    for trans_layer in downloaded_layers:
        task = pool.submit(
            thr_save_decompress, (trans_layer, agent, image_id, flow_type)
        )
        task.add_done_callback(thread_exception)
        result_list.append(task)

    join_thr = [task.result() for task in result_list]  # 用于阻塞

    # for trans_file in downloaded_files:
    #     file_name = trans_file.filename
    #     if not agent.check_if_local_exit("file", file_name):
    #         my_path = agent.menu_basic_path + "file/"  # + file_name
    #         tmp_path = agent.menu_basic_path + "tmp/" + file_name
    #         trans_file.save(tmp_path)
    #         agent.coming_soon_update_queue.put(
    #             {"action": "rm", "granularity": "file", "my_id": file_name}) if file_name in agent.coming_soon_set[
    #             "file"] else 0
    #         # agent.coming_soon_set["file"].remove(file_name) if file_name in agent.coming_soon_set["file"] else 0
    #         print("已成功接收file文件tmp_path为：", tmp_path)
    #         agent.decompress_rm_add([my_path, tmp_path, file_name, "file"])
    #     else:
    #         print("file_name：", file_name, " 已在本地存在副本，跳过下载...")
    return jsonify(status="success")


def v1_proxy_uploader(
    target_ip,
    target_port,
    layer_list,
    file_list,
    trans_id,
    layer_tag="my_layer",
    file_tag="my_file",
    image_id="0",
    flow_type="deploy",
):
    upload_time1 = time.time()
    url = "http://" + target_ip + ":" + target_port + "/proxy_node/downloader"
    whole_lf_list = []
    record_name_list = []
    miss_lf = []
    global agent

    for each_layer in layer_list:
        time_tmp = time.time()
        my_path = agent.menu_basic_path + "layer/" + each_layer
        tmp_path = agent.menu_basic_path + "tmp/" + each_layer
        if not os.path.exists(my_path):
            print("缺少:", my_path)
            miss_lf.append(each_layer)
            continue

        compress_name, stream_back = compress_layerFile(my_path, tmp_path, each_layer)
        # whole_lf_list.append((layer_tag, open(compress_name, "rb")))
        whole_lf_list.append((layer_tag, stream_back))
        record_name_list.append(compress_name)

        print(f"each_layer={each_layer}, use_time={time.time() - time_tmp}")

    header = {
        "trans_layer": json.dumps(layer_list),
        "trans_file": json.dumps(file_list),
        "miss_lf": json.dumps(miss_lf),
        "from_node": agent.host_name,
        "trans_id": str(trans_id),  # 传输编号，用于check
        "image_id": str(image_id),
        "timestamp": str(time.time()),
        "flow_type": flow_type,
    }

    res = requests.post(url=url, headers=header, files=whole_lf_list)
    print(
        f"proxy_uploader | trans_layer_list: {layer_list}, 完成压缩与post用时: {time.time() - upload_time1}"
    )

    remove_layerFile(record_name_list)
    return res.status_code


def proxy_uploader(
    target_ip,
    target_port,
    layer_list,
    file_list,
    trans_id,
    layer_tag="my_layer",
    file_tag="my_file",
    image_id="0",
    flow_type="deploy",
):
    upload_time1 = time.time()
    url = "http://" + target_ip + ":" + target_port + "/proxy_node/downloader"
    whole_lf_list = []
    # record_name_list = []
    miss_lf = []
    global agent

    for each_layer in layer_list:
        time_tmp = time.time()
        my_path = agent.menu_basic_path + "layer/" + each_layer
        tmp_path = agent.menu_basic_path + "tmp/" + each_layer
        if not os.path.exists(my_path):
            miss_lf.append(each_layer)
            continue

        tar_complete_path = os.path.join(my_path, "layer.tar")
        tar_change_path = os.path.join(my_path, each_layer + "layer.tar")
        os.rename(tar_complete_path, tar_change_path)
        whole_lf_list.append((layer_tag, open(tar_change_path, "rb")))
        os.rename(tar_change_path, tar_complete_path)

        print(f"each_layer={tar_change_path}, use_time={time.time() - time_tmp}")
    header = {
        "trans_layer": json.dumps(layer_list),
        "trans_file": json.dumps(file_list),
        "miss_lf": json.dumps(miss_lf),
        "from_node": agent.host_name,
        "trans_id": str(trans_id),
        "image_id": str(image_id),
        "timestamp": str(time.time()),
        "flow_type": flow_type,
    }

    res = requests.post(url=url, headers=header, files=whole_lf_list)
    print(
        f"proxy_uploader | trans_layer_list: {layer_list}, 完成压缩与post用时: {time.time() - upload_time1}"
    )

    return res.status_code


# TODO: 补充同步发送
def proxy_changes_syncer(target_ip, target_port, node_name, sync_dict):
    url = "http://" + target_ip + ":" + target_port + "/proxy_master/changes_syncer"
    # url = "http://127.0.0.1:8091/proxy/pre_orchestration"
    header = {
        "from_node": node_name,
    }

    print(
        "proxy_update_notifier has successfully sent the msg to ",
        target_ip,
        "; sync_dict:",
        sync_dict,
    )
    res = requests.post(url=url, headers=header, json=sync_dict)
    print("from ", target_ip, " proxy_update_notifier get returns: ", res.status_code)
    return res.status_code


@app.route("/proxy_node/push_schedule_node_receiver", methods=["POST"])
def proxy_push_schedule_node_receiver():
    from_node = request.headers["from_node"]
    image_id = request.headers["image_id"]
    image_name = request.headers["image_name"]
    lack_dict = json.loads(request.headers["lack_dict"])
    hub_to_get_dict = json.loads(request.headers["hub_to_get_dict"])

    global agent
    downloaded_metadata = request.files.getlist("image_metadata")
    metadata_store_path = agent.menu_basic_path + "construct/" + image_id + "/"
    metadata_cache_path = agent.menu_basic_path + "metadata_cache/" + image_id + "/"
    agent.hub_to_get_add(
        hub_to_get_dict["layer"], image_id, work_type="deploy"
    ) if hub_to_get_dict["layer"] else 0
    print(f"agent.hub_to_get_add -> {hub_to_get_dict['layer']}")

    try:
        os.mkdir(metadata_store_path, mode=0o777)
    except OSError:
        pass
    try:
        os.mkdir(metadata_cache_path, mode=0o777)
    except OSError:
        pass

    for trans_metadata in downloaded_metadata:
        metadata = trans_metadata.filename
        tmp_path = metadata_store_path + metadata
        cache_path = metadata_cache_path + metadata
        trans_metadata.save(tmp_path)
        trans_metadata.save(cache_path)
        print(f"已成功接收metadata文件,tmp_path:{tmp_path};metadata_cache_path:{cache_path}")

    for layer in lack_dict["layer"]:
        # agent.coming_soon_set["layer"].add(layer)
        agent.coming_soon_update_queue.put(
            {"action": "add", "granularity": "layer", "my_id": layer}
        )
    for file in lack_dict["file"]:
        # agent.coming_soon_set["file"].add(file)
        agent.coming_soon_update_queue.put(
            {"action": "add", "granularity": "file", "my_id": file}
        )
    # print(f"proxy_push_schedule_node_receiver | coming_soon_set = {agent.coming_soon_set['layer']}")

    agent.deploy_construct_add([image_id, image_name, lack_dict])
    # print(f"agent.deploy_construct_add | [image_id, image_name, lack_dict] = {[image_id, image_name, lack_dict]}")
    return jsonify(status="success")


@app.route("/proxy_node/push_node_receiver", methods=["POST"])
def proxy_push_node_receiver():
    from_node = request.headers["from_node"]
    schedule_node_ip = request.headers["schedule_node_ip"]
    schedule_node_port = request.headers["schedule_node_port"]
    image_id = request.headers["image_id"]
    flow_type = request.headers["flow_type"]

    whole_trans_dict = request.json  # {"layer": [], "file": []}

    res = proxy_uploader(
        schedule_node_ip,
        schedule_node_port,
        whole_trans_dict["layer"],
        whole_trans_dict["file"],
        "-1",
        image_id=image_id,
        flow_type=flow_type,
    )
    return jsonify(status="success")


def proxy_notify_kubectl_delete(
    target_ip, target_port, node_name, image_id, image_name, pull_time, cache_hit
):
    """
    kubeclt delete yaml
    pull_time collect

    """
    url = "http://" + target_ip + ":" + target_port + "/proxy_master/service_delete"
    header = {
        "from_node": node_name,
        "image_id": image_id,
        "image_name": image_name,
        "pull_time": str(pull_time),
        "hit_rate": str(cache_hit),
    }

    print(
        "proxy_notify_kubectl_delete has successfully sent the msg to ",
        target_ip,
        "; image_id:",
        image_id,
    )
    res = requests.post(url=url, headers=header)
    print(
        "from ",
        target_ip,
        " proxy_notify_kubectl_delete get returns: ",
        res.status_code,
    )
    return res.status_code


@app.route("/proxy_node/clean_all_cache_receiver", methods=["POST"])
def proxy_clean_all_cache_receiver():
    global agent

    path_construct = agent.menu_basic_path + "construct/*"
    path_layer = agent.menu_basic_path + "layer/*"
    path_tmp = agent.menu_basic_path + "tmp/*"

    clean_order_construct = "sudo rm -rf " + path_construct
    clean_order_layer = "sudo rm -rf " + path_layer
    clean_order_tmp = "sudo rm -rf " + path_tmp

    pro = subprocess.Popen(clean_order_construct, shell=True, stdout=subprocess.PIPE)
    pro.wait()
    pro = subprocess.Popen(clean_order_layer, shell=True, stdout=subprocess.PIPE)
    pro.wait()
    pro = subprocess.Popen(clean_order_tmp, shell=True, stdout=subprocess.PIPE)
    pro.wait()

    return jsonify(status="success")


def node_agent_and_proxy_start():
    # agent = Agent()
    my_proxy = Proxy()
    # proxy.proxy_start()
    # thread_handle = agent.init_daemons()  # 启动agent的守护线程

    # agent.deploy_construct("6d891ca36d5f3fdc256f65ae4300195e46b6742e2f859936bfc53562cf3e73a8", {})

    # thread_handle.join()
    return my_proxy, agent


if __name__ == "__main__":
    # agent = Agent()
    proxy = Proxy()
    proxy.proxy_start()
    thread_handle = agent.init_daemons()  # 启动agent的守护线程
    # agent.deploy_construct("6d891ca36d5f3fdc256f65ae4300195e46b6742e2f859936bfc53562cf3e73a8", {})

    thread_handle.join()
