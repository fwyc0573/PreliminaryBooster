import os
import json
import subprocess
import threading
import requests

# import multiprocessing
from flask import Flask, request, jsonify
from orchestration_module import BoosterOrchestrationModule

orchestrator = BoosterOrchestrationModule()
app = Flask(__name__)
print(f"in init master proxy the orchestrator's id -> {id(orchestrator)}")


class Proxy(object):
    def __init__(self, port="9000", ip="0.0.0.0"):
        self.port = port
        self.ip = ip

    def proxy_start(self, debug=False, threaded=True):
        threading.Thread(
            target=app.run, args=(self.ip, self.port, debug, threaded)
        ).start()
        # multiprocessing.Process(target=app.run, args=(self.ip, self.port, debug, threaded)).start()


# 传输文件给其他node
def proxy_file_uploader(target_ip, target_port):
    url = "http://" + target_ip + ":" + target_port + "/proxy_node/downloader"
    header = {
        "file_list": json.dumps([1, 2, 3]),
        "node_name": "node71",
        "trans_granularity": "file",
    }

    res = requests.post(
        url=url,
        headers=header,
        files=[
            # ("yar", open("C:/Users/aaa/Desktop/rules-master/cve_rules/aaa.yar", "rb")),
            ("my_layer", open("D:/tetete", "rb")),
            ("my_layer", open("D:/tetete2", "rb")),
        ],
    )


def proxy_update_notifier(target_ip, target_port, update_msg, trans_id):
    url = "http://" + target_ip + ":" + target_port + "/proxy_node/update_receiver"
    # url = "http://127.0.0.1:8091/proxy/pre_orchestration"
    header = {
        "from_node": "master",
        "trans_id": str(trans_id),
    }

    print("proxy_update_notifier has successfully sent the msg to ", target_ip)
    print("in proxy data: ", update_msg)
    res = requests.post(url=url, headers=header, json=update_msg)
    print("from ", target_ip, " proxy_update_notifier get returns: ", res.status_code)


@app.route("/proxy_master/downloader", methods=["POST"])
def proxy_downloader():
    header_1 = request.headers["file_list"]
    header_2 = request.headers["node_name"]
    print(header_1, header_2)

    uploaded_files = request.files.getlist("my_layer")

    for trans_file in uploaded_files:
        file_name = trans_file.filename
        # file_path = os.path.join(dirpath, file_name)
        # trans_file.save(file_path)
        trans_file.save(file_name)
        print("已成功接收文件：", file_name)

    return jsonify(status="success")


@app.route("/proxy_master/changes_syncer", methods=["POST"])
def proxy_changes_syncer():
    """
    sync_dict

    content1: sync_dict["layer"]["size_update"]
    content2: sync_dict["layer"]["rm"]
    content3: sync_dict["file"]["size_update"]
    content4: sync_dict["file"]["rm"]
    content5: sync_dict["image"]["size_update"]
    content6: sync_dict["image"]["rm"]

    :return:
    """
    from_node = request.headers["from_node"]
    sync_dict = request.json
    # print(f"in master proxy the orchestrator's id -> {id(orchestrator)}")
    orchestrator.changes_sync_add([from_node, sync_dict])

    return jsonify(status="success")


# @app.route("/proxy_master/push_schedule_node_notifier", methods=["POST"])
def proxy_push_schedule_node_notifier(
    target_ip,
    target_port,
    basic_path,
    metadata_list,
    lack_dict,
    image_id,
    image_name,
    hub_to_get_dict,
):

    url = (
        "http://"
        + target_ip
        + ":"
        + target_port
        + "/proxy_node/push_schedule_node_receiver"
    )
    header = {
        "from_node": orchestrator.host_name,
        "image_id": image_id,
        "image_name": image_name,
        "lack_dict": json.dumps(lack_dict),
        "hub_to_get_dict": json.dumps(hub_to_get_dict),
    }
    whole_lf_list = []
    for metadata in metadata_list:
        whole_lf_list.append(("image_metadata", open(basic_path + metadata, "rb")))
    res = requests.post(url=url, headers=header, files=whole_lf_list)

    return res.status_code


# @app.route("/proxy_master/push_node_notifier", methods=["POST"])
def proxy_push_node_notifier(
    target_ip, target_port, trans_dict, schedule_node_info, image_id, flow_type
):
    url = "http://" + target_ip + ":" + target_port + "/proxy_node/push_node_receiver"
    header = {
        "from_node": orchestrator.host_name,
        "schedule_node_ip": schedule_node_info[0],
        "schedule_node_port": schedule_node_info[1],
        "image_id": image_id,
        "flow_type": flow_type,
    }

    res = requests.post(url=url, headers=header, json=trans_dict)

    return res.status_code


def master_orchestrator_and_proxy_start():
    proxy = Proxy()
    proxy.proxy_start()
    orchestrator.orchestration_daemon()
    handle = orchestrator.init_daemons()
    orchestrator.simulate_deploy()
    orchestrator.init_random_collocation()

    # scheduler_entry()

    handle.join()
    return handle, orchestrator


@app.route("/proxy_master/service_delete", methods=["POST"])
def proxy_changes_syncer_service_delete():

    from_node = request.headers["from_node"]
    image_id = request.headers["image_id"]
    image_name = request.headers["image_name"]
    pull_time = request.headers["pull_time"]
    hit_rate = request.headers["hit_rate"]
    orchestrator.service_delete_queue.put(
        {
            "from_node": from_node,
            "image_id": image_id,
            "image_name": image_name,
            "pull_time": pull_time,
            "hit_rate": hit_rate,
        }
    )

    return jsonify(status="success")


def proxy_clean_all_cache(target_ip):
    url = "http://" + target_ip + ":" + "9000" + "/proxy_node/clean_all_cache_receiver"
    header = {
        "from_node": orchestrator.host_name,
    }
    res = requests.post(url=url, headers=header)
    return res.status_code


if __name__ == "__main__":
    proxy = Proxy()
    proxy.proxy_start()
    # orchestrator.orchestration_daemon()
    handle = orchestrator.init_daemons()
    orchestrator._get_schedulable_node()
    orchestrator.init_random_collocation()
    # handle.join()
