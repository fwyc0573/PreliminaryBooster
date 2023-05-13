#!/usr/bin/env python3


# from os import path
from kubernetes import client, config, utils
from kubernetes.client.api import core_v1_api
from kubernetes.client.rest import ApiException
from kubernetes.stream import stream
import time

# import itertools
# import numpy as np

try:
    config.load_kube_config()
except FileNotFoundError as e:
    print("WARNING %s\n" % e)
    config.load_incluster_config()
api = core_v1_api.CoreV1Api()


def get_node_names():
    return [
        node.metadata.name for node in (api.list_node(watch=False)).items
    ]  # if "master" not in node.metadata.name


def create_pod_template(pod_name, node_name):
    # Configureate Pod template container
    container = client.V1Container(
        name=pod_name,
        image="",
        # command=['iperf3 -s -f M']
    )

    # Create and configurate a spec section
    template = client.V1PodTemplateSpec(
        metadata=client.V1ObjectMeta(name=pod_name),
        spec=client.V1PodSpec(
            containers=[container], node_selector={"kubernetes.io/hostname": node_name}
        ),
    )

    return template


def deploy_measure_pod(pod_IPs, pod_node_mapping):
    for pod, pod_ip in pod_IPs.items():
        if pod_ip == None:
            template = create_pod_template(pod, pod_node_mapping[pod])

            api_instance = client.CoreV1Api()
            namespace = "default"
            body = client.V1Pod(metadata=template.metadata, spec=template.spec)
            api_response = api_instance.create_namespaced_pod(namespace, body)


def check_measure_pod(ping_pods):
    for pod in ping_pods:
        running = False
        time_out = 120
        cur_time = 0
        while cur_time < time_out:
            resp = api.read_namespaced_pod(name=pod, namespace="default")
            if resp.status.phase == "Running":
                running = True
                break
            time.sleep(1)
            cur_time += 1
        if not running:
            raise Exception("TIMEOUT: Pod {} is not running".format(pod))


def get_ping_pod_IPs(ping_pods, pod_IPs):
    ret = api.list_pod_for_all_namespaces(watch=False)
    for i in ret.items:
        if str(i.metadata.name) in ping_pods:
            pod_IPs[i.metadata.name] = i.status.pod_ip

    return pod_IPs


def measure_latency(pod_from, pod_to_IP):
    namespace = "default"

    exec_command = ["/bin/sh", "-c", "ping -c 3 {}".format(pod_to_IP)]

    resp = stream(
        api.connect_get_namespaced_pod_exec,
        pod_from,
        namespace,
        command=exec_command,
        stderr=True,
        stdin=False,
        stdout=True,
        tty=False,
    )
    # print(f"resp: {resp}")

    round_trip_line = next(
        line for line in resp.split("\n") if "round-trip min/avg/max/stddev" in line
    )
    min_rtt = round_trip_line.split("/")[3]
    avg_rtt = round_trip_line.split("/")[4]
    max_rtt = round_trip_line.split("/")[5]
    return float(avg_rtt)


def measure_bandwidth(pod_from, pod_to_IP):
    namespace = "default"

    exec_command = [
        "/bin/sh",
        "-c",
        "iperf3 -t 3 -c {} -l 2000B -f M".format(pod_to_IP),
    ]

    resp = stream(
        api.connect_get_namespaced_pod_exec,
        pod_from,
        namespace,
        command=exec_command,
        stderr=True,
        stdin=False,
        stdout=True,
        tty=False,
    )
    # print(f"measure_bandwidth | resp = {resp}")
    bandwidth_line = next(line for line in resp.split("\n") if "sender" in line)
    # print(f"bandwidth_line.split() -> {bandwidth_line.split()}")
    bandwidth = bandwidth_line.split()[6]
    # print(f"In detail, pod_from -> {pod_from}, pod_to_ip -> {pod_to_IP}, bandwidth -> {bandwidth}")
    return int(float(bandwidth))


def do_labeling(node_name, labels):
    # FIXME: This method could be stucked into a unvalid state: if we already delete the old rtt labels,
    #        but due to a failure, the new labels are not saved

    for key, value in labels.items():
        api_instance = client.CoreV1Api()
        # label_value = "_".join(value)
        body = {"metadata": {"labels": {key: str(value)}}}
        # print(f"in do_labeling, the body is -> {body}")
        api_response = api_instance.patch_node(node_name, body)


def do_measuring_delay(pod_IPs, pod_nodes_mapping):
    # measure_latency(): pod_name（执行方）, pod_ip（接收方）
    master_pod = next(
        pod_name
        for pod_name in pod_nodes_mapping
        if "master" in pod_nodes_mapping[pod_name]
    )
    record_delay_dict = {}
    for pod_name in pod_IPs:
        if pod_name != master_pod:
            # print(f"Now do measuring delay between {master_pod} -> {pod_name}...")
            record_delay_dict[pod_nodes_mapping[pod_name]] = measure_latency(
                master_pod, pod_IPs[pod_name]
            )

    return record_delay_dict


def do_measuring_bandwidth(pod_IPs, pod_nodes_mapping):
    master_pod = next(
        pod_name
        for pod_name in pod_nodes_mapping
        if "master" in pod_nodes_mapping[pod_name]
    )
    record_bandwidth_dict = {
        pod_nodes_mapping[pod_name]: {}
        for pod_name in pod_IPs
        if pod_name != master_pod
    }
    # print(f"record_bandwidth_dict -> {record_bandwidth_dict}")
    for pod_name_send in pod_IPs:
        if pod_name_send != master_pod:
            for pod_name_recv in pod_IPs:
                if pod_name_recv != master_pod and pod_name_recv != pod_name_send:
                    # print(f"Now do measuring bandwidth between {pod_name_send} -> {pod_name_recv}...")
                    record_bandwidth_dict[pod_nodes_mapping[pod_name_send]][
                        pod_nodes_mapping[pod_name_recv]
                    ] = measure_bandwidth(pod_name_send, pod_IPs[pod_name_recv])

    return record_bandwidth_dict


def labeling():
    nodes = get_node_names()
    ping_pod_list = [
        "booster-measure-pod{}".format(i) for i in range(1, len(nodes) + 1)
    ]
    pod_nodes_mapping = {ping_pod_list[i]: nodes[i] for i in range(len(ping_pod_list))}
    pod_IPs = {ping_pod_list[i]: None for i in range(len(ping_pod_list))}
    pod_IPs = get_ping_pod_IPs(ping_pod_list, pod_IPs)

    # Deploy measurement pods
    deploy_measure_pod(pod_IPs, pod_nodes_mapping)
    check_measure_pod(ping_pod_list)

    pod_IPs = get_ping_pod_IPs(ping_pod_list, pod_IPs)

    # Measure delay
    delay_dict = do_measuring_delay(pod_IPs, pod_nodes_mapping)
    # print(f"delay_dict -> {delay_dict}")

    # Measure bandwidth
    bandwidth_dict = do_measuring_bandwidth(pod_IPs, pod_nodes_mapping)
    # print(f"bandwidth_dict -> {bandwidth_dict}")

    for node in delay_dict:
        do_labeling(
            node,
            {
                **{"delay": delay_dict[node]},
                **{
                    "bandwidth_" + recv_node: bandwidth_dict[node][recv_node]
                    for recv_node in bandwidth_dict[node]
                },
            },
        )


def measure_entry(interval):
    while True:
        # print("Booster starts updating labels...")
        labeling()
        # print("Booster finish updating labels...")

        time.sleep(interval)


if __name__ == "__main__":
    measure_entry()
