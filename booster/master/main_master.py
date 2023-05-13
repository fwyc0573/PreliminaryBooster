"""
    entry point
"""

import json
import threading
import time

from kubernetes import client, watch
from master_proxy_module import master_orchestrator_and_proxy_start
from scheduler import BoosterScheduler
from measure_module import measure_entry
import multiprocessing
from queue import Queue as thr_queue


def booster_deploy_daemons(deploy_queue, scheduler):
    while True:
        event = deploy_queue.get()
        while not scheduler.orchestration_module.can_next_deploy:
            print("booster_deploy_daemons | waiting for last deploy job...")
            time.sleep(3)
        try:
            scheduler.orchestration_module.can_next_deploy = False
            print(
                "Creating pod - named {} - request received".format(
                    event["object"].metadata.name
                )
            )
            print(f"Total info about event[object]: {event['object']}")
            # res = scheduler.schedule(event['object'])
            # res = scheduler.test_schedule(event['object'])
            res = scheduler.do_schedule(event["object"])

        except client.rest.ApiException as e:
            print(json.loads(e.body)["message"])


def v1_main():
    booster_measure_labeler = multiprocessing.Process(target=measure_entry, args=(3,))
    booster_measure_labeler.start()
    print("Booster measuring labeler is starting...")

    daemon_handle, orchestration_module = master_orchestrator_and_proxy_start()
    print(
        f"master_orchestrator_and_proxy_start返回的orchestrator所在内存为：{id(orchestration_module)}"
    )

    scheduler = BoosterScheduler(orchestration_module)
    print("Booster scheduler is starting...")

    w = watch.Watch()
    # FIXME: API BUG: https://github.com/kubernetes-client/python/issues/547
    for event in w.stream(scheduler.v1.list_namespaced_pod, "default"):
        if (
            event["object"].status.phase == "Pending"
            and event["type"] == "ADDED"
            and event["object"].spec.scheduler_name == scheduler.scheduler_name
        ):
            try:
                scheduler.orchestration_module.can_next_deploy = False
                print(
                    "Creating pod - named {} - request received".format(
                        event["object"].metadata.name
                    )
                )
                print(f"Total info about event[object]: {event['object']}")
                # res = scheduler.schedule(event['object'])
                # res = scheduler.test_schedule(event['object'])
                res = scheduler.do_schedule(event["object"])

            except client.rest.ApiException as e:
                print(json.loads(e.body)["message"])

    daemon_handle.join()


def main():
    booster_measure_labeler = multiprocessing.Process(target=measure_entry, args=(3,))
    booster_measure_labeler.start()

    daemon_handle, orchestration_module = master_orchestrator_and_proxy_start()
    scheduler = BoosterScheduler(orchestration_module)

    deploy_queue = thr_queue(maxsize=100)
    booster_deploy_daemons_thr = threading.Thread(
        target=booster_deploy_daemons, args=(deploy_queue, scheduler)
    )
    booster_deploy_daemons_thr.start()

    w = watch.Watch()
    # FIXME: API BUG: https://github.com/kubernetes-client/python/issues/547
    for event in w.stream(scheduler.v1.list_namespaced_pod, "default"):
        if (
            event["object"].status.phase == "Pending"
            and event["type"] == "ADDED"
            and event["object"].spec.scheduler_name == scheduler.scheduler_name
        ):
            deploy_queue.put(event)

    daemon_handle.join()


if __name__ == "__main__":
    main()
