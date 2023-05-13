import multiprocessing
import os
import shutil
import subprocess
import tarfile
import threading
import time
import random
import json

# import docker
from multiprocessing import Queue, Manager
from concurrent.futures import ThreadPoolExecutor
from threading import BoundedSemaphore
from queue import Queue as thr_queue
import requests
import gzip
from utils_images import (
    get_single_layer_from_hub,
    decompress_layerFile,
    rm_single_layerFile,
    layer_list_get,
    gzip_decompress_rm,
    layer_json_creat,
    deploy_tar_creat,
    get_image_layer_from_hub,
)
from my_object import LRU, LFU, NAIVEALG
from my_arc import ARC
from utils_images import cache_copy_compress_rename
import sys


def thread_exception(worker):
    worker_exception = worker.exception()
    if worker_exception:
        print("erro: {}".format(worker_exception))


def progress_bar(ublob, nb_traits):
    sys.stdout.write("\r" + ublob[7:19] + ": Downloading [")
    for i in range(0, nb_traits):
        if i == nb_traits - 1:
            sys.stdout.write(">")
        else:
            sys.stdout.write("=")
    for i in range(0, 49 - nb_traits):
        sys.stdout.write(" ")
    sys.stdout.write("]")
    sys.stdout.flush()


def v1_thr_downloading(args):
    print(
        "thr_downloading | Thread %s, parent process %s"
        % (threading.get_ident(), os.getppid())
    )
    obj, ublob = args
    print(f"thr_downloading | ublob -> {ublob}")
    # ublob = obj["ublob"]
    layer_dir = obj["save_path"] + ublob[7:]
    try:
        os.mkdir(layer_dir, mode=0o777)
    except OSError:
        pass

    sys.stdout.write(ublob[7:19] + ": Downloading...")
    sys.stdout.flush()

    time1 = time.time()
    bresp = requests.get(
        "https://{}/v2/{}/blobs/{}".format(obj["registry"], obj["repository"], ublob),
        headers=obj["auth_head"],
        stream=True,
        verify=False,
    )
    print(f"thr_downloading | get bresp time -> {time.time() - time1}")

    # Stream download and follow the progress
    bresp.raise_for_status()  # print check if an error has occurred
    unit = int(bresp.headers["Content-Length"]) / 50
    acc = 0
    nb_traits = 0
    progress_bar(ublob, nb_traits)

    time2 = time.time()
    with open(layer_dir + "/layer_gzip.tar", "wb") as file:
        for chunk in bresp.iter_content(chunk_size=8192):
            if chunk:
                file.write(chunk)
                acc = acc + 8192
                if acc > unit:
                    nb_traits = nb_traits + 1
                    progress_bar(ublob, nb_traits)
                    acc = 0
    print(f"thr_downloading | layer_gzip.tar time -> {time.time() - time2}")

    time3 = time.time()
    with open(layer_dir + "/layer.tar", "wb") as file:
        unzLayer = gzip.open(layer_dir + "/layer_gzip.tar", "rb")
        shutil.copyfileobj(unzLayer, file)
        unzLayer.close()

    os.remove(layer_dir + "/layer_gzip.tar")
    print(f"thr_downloading | layer.tar time -> {time.time() - time3}")
    print(f"thr_downloading | downloading layer -> {ublob} | finished")


class BoosterAgent(object):
    def __init__(self, agent_name="booster-agent", host_node=os.getlogin()):
        self.my_layers = []
        self.agent_name = agent_name
        self.__menu_basic_path = "/home/" + os.getlogin() + "/edge_cloud_DB/"
        self.__host_node_name = host_node
        self.init_dir_structure()
        # self.init_node_proxy(PORT)
        self.coming_soon_set = {"layer": set(), "file": set()}
        self._wait_rm_dict = {"layer": [], "file": []}
        self.running_container_list = []
        self.cache_updating_now_dict = {}
        self._hub_to_get_queue = thr_queue(maxsize=100)

        self._upload_conn = BoundedSemaphore(3)
        self._download_conn = 3
        # self._bro_to_send_queue = thr_queue(maxsize=100)
        self._bro_to_send_queue = Queue(maxsize=50)

        self._local_to_del_queue = thr_queue(maxsize=100)

        self._decompress_rm_queue = thr_queue(maxsize=100)
        self._deploy_construct_queue = thr_queue(maxsize=20)

        self.downloading_queue = Queue(maxsize=20)
        self.coming_soon_update_queue = Queue(maxsize=50)
        self.cache_update_queue = thr_queue(maxsize=20)

        self._changes_sync_dict = {
            "layer": {"size_update": {}, "rm": []},
            "file": {"size_update": {}, "rm": []},
            "image": {"size_update": {}, "rm": []},
        }
        self._changes_sync_lock = threading.Lock()
        self.coming_soon_lock = threading.Lock()
        self._cache_update_lock = threading.Lock()
        # self.lfu = LFU(1024 * 2)
        # self.naive_cache = NAIVEALG(1024 * 2, self.__menu_basic_path + "layer")
        # self.lru = LRU(1024 * 2, self.__menu_basic_path + "layer")
        self.cache_size = 500
        self.arc = ARC(self.cache_size, self.__menu_basic_path + "layer")
        self.init_local_cache_info_to_master()

    @property
    def menu_basic_path(self):
        return self.__menu_basic_path

    @property
    def host_name(self):
        return self.__host_node_name

    def init_dir_structure(self):
        if not os.path.exists(self.__menu_basic_path):
            os.mkdir(self.__menu_basic_path, mode=0o777)
            os.mkdir(self.__menu_basic_path + "layer/", mode=0o777)
            os.mkdir(self.__menu_basic_path + "file/", mode=0o777)
            os.mkdir(self.__menu_basic_path + "tmp/", mode=0o777)
            os.mkdir(self.__menu_basic_path + "construct/", mode=0o777)
            os.mkdir(self.__menu_basic_path + "metadata_cache/", mode=0o777)

    def init_local_cache_info_to_master(self):
        # init_layer_list = self.lru.init_cache_add()
        init_layer_list = self.arc.init_cache_add(self.arc)
        for layer_dict in init_layer_list:
            self.changes_sync_add(
                {"layer": {"size_update": layer_dict}}, "layer", "size_update"
            )
        print(f"now the cache size is {self.cache_size}")

    def init_daemons(self):
        hub_to_get_daemons_thr = threading.Thread(target=self.hub_to_get_daemons)
        hub_to_get_daemons_thr.start()

        # parallel_uploading_daemons_thr = threading.Thread(target=self.bro_to_send_daemons)
        parallel_uploading_daemons_thr = multiprocessing.Process(
            target=self.bro_to_send_daemons
        )
        parallel_uploading_daemons_thr.start()

        decompress_rm_daemons_thr = threading.Thread(target=self.decompress_rm_daemons)
        decompress_rm_daemons_thr.start()

        local_rm_daemons_thr = threading.Thread(target=self.local_rm_daemons)
        local_rm_daemons_thr.start()

        changes_sync_daemons_thr = threading.Thread(target=self.changes_sync_daemons)
        changes_sync_daemons_thr.start()

        coming_soon_sync_daemons_thr = threading.Thread(
            target=self.coming_soon_sync_daemons
        )
        coming_soon_sync_daemons_thr.start()

        deploy_construct_daemons_thr = threading.Thread(
            target=self.deploy_construct_daemons
        )
        deploy_construct_daemons_thr.start()

        asyn_cache_update_daemons_pro = threading.Thread(
            target=self.cache_update_daemons
        )
        asyn_cache_update_daemons_pro.start()

        parallel_downloading_daemons_pro = multiprocessing.Process(
            target=self.downloading_daemons
        )
        # args=(self.downloading_queue,))
        parallel_downloading_daemons_pro.start()

        return hub_to_get_daemons_thr
        # hub_to_get_daemons_thr.join()

    def thr_downloading(self, args):
        downloading_dict = args
        get_size = get_single_layer_from_hub(
            downloading_dict["ublob"],
            downloading_dict["image_name"],
            downloading_dict["save_path"],
            downloading_dict["tmp_path"],
        )
        my_id = (
            "sha256:" + downloading_dict["ublob"]
            if "sha256:" not in downloading_dict["ublob"]
            else downloading_dict["ublob"]
        )

        print(f"thr_downloading | downloading_dict['ublob']: {my_id} -> finish")

        self.coming_soon_update_queue.put(
            {"action": "rm", "granularity": "layer", "my_id": my_id[7:]}
        )  # if my_id[7:] in self.coming_soon_set["layer"] else 0

    def coming_soon_sync_daemons(self):
        print("coming_soon_sync_daemons | 正常运行...")
        while True:
            my_dict = self.coming_soon_update_queue.get()
            # print(f"coming_soon_sync_daemons | my_dict = {my_dict}")
            action, granularity, my_id = (
                my_dict["action"],
                my_dict["granularity"],
                my_dict["my_id"],
            )
            self.coming_soon_sync(action, granularity, my_id)

    def coming_soon_sync(self, action, granularity, my_id):
        if action == "rm":
            self.coming_soon_set[granularity].discard(my_id)
            # print(f"coming_soon_sync | 已经删除 {my_id}, coming_soon_set = {self.coming_soon_set[granularity]}")
        elif action == "add":
            self.coming_soon_set[granularity].add(my_id)
            # print(f"coming_soon_sync | 已经添加 {my_id}, coming_soon_set = {self.coming_soon_set[granularity]}")

    def cache_update_daemons(self):
        pool = ThreadPoolExecutor()

        while True:
            my_dict = self.cache_update_queue.get()
            task = pool.submit(self.cache_update, (my_dict))
            task.add_done_callback(thread_exception)

    def v1_downloading_daemons(self):
        pool = ThreadPoolExecutor(max_workers=self._download_conn)

        while True:
            obj = self.downloading_queue.get()
            for ublob in obj["need_ublob_list"]:
                obj["ublob"] = ublob
                task = pool.submit(self.thr_downloading, (obj, ublob))
                task.add_done_callback(thread_exception)
                print(
                    f"downloading_daemons | ublob: {ublob} -> pool.submit | worker_num: {pool._work_queue.qsize()}"
                )

    def downloading_daemons(self):
        pool = ThreadPoolExecutor(max_workers=self._download_conn)
        while True:
            obj = self.downloading_queue.get()  # ublob, image_name, save_path
            task = pool.submit(self.thr_downloading, (obj))
            task.add_done_callback(thread_exception)
            # print(f"downloading_daemons | ublob: {obj['ublob']} -> pool.submit | worker_num: {pool._work_queue.qsize()}")

    def get_layer_from_hub(
        self, my_dict
    ):  # {"layer_list": layer_list, "image_name": image_name, "image_id": image_id, "work_type": work_type}
        if my_dict["work_type"] == "deploy":
            for my_layer in my_dict["layer_list"]:
                downloading_dict = {
                    "ublob": my_layer,
                    "image_name": my_dict["image_name"],
                    "save_path": self.__menu_basic_path
                    + "construct/"
                    + my_dict["image_id"]
                    + "/",
                    "tmp_path": self.__menu_basic_path + "tmp/",
                }
                self.downloading_queue.put(downloading_dict)

            # v1-----------------------------------------------------------------------------
            # downloading_dict = get_image_layer_from_hub(my_dict["layer_list"], my_dict['image_name'],
            #                                             self.__menu_basic_path + "construct/" + my_dict[
            #                                                 'image_id'] + "/")
            # if downloading_dict:
            #     self.downloading_queue.put(downloading_dict)
            # v1-----------------------------------------------------------------------------

            # for my_layer in my_dict["layer_list"]:
            #     with self.coming_soon_lock:
            #         self.coming_soon_set["layer"].remove(my_layer[7:]) if my_layer[7:] in self.coming_soon_set[
            #             "layer"] else 0

        # else:
        #     print(f"get_layer_from_hub | digest_name_list[2] == '0', 后续涉及缓存的主动更新")
        #     layer_gzip_size = get_single_layer_from_hub(digest_name_list[0], digest_name_list[1],
        #                                                     self.__menu_basic_path + "layer/")
        #     tmp_dict = {"layer": {"size_update": {digest_name_list[0][7:]: layer_gzip_size}}}
        #     self.changes_sync_add(tmp_dict, "layer", "size_update")  # self.changes_sync_add(tmp_dict, res[1], res[0])
        #     # print("get_layer_from_hub 完成 changes_sync_add...")

        # with self.coming_soon_lock:
        #     self.coming_soon_set["layer"].remove(digest_name_list[0][7:]) if digest_name_list[0][7:] in \
        #                                                                      self.coming_soon_set["layer"] else 0
        return 0

    def trans_layer_to_bro(self, args):
        """
        update header
        update body
        proxy_uploader参数： [ip, ip_digestList_dict[ip]["layer"], ip_digestList_dict[ip]["file"],
                                     ip_digestList_dict[ip]["trans_id"]
        """
        # FIXME: import能修正到更加合适的位置吗，这样子每次调用都要import
        # self._upload_conn.acquire()
        from node_proxy_module import proxy_uploader

        ip_digestList_list = args
        print("trans_layer_to_bro开始传输 -> ", ip_digestList_list[0])
        result = proxy_uploader(
            ip_digestList_list[0],
            "9000",
            ip_digestList_list[1],
            ip_digestList_list[2],
            ip_digestList_list[3],
        )

        return True

    def v2_trans_layer_to_bro(self, args):
        """

        首先定位到layer/file（已是gzip压缩格式） -> 获取path
        update header
        update body
        proxy_uploader参数： [ip, ip_digestList_dict[ip]["layer"], ip_digestList_dict[ip]["file"],
                                     ip_digestList_dict[ip]["trans_id"]
        """
        # self._upload_conn.acquire()
        from node_proxy_module import proxy_uploader

        ip_digestList_list = args
        # for layer_id in ip_digestList_list[1]:
        result = proxy_uploader(
            ip_digestList_list[0],
            "9000",
            ip_digestList_list[1],
            ip_digestList_list[2],
            ip_digestList_list[3],
        )
        # self._upload_conn.release()

        # 此时完成传输，校对wait_rm_rf
        if result == 200:
            if self._wait_rm_dict["layer"] or self._wait_rm_dict["file"]:
                self.local_to_del_add(self._wait_rm_dict)
                self._wait_rm_dict["layer"].clear()
                self._wait_rm_dict["file"].clear()
        return True

    def v1_thread_callback_sync(self, res):
        """
        :param res[0]: operation type
        :param res[1]: granularity
        :param res[2]: digest id (不应带“sha256:”)
        :param res[3]: gzip_size
        :final add tmp_dict[granularity]["size_update"/"rm"], granularity, operation type to queue
        """
        res, tmp_dict = res.result(), None
        print("thread_callback_sync res:", res)

        if res[0] == "size_update":
            tmp_dict = {res[1]: {res[0]: {res[2]: res[3]}}}
            print(f"待测试 | thread_callback_sync | tmp_dict = {tmp_dict}")
        elif res[0] == "rm":  # "rm",
            tmp_dict = {res[1]: {res[0]: [res[2]]}}
        self.changes_sync_add(tmp_dict, res[1], res[0])
        # print("thread_callback_sync 完成sync同步...", res)

    def thread_callback_sync(self, res):
        """
        bro_trans, update  coming_soon_set

        :param res[0]: granularity
        :param res[1]: pure_lf_name[:-7]], i.e., layer digest
        """
        res = res.result()
        self.coming_soon_update_queue.put(
            {"action": "rm", "granularity": "layer", "my_id": res[1]}
        ) if res[1] in self.coming_soon_set["layer"] else 0
        print(
            f"thread_callback_sync | bro_trans layer_id = {res[1]} -> coming_soon_set"
        )

    def decompress_rm_daemons(self):
        pool = ThreadPoolExecutor()
        while True:
            # print("decompress_rm_daemons守护中...")
            wait_obj = self._decompress_rm_queue.get()
            task = pool.submit(
                decompress_layerFile,
                (wait_obj[0], wait_obj[1], wait_obj[2], wait_obj[3], wait_obj[4]),
            )
            task.add_done_callback(self.thread_callback_sync)

    def local_rm_daemons(self):
        """

        rm_obj[0]: id
        rm_obj[1]: granularity
        """
        pool = ThreadPoolExecutor()
        while True:
            rm_obj = self._local_to_del_queue.get()
            obj_path = self.__menu_basic_path + rm_obj[1] + "/" + rm_obj[0]
            task = pool.submit(rm_single_layerFile, (obj_path, rm_obj[1], rm_obj[0]))
            # task.add_done_callback(self.thread_callback_sync)

    def changes_sync_daemons(self, interval=0.2):
        """
        size_update -> exit:1, size: int ; when a new cache layer get in
            self._changes_sync_dict["layer"]["size_update"] = {image_id: size, ...}
        rm -> exit:0 ; when a cache layer get out
            self._changes_sync_dict["layer"]["rm"] = [image_id, ...]

        note that add is not used in local node, but in master
        需要区分cache是local维护还是master维护。前者由cache alg来调用rm和size_update，后者需要区分是deploy需要还是cache需要

        """
        from node_proxy_module import proxy_changes_syncer

        while True:
            with self._changes_sync_lock:
                if (
                    self._changes_sync_dict["layer"]["size_update"]
                    or self._changes_sync_dict["layer"]["rm"]
                    or self._changes_sync_dict["image"]["size_update"]
                ):
                    result = proxy_changes_syncer(
                        "192.168.1.70", "9000", self.host_name, self._changes_sync_dict
                    )
                    print("changes_sync_daemons result:", result)
                    self._changes_sync_dict["layer"]["rm"].clear()
                    self._changes_sync_dict["layer"]["size_update"].clear()
                    self._changes_sync_dict["image"]["size_update"].clear()
                    # print("已清空_changes_sync_dict...")
            time.sleep(interval)

    def check_if_local_exit(self, granularity, pure_digest):
        if os.path.exists(self.menu_basic_path + granularity + "/" + pure_digest[:-7]):
            return True
        return False

    def hub_to_get_add(self, digest_name_dict, image_id="0", work_type=""):

        layer_list = []
        for layer_id in digest_name_dict:
            layer_id = "sha256:" + layer_id if "sha256" not in layer_id else layer_id
            layer_list.append(layer_id)
        image_name = digest_name_dict[layer_list[0]]
        self._hub_to_get_queue.put(
            {
                "layer_list": layer_list,
                "image_name": image_name,
                "image_id": image_id,
                "work_type": work_type,
            }
        )

    def bro_to_send_add(self, ip_digestList_dict):
        for ip in ip_digestList_dict:
            # [ip, [layer_digest1,], [file_digest1]]
            self._bro_to_send_queue.put(
                [
                    ip,
                    ip_digestList_dict[ip]["layer"],
                    ip_digestList_dict[ip]["file"],
                    ip_digestList_dict[ip]["trans_id"],
                ]
            )

    def local_to_del_add(self, tmp_dict):
        for layer in tmp_dict["layer"]:
            self._local_to_del_queue.put([layer, "layer"])

    def bro_get_check_add(self, tmp_dict):
        print("加入队列")

    def decompress_rm_add(self, tmp_list):
        self._decompress_rm_queue.put(tmp_list)

    def wait_rm_add(self, granularity, lf_id):
        self._wait_rm_dict[granularity].append(lf_id)

    def changes_sync_add(self, tmp_dict, granularity, operation):
        with self._changes_sync_lock:
            if operation == "rm":
                self._changes_sync_dict[granularity][operation] += tmp_dict[
                    granularity
                ][operation]
            elif operation == "size_update":
                for my_id in tmp_dict[granularity][operation]:
                    self._changes_sync_dict[granularity][operation][my_id] = tmp_dict[
                        granularity
                    ][operation][my_id]

    def deploy_construct_add(self, tmp_list):
        self._deploy_construct_queue.put(tmp_list)

    def hub_to_get_daemons(self):
        while True:
            self.get_layer_from_hub(self._hub_to_get_queue.get())

    def bro_to_send_daemons(self):
        pool = ThreadPoolExecutor(max_workers=self._download_conn)

        while True:
            obj = self._bro_to_send_queue.get()
            task = pool.submit(self.trans_layer_to_bro, (obj))
            task.add_done_callback(thread_exception)

    def deploy_construct_daemons(self):
        pool = ThreadPoolExecutor()
        while True:
            wait_obj = self._deploy_construct_queue.get()
            print(f"deploy_construct_daemons work | wait_obj = {wait_obj}")

            task = pool.submit(
                self.lifecyle_manager, (wait_obj[0], wait_obj[1], wait_obj[2])
            )
            task.add_done_callback(thread_exception)

    def bro_get_check_daemons(self):
        print("守护中...")

    def cache_update(self, my_dict):
        start_time = time.time()

        image_id, image_name, my_id, layer_path = (
            my_dict["image_id"],
            my_dict["image_name"],
            my_dict["my_id"],
            my_dict["layer_path"],
        )
        # print(f"cache_update | local cache update, layer id is {my_id}.")
        layer_size = int(
            os.path.getsize(os.path.join(layer_path, "layer.tar")) / 1024 / 1024
        )
        with self._cache_update_lock:
            # do_size_update, remove_back_layer_list = self.lru.set(key=my_id, image_name=image_name, image_id=image_id,
            #                                                       size=layer_size)
            do_size_update, remove_back_layer_list = self.arc.set(
                key=my_id, image_name=image_name, image_id=image_id, size=layer_size
            )

        midle_time = time.time()
        # print(f"cache_update | lru.set = {midle_time-start_time}")

        if do_size_update:
            # move到cacheDB中
            target_path = os.path.join(self.__menu_basic_path, "layer")
            shutil.move(
                os.path.join(self.__menu_basic_path, "construct", image_id, my_id),
                target_path,
            )
            # target_path = os.path.join(self.__menu_basic_path, "layer", my_id)
            # cache_copy_compress_rename(layer_path, target_path, "layer.tar", "layer_gzip.tar")
            self.changes_sync_add(
                {"layer": {"size_update": {my_id: layer_size}}}, "layer", "size_update"
            )
        last_time = time.time()
        if len(remove_back_layer_list) > 0:
            self.local_to_del_add({"layer": remove_back_layer_list})
            self.changes_sync_add(
                {"layer": {"rm": remove_back_layer_list}}, "layer", "rm"
            )
            # print(f"cache_update | remove time = {time.time()-last_time}")
        self.cache_updating_now_dict[image_id].remove(my_id)
        print(
            f"cache_update | layer: {my_id}, whole use time = {time.time()-start_time}"
        )

    def lifecyle_manager(self, args):
        image_id, image_name, lack_dict = args
        print(f"lifecyle_manager | image_id, lack_dict = {args}")

        # test
        time_start = time.time()

        construct_path = self.__menu_basic_path + "construct/" + image_id + "/"
        layer_list = layer_list_get(construct_path + "manifest.json")
        not_ready_list = layer_list.copy()

        metadata_list = os.listdir(construct_path)
        tar_path = os.path.join(
            self.__menu_basic_path, "construct", image_id, image_id + ".tar"
        )  # tar包也生成在dir内部
        tar = tarfile.open(tar_path, "w", dereference=True)  # 针对缓存的软链接

        # First for metadata integration.
        for metadata in metadata_list:
            print(
                f"add path: ",
                os.path.join(self.__menu_basic_path, "construct", image_id, metadata),
            )
            tar.add(
                os.path.join(self.__menu_basic_path, "construct", image_id, metadata),
                arcname=metadata,
            )  # just add the file use "arcname=metadata"
            print(f"now we finish the construct of metadata -> {metadata}")
        print(f"开始进行镜像重构，初始需要的lf为{not_ready_list}.")

        # Then the integration of data is performed.
        # cache_list = []
        while len(not_ready_list):
            # print(f"There is layer still not ready for constructing, coming_soon_set = {self.coming_soon_set}")
            # tar_path = os.path.join(self.__menu_basic_path, "construct", image_id, image_id+".tar")  # tar包也生成在dir内部
            # tar = tarfile.open(tar_path, "w")
            for my_id in not_ready_list:
                my_layer_construct_dir = os.path.join(
                    self.__menu_basic_path, "construct", image_id, my_id
                )
                if (
                    os.path.exists(my_layer_construct_dir)
                    and len(os.listdir(my_layer_construct_dir)) > 0
                ):  # 从tmp mv
                    layer_json_creat(
                        construct_path, my_id, layer_list, image_id
                    )  # 写入json
                    tar.add(
                        os.path.join(
                            self.__menu_basic_path, "construct", image_id, my_id
                        ),
                        arcname=my_id,
                    )
                    # cache_list.append({"image_id": image_id, "image_name": image_name, "my_id": my_id,
                    #                    "layer_path": os.path.join(self.__menu_basic_path, "construct", image_id,
                    #                                               my_id)})
                    self.cache_update_queue.put(
                        {
                            "image_id": image_id,
                            "image_name": image_name,
                            "my_id": my_id,
                            "layer_path": os.path.join(
                                self.__menu_basic_path, "construct", image_id, my_id
                            ),
                        }
                    )
                    if image_id not in self.cache_updating_now_dict:
                        self.cache_updating_now_dict[image_id] = []
                        self.cache_updating_now_dict[image_id].append(my_id)
                    else:
                        self.cache_updating_now_dict[image_id].append(my_id)
                    not_ready_list.remove(my_id)
                elif os.path.exists(
                    os.path.join(self.__menu_basic_path, "layer", my_id)
                ):  # 缓存中找
                    start_time = time.time()
                    os.mkdir(construct_path + my_id)  # make layer dir
                    src = os.path.join(
                        self.__menu_basic_path, "layer", my_id, "layer.tar"
                    )
                    dst = os.path.join(
                        self.__menu_basic_path,
                        "construct",
                        image_id,
                        my_id,
                        "layer.tar",
                    )
                    os.symlink(src, dst, target_is_directory=False)
                    layer_json_creat(
                        construct_path, my_id, layer_list, image_id
                    )  # 写入json
                    # tar.add(os.path.join(self.__menu_basic_path, "construct", image_id, my_id), arcname=my_id, dereference=True)
                    for root, dirs, files in os.walk(
                        os.path.join(
                            self.__menu_basic_path, "construct", image_id, my_id
                        )
                    ):
                        for file in files:
                            abs_path = os.path.join(root, file)
                            info = tar.gettarinfo(abs_path, arcname=file)
                            if os.path.islink(abs_path):
                                link_target = os.readlink(abs_path)
                                info.type = tarfile.SYMTYPE
                                info.linkname = link_target
                            tar.addfile(info, open(abs_path, "rb"))
                    not_ready_list.remove(my_id)
                    # coming_soon?
                    print(
                        f"lifecyle_manager | cache add to tar, use time = {time.time() - start_time}"
                    )
                else:
                    if (
                        my_id not in self.coming_soon_set["layer"]
                    ):  # layer_list里头取的，这儿先不考虑file了
                        self.get_layer_from_hub(
                            {
                                "layer_list": [my_id],
                                "image_name": image_name,
                                "image_id": image_id,
                                "work_type": "deploy",
                            }
                        )
                        self.coming_soon_update_queue.put(
                            {"action": "add", "granularity": "layer", "my_id": my_id}
                        )
                    continue
                print(f"now we finish the construct of layer -> {my_id}")
            time.sleep(0.3) if len(not_ready_list) else 0
        tar.close()

        pro = subprocess.Popen(
            "docker load -i " + tar_path, shell=True, stdout=subprocess.PIPE
        )
        pro.wait()

        image_tar_size = int(os.path.getsize(tar_path) / 1024 / 1024)
        print(f"image_tar_size -> {image_tar_size}")

        self.changes_sync_add(
            {"image": {"size_update": {image_id: image_tar_size}}},
            "image",
            "size_update",
        )
        print("changes_sync_add -> image_size_update finished...")

        pull_and_tar_time = time.time() - time_start
        print(f"lifecyle_manager | pull_and_tar_time = {pull_and_tar_time}")

        if image_id in self.cache_updating_now_dict:
            while len(self.cache_updating_now_dict[image_id]) > 0:
                time.sleep(1)
        shutil.rmtree(os.path.join(self.__menu_basic_path, "construct", image_id))

        from node_proxy_module import proxy_notify_kubectl_delete

        proxy_notify_kubectl_delete(
            "192.168.1.70",
            "9000",
            self.host_name,
            image_id,
            image_name,
            pull_and_tar_time,
            self.arc._cache_hit,
        )
