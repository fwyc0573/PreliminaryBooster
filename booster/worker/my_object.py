import copy
import os
from typing import Dict, Optional


class Layer(object):
    #
    def __init__(self, layer_id: str, image_name: str, image_id: str, size: int):
        self.key: str = layer_id
        # self.value: object = value
        self.prev: Optional[Layer] = None
        self.post: Optional[Layer] = None
        self.image_name = image_name
        self.image_id = image_id
        self.size = size
        self.freq: int = 1

class DoubleLink(object):

    def __init__(self):
        self.head: Optional[Layer] = None
        self.tail: Optional[Layer] = None
        self.size: int = 0

    def append_front(self, node: Layer):
        if self.size <= 0:
            node.prev = None
            node.post = None
            self.head = node
            self.tail = node
            self.size += node.size
            return
        old_head = self.head
        node.prev = None
        node.post = old_head
        if old_head:
            old_head.prev = node
        self.head = node
        self.size += node.size

    def append_tail(self, node: Layer):
        if self.size <= 0:
            node.prev = None
            node.post = None
            self.head = node
            self.tail = node
            self.size += node.size
            return
        old_tail = self.tail
        node.post = None
        node.prev = old_tail
        if old_tail:
            old_tail.post = node
        self.tail = node
        self.size += node.size

    def remove(self, node: Layer):
        prev = node.prev
        post = node.post
        if node == self.head:
            self.head = post
        if node == self.tail:
            self.tail = prev
        if prev:
            prev.post = post
        if post:
            post.prev = prev

        node.prev = None
        node.post = None
        self.size -= node.size
        return node

    def pop_back(self):
        tail = self.tail
        if not tail:
            return tail
        prev_tail = tail.prev
        self.tail = prev_tail
        self.size -= tail.size
        if prev_tail:
            prev_tail.post = None
        return tail

    def __str__(self):
        if self.size <= 0:
            return ""
        head = self.head
        nodes = []
        while head:
            nodes.append(f"【layer: {head.key}, freq: {head.freq}, size: {head.size}】")
            head = head.post
        return " -> ".join(nodes) + f"| head = {self.head.key}, tail = {self.tail.key}, link_size = {self.size}"


class LRU(object):

    def __init__(self, capacity: int, local_cache_path: str):
        self.capacity = capacity
        self.local_cache_path = local_cache_path
        self.keys: Dict[str, Layer] = dict()
        self.link: DoubleLink = DoubleLink()
        # self.init_cache_add()

    def get_left_space(self):
        return self.capacity - self.link.size

    def init_cache_add(self):
        item_lst = os.listdir(self.local_cache_path)
        init_layer_list = []
        for layer_id in item_lst:
            path_item = os.path.join(self.local_cache_path, layer_id)
            layer_size = int(os.path.getsize(os.path.join(path_item, "layer.tar"))/1024/1024)
            _, remove_back_layer_list = self.set(key=layer_id, image_name="", image_id="", size=layer_size)
            init_layer_list.append({layer_id: layer_size})
            if len(remove_back_layer_list) > 0:
                raise
        print(f"init_cache_add | 完成init, self.link = {self.link}")
        return init_layer_list


    def set(self, key: str, image_name: str, image_id: str, size: int) -> bool and list:
        remove_back_layer_list = []
        if key in self.keys:
            node = self.keys[key]
            node.freq += 1
            if size != node.size:
                node.size = size
                self.link.size += size - node.size
            node.image_name, node.image_id = image_name, image_id
            r_node = self.link.remove(node)
            self.link.append_front(r_node)
            print(self.link.size, self.link)
            return False, remove_back_layer_list

        node = Layer(layer_id=key, image_name=image_name, image_id=image_id, size=size)
        while self.link.size + node.size > self.capacity:
            p_node = self.link.pop_back()
            if p_node:
                remove_back_layer_list.append(p_node.key)
                del self.keys[p_node.key]
                del p_node
            else:
                raise
        self.keys[key] = node
        self.link.append_front(node)
        return True, remove_back_layer_list


    def get(self, key: str):
        if key not in self.keys:
            return None
        node = self.keys[key]
        return node.size, node.freq


class LFU(object):

    def __init__(self, capacity: int, local_cache_path: str):
        self.capacity = capacity
        self.local_cache_path = local_cache_path
        self.keys: Dict[str, Layer] = dict()
        self.link: DoubleLink = DoubleLink()
        # self.init_cache_add()

    def get_left_space(self):
        return self.capacity - self.link.size

    def init_cache_add(self):
        item_lst = os.listdir(self.local_cache_path)
        init_layer_list = []
        for layer_id in item_lst:
            path_item = os.path.join(self.local_cache_path, layer_id)
            layer_size = int(os.path.getsize(os.path.join(path_item, "layer.tar"))/1024/1024)
            _, remove_back_layer_list = self.set(key=layer_id, image_name="", image_id="", size=layer_size)
            init_layer_list.append({layer_id: layer_size})
            if len(remove_back_layer_list) > 0:
                raise
        print(f"init_cache_add | 完成init, self.link = {self.link}")
        return init_layer_list

    def set(self, key: str, image_name: str, image_id: str, size: int) -> bool and list:
        remove_back_layer_list = []
        if key in self.keys:
            node = self.keys[key]
            node.freq += 1
            if size != node.size:
                node.size = size
                self.link.size += size - node.size
            node.image_name, node.image_id = image_name, image_id
            while node != self.link.head and node.prev.freq <= node.freq:
                if node == self.link.tail:
                    self.link.tail = node.prev
                    node.prev.prev.post = node
                    node.prev.post = None
                    node.post = node.prev
                    node.prev = node.prev.prev
                    node.prev.prev = node
                elif node.prev == self.link.head:
                    # print("position -> 2")
                    self.link.head = node
                    old_me = copy.copy(node)
                    node.prev = None
                    node.post = old_me.prev
                    old_me.prev.post = old_me.post
                    old_me.post.prev = old_me.prev
                else:
                    # print("position -> 3")
                    old_me = copy.copy(node)
                    node.post = old_me.prev
                    node.prev = old_me.prev.prev
                    old_me.prev.prev.post = node
                    old_me.prev.prev = node
                    old_me.prev.post = old_me.post
                    old_me.post.prev = old_me.prev
            return False, remove_back_layer_list
        # print("position -> 4")
        node = Layer(layer_id=key, image_name=image_name, image_id=image_id, size=size)

        while self.link.size + node.size > self.capacity:
            p_node = self.link.pop_back()
            if p_node:
                remove_back_layer_list.append(p_node.key)
                del self.keys[p_node.key]
                del p_node
            else:
                raise
        self.keys[key] = node
        self.link.append_tail(node)

        return True, remove_back_layer_list

    def get(self, key: str):
        if key not in self.keys:
            return None
        node = self.keys[key]

        return node.size, node.freq,


class NAIVEALG(object):

    def __init__(self, capacity: int, local_cache_path: str):
        self.capacity = capacity
        self.local_cache_path = local_cache_path
        self.keys: Dict[str, Layer] = dict()
        self.link: DoubleLink = DoubleLink()
        # self.init_cache_add()

    def get_left_space(self):
        return self.capacity - self.link.size

    def init_cache_add(self):
        item_lst = os.listdir(self.local_cache_path)
        init_layer_list = []
        for layer_id in item_lst:
            path_item = os.path.join(self.local_cache_path, layer_id)
            layer_size = int(os.path.getsize(os.path.join(path_item, "layer.tar"))/1024/1024)
            _, remove_back_layer_list = self.set(key=layer_id, image_name="", image_id="", size=layer_size)
            init_layer_list.append({layer_id: layer_size})
            if len(remove_back_layer_list) > 0:
                raise
        return init_layer_list

    def set(self, key: str, image_name: str, image_id: str, size: int) -> bool and list:
        remove_back_layer_list = []
        # 刷新下初始化误差即可
        if key in self.keys:
            node = self.keys[key]
            node.freq += 1
            if size != node.size:
                node.size = size
                self.link.size += size - node.size
            node.image_name, node.image_id = image_name, image_id
            return False, remove_back_layer_list

        node = Layer(layer_id=key, image_name=image_name, image_id=image_id, size=size)
        if self.link.size + node.size > self.capacity:
            return False, remove_back_layer_list
        else:
            self.keys[key] = node
            self.link.append_front(node)
        return True, []

    def get(self, key: str):
        if key not in self.keys:
            return None
        node = self.keys[key]
        return node.size, node.freq


if __name__ == '__main__':
    # my_lru = LRU(2000, "/home/node71/edge_cloud_DB/layer")
    my_NAIVEALG = NAIVEALG(2000, "/home/node71/edge_cloud_DB/layer")
    my_NAIVEALG.set(key="1", image_name="python1.2", image_id="image1", size=100)
    my_NAIVEALG.set(key="2", image_name="python1.3", image_id="image2", size=100)
    my_NAIVEALG.set(key="3", image_name="python1.4", image_id="image3", size=100)
    my_NAIVEALG.set(key="3", image_name="python1.4", image_id="image3", size=100)
    my_NAIVEALG.set(key="3", image_name="python1.4", image_id="image3", size=100)
    my_NAIVEALG.set(key="4", image_name="python1.5", image_id="image4", size=5000)
    print(my_NAIVEALG.link)
    print(my_NAIVEALG.get(key="3"))

    # my_lru.set(key="1", image_name="python1.2", image_id="image1", size=100)
    # my_lru.set(key="2", image_name="java.21", image_id="image2", size=200)
    # my_lru.set(key="1", image_name="python1.2", image_id="image1", size=100)
    # my_lru.set(key="3", image_name="go1.5", image_id="image3", size=300)
    # print(my_lru.get(key="3"))

    # my_lfu = LFU(10, 1)
    # my_lfu.set(key="1", image_name="1", image_id="1", size=3)
    # my_lfu.set(key="2", image_name="2", image_id="2", size=5)
    # my_lfu.set(key="3", image_name="3", image_id="3", size=1)
    # my_lfu.set(key="4", image_name="4", image_id="4", size=1)
    # print(my_lfu.link)
    # print(f"my_lfu.link.head = {my_lfu.link.head.key}, my_lfu.link.tail = {my_lfu.link.tail.key}")
    # my_lfu.set(key="4", image_name="4", image_id="4", size=1)
    # print(my_lfu.link)
    # print(f"my_lfu.link.head = {my_lfu.link.head.key}, my_lfu.link.tail = {my_lfu.link.tail.key}")
    # my_lfu.set(key="5", image_name="5", image_id="5", size=4)
    # print(my_lfu.link)
    # my_lfu.set(key="3", image_name="1", image_id="1", size=1)
