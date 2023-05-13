import copy
from collections import namedtuple
from typing import Dict, Optional
from collections import deque

# Layer = namedtuple('Layer', ['image_name', 'image_id', 'layer_id', 'size', 'freq', 'exit'])
# Image = namedtuple('Image', ['image_name', 'image_id', 'size', 'freq', 'exit'])


class Layer(object):
    #
    def __init__(self, layer_id: str, image_name: str, image_id: str, size: int):
        self.key: str = layer_id
        self.prev: Optional[Layer] = None
        self.post: Optional[Layer] = None

        self.image_name = image_name
        self.image_id = image_id
        self.size = size
        self.freq: int = 1
        self.exit: int = 1


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
        return " -> ".join(nodes) + f"| head = {self.head.key}, tail = {self.tail.key}"


class LRU(object):
    def __init__(self, capacity: int):
        self.capacity: int = capacity
        self.keys: Dict[str, Layer] = dict()
        self.link: DoubleLink = DoubleLink()

    def set(self, key: str, image_name: str, image_id: str, size: int):
        remove_back_layer_list = []
        if key in self.keys:
            node = self.keys[key]
            node.freq += 1
            r_node = self.link.remove(node)
            self.link.append_front(r_node)
            print(self.link.size, self.link)
            return remove_back_layer_list

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
        # print("size: ", self.link.size, ", link:", self.link)

        return remove_back_layer_list

    def get(self, key: str):
        if key not in self.keys:
            return None
        node = self.keys[key]
        r_node = self.link.remove(node)
        self.link.append_front(r_node)
        # print(f"layer_id -> {key}, node_size -> {node.size}, node_freq -> {node.freq}")

        return node.size, node.freq


class LFU(object):
    def __init__(self, capacity: int):
        self.capacity: int = capacity
        self.keys: Dict[str, Layer] = dict()
        self.link: DoubleLink = DoubleLink()

    def set(self, key: str, image_name: str, image_id: str, size: int) -> list:
        remove_back_layer_list = []
        if key in self.keys:
            node = self.keys[key]
            node.freq += 1
            while node != self.link.head and node.prev.freq <= node.freq:
                if node == self.link.tail:
                    # print("position -> 1")
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
            return remove_back_layer_list
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

        return remove_back_layer_list

    def get(self, key: str):
        if key not in self.keys:
            return None
        node = self.keys[key]
        r_node = self.link.remove(node)
        self.link.append_front(r_node)

        return (
            node.size,
            node.freq,
        )


class ARC(object):
    def __init__(self, maxsize):
        self._cache = {}
        self._T1 = deque()
        self._B1 = deque()
        self._T2 = deque()
        self._B2 = deque()
        self._maxsize = maxsize
        self._ratio = 0
        self._cache_hit = 0

    def get(self, key):
        try:
            value = self._cache[key]
            self._adapt(key, value.size)
            return value
        except Exception as e:
            print("miss", e)
            return "miss"

    def set(self, key: str, image_name: str, image_id: str, size: int) -> list:
        if self._maxsize < size:
            raise
        else:
            self._adapt(key, size)
            node = Layer(
                layer_id=key, image_name=image_name, image_id=image_id, size=size
            )
            self._cache[key] = node

    def _adapt(self, key, size):
        cnt = (self._T1, self._B1, self._T2, self._B2)

        lt1, lb1, lt2, lb2 = (
            sum([self._cache[one_key].size for one_key in one_list]) for one_list in cnt
        )
        if key in self._cache:
            if key in self._T1:
                self._T1.remove(key)
            if key in self._T2:
                self._T2.remove(key)
            self._T2.append(key)
            self._cache_hit += 1

        elif key in self._B1:
            self._ratio = min(self._maxsize, self._ratio + max(1, lb2 / lb1))
            self._replace(key)
            self._B1.remove(key)
            self._T2.append(key)

        elif key in self._B2:
            self._ratio = max(self._maxsize, self._ratio - max(1, lb1 / lb2))
            self._replace(key)
            self._B2.remove(key)
            self._T2.append(key)

            flag = 0
            if size > self._maxsize:
                raise
            while lt1 + lb1 + size > self._maxsize:
                if lt1 == self._maxsize:
                    self._B1.popleft()
                    if flag == 0:
                        self._replace(key)
                        flag = 1
                    lt1, lb1, lt2, lb2 = (
                        sum([self._cache[one_key].size for one_key in one_list])
                        for one_list in cnt
                    )
                else:
                    del self._cache[self._T1.popleft()]
                    lt1, lb1, lt2, lb2 = (
                        sum([self._cache[one_key].size for one_key in one_list])
                        for one_list in cnt
                    )
            else:
                sm = lt1 + lt2 + lb1 + lb2
                if sm >= self._maxsize:
                    while sm >= 2 * self._maxsize:
                        self._B2.popleft()
                        lt1, lb1, lt2, lb2 = (
                            sum([self._cache[one_key].size for one_key in one_list])
                            for one_list in cnt
                        )
                        sm = lt1 + lt2 + lb1 + lb2
                    self._replace(key)
            self._T1.append(key)

    def contains(
        self,
    ):
        all_size = 0
        for node_index in self._cache:
            node = scheduler._cache[node_index]
            all_size = all_size + node.size
            print(f"layer_id -> {node.key}, node_size -> {node.size}")
        print(f"Cached size:{all_size}")
        return 0

    def clear(self):
        self._cache.clear()
        self._T1.clear()
        self._B1.clear()
        self._T2.clear()
        self._B2.clear()
        self._ratio = 0

    def _replace(self, key):
        l = len(self._T1)
        if l > 0 and (self._ratio < l or (l == self._ratio and key in self._B2)):
            x = self._T1.popleft()
            self._B1.append(x)
        else:
            x = self._T2.popleft()
            self._B2.append(x)
        del self._cache[x]


if __name__ == "__main__":
    # my_lru = LRU(2000)
    # my_lru.set(key="1", image_name="python1.2", image_id="image1", size=100)
    # my_lru.set(key="2", image_name="java.21", image_id="image2", size=200)
    # my_lru.set(key="1", image_name="python1.2", image_id="image1", size=100)
    # my_lru.set(key="3", image_name="go1.5", image_id="image3", size=300)
    # print(my_lru.get(key="3"))

    my_lfu = LFU(10)
    my_lfu.set(key="1", image_name="1", image_id="1", size=3)
    my_lfu.set(key="2", image_name="2", image_id="2", size=5)
    # my_lfu.set(key="3", image_name="3", image_id="3", size=1)
    my_lfu.set(key="4", image_name="4", image_id="4", size=1)
    print(my_lfu.link)
    # print(f"my_lfu.link.head = {my_lfu.link.head.key}, my_lfu.link.tail = {my_lfu.link.tail.key}")
    my_lfu.set(key="4", image_name="4", image_id="4", size=1)
    print(my_lfu.link)
    # print(f"my_lfu.link.head = {my_lfu.link.head.key}, my_lfu.link.tail = {my_lfu.link.tail.key}")
    my_lfu.set(key="5", image_name="5", image_id="5", size=4)
    print(my_lfu.link)
    # my_lfu.set(key="3", image_name="1", image_id="1", size=1)

    ##################### ARC-TEST ####################
    max_size = 10
    scheduler = ARC(max_size)

    scheduler.set(key="4", image_name="4", image_id="4", size=3)
    scheduler.set(key="5", image_name="5", image_id="5", size=2)
    scheduler.set(key="6", image_name="6", image_id="6", size=4)
    scheduler.set(key="7", image_name="7", image_id="7", size=1)
    scheduler.contains()
    print(f"MAX size: {max_size}\n")

    node = scheduler.get("7")
    try:
        print(f"layer_id -> {node.key}, node_size -> {node.size}")
    except:
        print(node)

    node = scheduler.get("4")
    try:
        print(f"layer_id -> {node.key}, node_size -> {node.size}")
    except:
        print(node)
