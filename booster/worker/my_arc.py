import os
from collections import deque
import copy
from collections import namedtuple
from typing import Dict, Optional

class Layer(object):
    def __init__(self, layer_id: str, image_name: str, image_id: str, size: int):
        self.key: str = layer_id
        self.prev: Optional[Layer] = None
        self.post: Optional[Layer] = None
        self.image_name = image_name
        self.image_id = image_id
        self.size = size
        self.freq: int = 1
        self.exit: int = 1


class ARC(object):

    def __init__(self, maxsize: int, local_cache_path: str):
        self._cache = {}
        self._T1 = deque()
        self._B1 = deque()
        self._T2 = deque()
        self._B2 = deque()
        self._maxsize = maxsize
        self._ratio = 0
        self._cache_hit = 0
        self.local_cache_path = local_cache_path

    def init_cache_add(self, arc):
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

    def get(self, key):
        try:
            value = self._cache[key]
            return value#, hit_flag, remove_list
        except Exception as e:
            print('miss', e)
            return 'miss', False, []

    def set(self, key: str, image_name: str, image_id: str, size: int):
        if self._maxsize < size:
            return False, []
        else:
            hit_flag, remove_list = self._adapt(key, size)  # 当添加新元素时，更新列表
            tmp_node = Layer(layer_id=key, image_name=image_name, image_id=image_id, size=size)
            self._cache[key] = tmp_node
        return not hit_flag, remove_list

    def _adapt(self, key, size):
        hit_flag_adapt = True
        remove_list = []
        cnt = (self._T1, self._B1, self._T2, self._B2)
        lt1, lb1, lt2, lb2 = (sum([self._cache[one_key].size for one_key in one_list]) for one_list in cnt)
        if key in self._cache:
            hit_flag_adapt = True
            if key in self._T1:
                self._T1.remove(key)
            if key in self._T2:
                self._T2.remove(key)
            self._T2.append(key)
            self._cache_hit += 1

        elif key in self._B1:
            hit_flag_adapt = True
            self._ratio = min(self._maxsize, self._ratio + max(1, lb2 / lb1))
            self._replace(key)
            self._B1.remove(key)
            self._T2.append(key)

        elif key in self._B2:
            self._ratio = max(self._maxsize, self._ratio - max(1, lb1 / lb2))
            self._replace(key)
            self._B2.remove(key)
            self._T2.append(key)

            hit_flag_adapt = False
            flag = 0
            if size > self._maxsize:
                print(f"该layer过大无法缓存 -> {size}M")
                raise

            # print('wai', lt1 + lb1 + size, self._maxsize)
            if lt1 + lb1 + size >= self._maxsize:
                while lt1 + lb1 + size >= self._maxsize:
                    # print('nei', lt1, lb1 ,lt2, lb2, size, self._maxsize)
                    if lt1 + size < self._maxsize:
                        remove_id = self._B1.popleft()
                        remove_list.append(remove_id)
                        if flag == 0:
                            self._replace(key)
                            flag = 1
                    else:
                        remove_id = self._T1.popleft()
                        remove_list.append(remove_id)
                        # print('DEL', remove_id)
                        del self._cache[remove_id]

                    cnt = (self._T1, self._B1, self._T2, self._B2)
                    lt1, lb1, lt2, lb2 = (sum([self._cache[one_key].size for one_key in one_list]) for one_list in cnt)

            else:
                sm = lt1 + lt2 + lb1 + lb2
                while sm >= self._maxsize:
                    while sm >= 2*self._maxsize:
                        remove_id =  self._B2.popleft()
                        remove_list.append(remove_id)
                        cnt = (self._T1, self._B1, self._T2, self._B2)
                        lt1, lb1, lt2, lb2 = (sum([self._cache[one_key].size for one_key in one_list]) for one_list in cnt)
                        sm = lt1 + lt2 + lb1 + lb2

                    self._replace(key)
                    lt1, lb1, lt2, lb2 = (sum([self._cache[one_key].size for one_key in one_list]) for one_list in cnt)
                    sm = lt1 + lt2 + lb1 + lb2
            self._T1.append(key)
        return hit_flag_adapt, remove_list

    def contains(self, scheduler):
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

    # 移除key对应的缓存项
    def _replace(self, key):
        l = len(self._T1)
        if l > 0 and (self._ratio < l or (l == self._ratio and key in self._B2)):
            x = self._T1.popleft()
            self._B1.append(x)
        else:
            x = self._T2.popleft()
            self._B2.append(x)
        del self._cache[x]


if __name__ == '__main__':
    max_size = 10
    scheduler = ARC(max_size, '')

    print(3, scheduler.set(key="4", image_name="4", image_id="14", size=3))
    print(2,scheduler.set(key="5", image_name="5", image_id="15", size=2))
    print(4,scheduler.set(key="6", image_name="6", image_id="16", size=4))
    print(1,scheduler.set(key="7", image_name="7", image_id="17", size=1))
    print(1,scheduler.set(key="7", image_name="7", image_id="17", size=1))
    print(4,scheduler.set(key="8", image_name="8", image_id="18", size=4))


    scheduler.contains()
    print(f"MAX size: {max_size}\n")
