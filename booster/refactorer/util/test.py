import json
from treelib import Tree
import random
import time
from multiprocessing import Pool
from multiprocessing.dummy import Pool as ThreadPool
import os
import tarfile
import shutil
import gzip
import hashlib
import pathlib
import copy
from pathlib import Path
from collections import Counter
TEST_FLAG = True

# 存为json, 处理后的数据转为json格式
def save_json(addr, data):
    with open(addr, "w") as f:
        json.dump(data, f)
    return 0

def del_file(filepath):
    try:
        os.remove(filepath)
        return
    except Exception as e:
        shutil.rmtree(filepath)
        return

def sha256_update_from_file(filename, hash):
    assert Path(filename).is_file()
    with open(str(filename), "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash.update(chunk)
    return hash


def sha256_file(filename):
    return sha256_update_from_file(filename, hashlib.sha256()).hexdigest()


def sha256_update_from_dir(directory, hash):
    assert Path(directory).is_dir()
    for path in sorted(Path(directory).iterdir()):
        hash.update(path.name.encode())
        if path.is_file():
            hash = sha256_update_from_file(path, hash)
        elif path.is_dir():
            hash = sha256_update_from_dir(path, hash)
    return hash


def sha256_dir(directory):
    return sha256_update_from_dir(directory, hashlib.sha256()).hexdigest()

def file_hash_coding(file_path, is_md5=False):
    try:
        with open(file_path, 'rb') as fp:
            data = fp.read()
            if fp.tell():
                return hashlib.md5(data).hexdigest() if is_md5 else hashlib.sha256(data).hexdigest()
            else:
                return 0
    except:
        return 0
        print('hash error', file_path)

def make_gz(source_dir, output_filename ):
    sourcename = source_dir.split('/')[-1]
    filename = output_filename.split('/')[-1]
    if not os.path.exists(output_filename.split(filename)[0]):
        os.makedirs(output_filename.split(filename)[0])
    with tarfile.open(output_filename, "w") as tar:#"w:gz"
        # tar.add(source_dir+'/', arcname=os.path.basename(output_filename.split('.')[0]))
        tar.addfile(source_dir+'/', arcname=os.path.basename(output_filename.split('.')[0]), dereference=True)
    return 0

def un_gz(file_name,target_dir):
    tar = tarfile.open(file_name)
    names = tar.getnames()
    for name in names:
        tar.extract(name, target_dir)
    return 0

# 获取文件md5
def get_MD5(file_path):
    files_md5 = os.popen('md5 %s' % file_path).read().strip()
    file_md5 = files_md5.replace('MD5 (%s) = ' % file_path, '')
    return file_md5

def copy_dirs(addr1, addr2):
    if not os.path.exists(addr2):
        os.makedirs(addr2)
    if os.path.exists(addr2):
        init_root = ''
        for root, dirs, files in os.walk(addr1):
            init_root = root
            break
        for root, dirs, files in os.walk(addr1):
            for dir in dirs:
                addr_dir = addr2 + root.split(init_root)[1] + '/' + dir
                if not os.path.exists(addr_dir):
                    os.makedirs(addr_dir)
            for file in files:
                src_file = os.path.join(root, file)
                target_path = addr2 + root.split(init_root)[1] + '/' + file
                shutil.copy(src_file, target_path, follow_symlinks=False)
    return 0

def get_tree(tree, node_id,hash_addr, layer_addr,img):
    t1 = time.time()
    with open(hash_addr, 'r') as file:
        hash_dic = json.loads(file.read())

    path_list = hash_dic[list(hash_dic)[0]][0]['path'].split('/')
    root_name = path_list[0]+'/'+path_list[1]

    init_id = node_id
    tree.create_node(tag=root_name, identifier=init_id, parent=tree.get_node(0), data={'name': root_name, 'size': 0, 'hash': 0, 'repeat': False, 'img':img})  # 根节点
    node_id += 1

    for one_hash in hash_dic:
        for one_file_dic in hash_dic[one_hash]:
            file_name = one_file_dic['name']
            file_path = one_file_dic['path'].split(root_name)[-1].split('/')
            # BUG：会出一个空元素
            del file_path[0]
            file_size = one_file_dic['size']
            pointer = init_id

            for one_dict in file_path:
                children_list = [node.tag for node in tree.children(pointer)]
                if one_dict not in children_list:
                    # if pointer==init_id:
                    #     print(one_dict, pointer, [node.tag for node in tree.children(0)])
                    tree.create_node(tag=one_dict, identifier=node_id, parent=pointer, data={'name': one_dict, 'size': 0, 'hash': 0, 'repeat': False, 'img':img})
                    pointer = node_id
                    node_id += 1
                else:
                    pointer = [node.identifier for node in tree.children(pointer) if node.tag == one_dict][0]
                    pass

            children_list = [node.tag for node in tree.children(pointer)]
            # if  file_name in children_list:
            #     print(file_name, children_list)
            assert file_name not in children_list
            tree.create_node(tag=file_name, identifier=node_id,
                             parent=pointer, data={'name': file_name, 'size': file_size, 'hash': one_hash, 'repeat': False, 'img':img})
            node_id += 1

    with open(layer_addr, 'r') as file:
        layer_dic = json.loads(file.read())

    for one_layer in layer_dic:
        pointer = init_id
        children_list = [node.tag for node in tree.children(0)]
        if one_layer not in children_list:
            tree.create_node(tag=one_layer, identifier=node_id, parent=pointer, data={'name': one_layer, 'size': 0, 'hash': 0, 'repeat': False, 'img':img})
            node_id += 1
        for cur_path in layer_dic[one_layer]:
            pointer = [node.identifier for node in tree.children(init_id) if node.tag == one_layer][0]
            cur_path_list = cur_path.split('/')
            del cur_path_list[0]
            cur_file_list = layer_dic[one_layer][cur_path]
            for one_dict in cur_path_list:
                children_list = [node.tag for node in tree.children(pointer)]
                # 若文件夹未建立，则创建节点
                if one_dict not in children_list:
                    tree.create_node(tag=one_dict, identifier=node_id,
                                     parent=pointer, data={'name': one_dict, 'size': 0, 'hash': -1, 'repeat': False, 'img':img})
                    pointer = node_id
                    node_id += 1
                else:
                    pointer = [node.identifier for node in tree.children(pointer) if node.tag == one_dict][0]
                    pass
            for one_file in cur_file_list:
                children_list = [node.data['hash'] for node in tree.children(pointer) if node.data['hash']!= 0]
                if one_file not in children_list:
                    tree.create_node(tag=one_file, identifier=node_id, parent=pointer, data={'name': one_file, 'size': 0, 'hash': 0, 'repeat': False, 'img':img})
                    node_id += 1

    t2 = time.time()
    # print("node number:%s, depth:%s, cost time:%s, node_id:%s" % (tree.size(), tree.depth(), t2 - t1, node_id))
    return tree, node_id

def same_node(node1, node2):
    if node1.tag != node2.tag:
        return False
    if node1.data['hash'] != node2.data['hash']:
        return False
    if node1.data['size'] != node2.data['size']:
        return False
    return True

def same_node_children(node1, node2, tree1):
    if node1.identifier == node2.identifier:
        return False
    if same_node(node1, node2) == False:
        return False

    node1_children = tree1.children(node1.identifier)
    node2_children = tree1.children(node2.identifier)
    if len(node1_children) != len(node2_children):
        return False

    for node in node1_children:
        if node.data['repeat'] == False:
            return False
    for node in node2_children:
        if node.data['repeat'] == False:
            return False

    node1_children_tag = [node.tag for node in node1_children]
    node2_children_tag = [node.tag for node in node2_children]
    for child_tag in node1_children_tag:
        if child_tag not in node2_children_tag:
            return False

    for child_index in range(len(node1_children_tag)):
        child2_index = node2_children_tag.index(node1_children_tag[child_index])
        if node1_children[child_index].data['hash'] != node2_children[child2_index].data['hash']:
            return False
        if node1_children[child_index].data['size'] != node2_children[child2_index].data['size']:
            return False
    return True

def split_list(list, num):
    out = []
    one_num = int(len(list)/num)
    if one_num == 0:
        one_num = 1
    pointer=0
    while pointer<len(list):
        out.append(list[pointer:pointer+one_num])
        pointer = pointer + one_num
    return out


def same_subtree(tree1):
    t1 = time.time()
    tree1_leaf = tree1.leaves()

    tt1 = time.time()
    hash_list = [str(node.data['hash'])+ str(node.data['size'])+node.tag for node in tree1_leaf]
    repeat_hash = {k:[] for k, v in Counter(hash_list).items()}

    for node in tree1_leaf:
        repeat_hash[str(node.data['hash'])+ str(node.data['size'])+node.tag].append(node)

    for hash_tag in repeat_hash:
        if len(repeat_hash[hash_tag])>1:
            for one_node in repeat_hash[hash_tag]:
                tmp_data = one_node.data
                tmp_data['repeat'] = True
                tree1.update_node(one_node.identifier, data=tmp_data)
    tt2 = time.time()

    tree1_depth_node_old = [node for node in tree1.leaves() if node.data['repeat']==True]

    while len(tree1_depth_node_old)+len(tree1_depth_node_old)>0:
        tree1_depth_node = []
        tree1_depth_node_id = []
        tree1_depth_node_old_repeat = [node for node in tree1_depth_node_old if node.data['repeat']==True]
        for child in tree1_depth_node_old_repeat:
            parent_node = tree1.parent(child.identifier)
            if parent_node is None:
                continue
            if parent_node.identifier not in tree1_depth_node_id and child.data['repeat']==True:
                tree1_depth_node.append(parent_node)
                tree1_depth_node_id.append(parent_node.identifier)

        tree1_depth_node_old = tree1_depth_node

        hash_list = [str(node.data['hash']) + str(node.data['size'])+ node.tag for node in tree1_depth_node]
        repeat_hash = {k: [] for k, v in Counter(hash_list).items()}
        for node in tree1_depth_node:
            repeat_hash[str(node.data['hash']) + str(node.data['size'])+ node.tag].append(node)

        for hash_tag in repeat_hash:
            node_list=repeat_hash[hash_tag]
            for node1 in node_list:
                for node2 in node_list:
                    if node1.identifier==node2.identifier:
                        continue
                    if node1.data['repeat'] == True and node2.data['repeat'] == True:
                        continue
                    if same_node_children(node1, node2, tree1):
                        tmp_data = node1.data
                        tmp_data['repeat'] = True
                        tmp_size = sum([node.data['size'] for node in tree1.children(node1.identifier)])
                        tmp_data['size'] = tmp_size
                        tree1.update_node(node1.identifier, data=tmp_data)

                        tmp_data = node2.data
                        tmp_data['repeat'] = True
                        tmp_size = sum([node.data['size'] for node in tree1.children(node2.identifier)])
                        tmp_data['size'] = tmp_size
                        tree1.update_node(node2.identifier, data=tmp_data)

    t3 = time.time()

    return tree1

def get_subtree_size(tree):
    subtree_root_list = []
    root = tree.all_nodes()[0]

    all_size = sum([node.data['size'] for node in tree.all_nodes()])
    all_repeat_node_tmp = [node for node in tree.all_nodes() if node.data['repeat'] == True and tree.parent(node.identifier).data['repeat'] != True and node.data['size'] > 0]
    hash_list = [str(node.data['hash']) + str(node.data['size']) + node.tag for node in all_repeat_node_tmp]
    repeat_hash = {k: [] for k, v in Counter(hash_list).items()}
    for node in all_repeat_node_tmp:
        repeat_hash[str(node.data['hash']) + str(node.data['size']) + node.tag].append(node)
    all_repeat_node = []
    for hash_one in repeat_hash:
        node_list = repeat_hash[hash_one]
        if len(node_list)>1:
            del node_list[0]
            for one_node in node_list:
                all_repeat_node.append(one_node)

    all_repeat_node_size = sum([node.data['size'] for node in all_repeat_node])
    all_repeat_layer_size = sum([node.data['size'] for node in all_repeat_node if node.data['repeat'] == True and tree.parent(tree.parent(node.identifier).identifier).identifier == 0])
    reduce_size = all_repeat_node_size - all_repeat_layer_size

    return all_size, reduce_size


def classfy_subtree(img_list):
    time_all1=time.time()
    # 获取树
    node_id = 0
    tree = Tree()
    tree.create_node(tag='root', identifier=node_id, parent=None,
                     data={'name': 0, 'size': 0, 'hash': 0, 'repeat': False, 'img':None})  # 根节点

    node_id = node_id + 1
    for img in img_list:
        t1 = time.time()
        hash_dict = './input/DATA/'+img+'/hash_dict.txt'
        layer_dict = './input/DATA/'+img+'/layer_dict.txt'
        tree1, node_id = get_tree(tree, node_id, hash_dict, layer_dict, img)
        t2 = time.time()

    t2 = time.time()
    tree1 = same_subtree(tree1)

    t3 = time.time()
    all_size, reduce_size = get_subtree_size(tree1)
    return 0


def get_file_size(path):
    try:
        size = os.path.getsize(path)
        return round(size / 1024 / 1024, 2)
    except Exception as err:
        return 0
def load_json(addr):
    with open(addr, 'r') as load_f:
        load_data = json.load(load_f)
        return load_data

def clean_diff_hash_repeat(addr):
    layer_order = [one_layer.split('/')[0] for one_layer in load_json(addr+'/manifest.json')[0]['Layers']]
    file_all_size = 0.0
    addr_file_dict = {}
    for root, dirs, files in os.walk(addr):
        for file in files:
            file_path = os.path.join(root, file).replace('\\', '/')
            file_layer = file_path.split('/')[4]
            if len(file_layer)!=64 and len(file_path.split('/'))==5:
                continue
            file_path_inlayer = file_path.split(file_layer)[1]
            file_size = get_file_size(file_path)
            file_all_size += file_size
            if addr_file_dict.__contains__(file_path_inlayer):
                if layer_order.index(addr_file_dict[file_path_inlayer][0])>layer_order.index(file_layer):
                    del_file(file_path)
                if layer_order.index(addr_file_dict[file_path_inlayer][0])<layer_order.index(file_layer):
                    addr_file_dict[file_path_inlayer] = [file_layer, file_size, file_path]
                    del_file(addr_file_dict[file_path_inlayer][2])
            else:
                addr_file_dict[file_path_inlayer] = [file_layer, file_size, file_path]
    return file_all_size

def dict_merge(dict1, dict2):
    dict1_key = [key for key in dict1]
    for key in dict2:
        if key not in dict1_key:
            dict1_key.append(key)
            dict1[key] = dict2[key]
        else:
            for value in dict2[key]:
                dict1[key].append(value)
    return dict1

def split_image(dict_all, img_list):
    repeat_file_dict = {}
    for key in dict_all:
        if len(dict_all[key]) > 1 and dict_all[key][0]['size'] > 0:
            repeat_file_dict[key] = dict_all[key]
    if TEST_FLAG == True:
        for img in img_list:
            t1=time.time()
            addr = './input/images/' + img
            layer_order_addr = [addr+'/'+one_layer.split('/')[0]+'/' for one_layer in load_json(addr + '/manifest.json')[0]['Layers']]
            for addr in layer_order_addr:
                copy_dirs(addr, './output/'+img+'/unit/base/')
            t2 = time.time()

    if TEST_FLAG == True:
        if not os.path.exists('./output/tmp'):
            os.makedirs('./output/tmp')
        for img in img_list:
            t1 = time.time()
            init_root = ''
            addr1 = './output/'+img+'/unit/base/'
            for root, dirs, files in os.walk(addr1):
                for file in files:
                    file_addr = os.path.join(root, file)
                    file_hash = file_hash_coding(file_addr)
                    file_size = get_file_size(file_addr)
                    if file_size>0:
                        try:
                            test = dict_all[file_hash + file + str(file_size)]
                        except:
                            continue
                        if len(dict_all[file_hash + file + str(file_size)]) > 1 and \
                                dict_all[file_hash + file + str(file_size)][0]['size'] > 0:
                            unit_class = []
                            for img_name in dict_all[file_hash + file + str(file_size)]:
                                if img_name['img'] not in unit_class:
                                    unit_class.append(img_name['img'])
                            if len(unit_class) <= 1:
                                continue
                            unit_class.sort()
                            unit_name = '__'.join(unit_class)
                            if not os.path.exists('./output/tmp/' + unit_name +'/unit_dir/'):
                                os.makedirs('./output/tmp/' + unit_name +'/unit_dir/')
                            if not os.path.exists('./output/tmp/' + unit_name + '/unit_dir/' + file):
                                shutil.move(file_addr, './output/tmp/' + unit_name + '/unit_dir/' + file)
                                os.popen('ln -s ' + '/unit_dir/' + file + ' ' + file_addr).read()
            t2 = time.time()
            time.sleep(10)
    for img in img_list:
        addr1 = './output/' + img + '/unit/'
        make_gz(addr1 + 'base', addr1 + 'base.tar')
        shutil.rmtree(addr1 + 'base')
        gz_hash_tar = file_hash_coding(addr1 + 'base.tar')
        os.makedirs(addr1 + gz_hash_tar)
        shutil.move(addr1 + 'base.tar', addr1 + gz_hash_tar + '/layer.tar')

    for root, dirs, files in os.walk('./output/tmp/'):
        for dir in dirs:
            img_list = dir.split('__')
            make_gz('./output/tmp/'+dir, './output/tmp/' + dir + '.tar')
            gz_hash_tar = file_hash_coding('./output/tmp/' + dir + '.tar')
            for img in img_list:
                addr1 = './output/' + img + '/unit/'
                os.makedirs(addr1 + gz_hash_tar)
                shutil.copyfile('./output/tmp/' + dir + '.tar', addr1 + gz_hash_tar + '/layer.tar')
        break
    shutil.rmtree('./output/tmp')
    os.popen('chmod 777 -R ./output').read()
    return


def classfy_file(img_list):
    t1=time.time()
    dict_all = {}
    dict_all_new = {}
    if TEST_FLAG==True and os.path.exists('./input/DATA/all_hash_dict.txt'):
        os.remove('./input/DATA/all_hash_dict.txt')
    if os.path.exists('./input/DATA/all_hash_dict.txt'):
        dict_all_new = load_json('./input/DATA/all_hash_dict.txt')
    else:
        for img in img_list:
            hash_addr = './input/DATA/' + img + '/hash_dict.txt'
            one_image_hash_dict = load_json(hash_addr)
            for key in one_image_hash_dict:
                for value_dict_index in range(len(one_image_hash_dict[key])):
                    one_image_hash_dict[key][value_dict_index]['img']=img
            dict_all = dict_merge(dict_all, one_image_hash_dict)
        for key in dict_all:
            for value_dict in dict_all[key]:
                new_key = key + value_dict['name'] + str(value_dict['size'])
                if dict_all_new.__contains__(new_key):
                    dict_all_new[new_key].append(value_dict)
                else:
                    dict_all_new[new_key] = [value_dict]
        save_json('./input/DATA/all_hash_dict.txt', dict_all_new)

    split_image(dict_all_new, img_list)
    t2 = time.time()
    return

if __name__ == '__main__':
    img_list = ['python', 'golang', 'openjdk', 'alpine', 'ubuntu', 'memcached',  'httpd', 'mysql', 'mariadb', 'redis',  'postgres', 'rabbitmq', 'registry', 'wordpress', 'ghost', 'node', 'flink', 'cassandra', 'eclipse-mosquitto']

