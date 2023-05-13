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
import magic

TEST_FLAG = True


def is_shared_lib(file_path):
    magic_obj = magic.Magic()

    file_type = magic_obj.from_file(file_path)
    return "shared object" in file_type


# 将数据存储为JSON格式的函数
def save_json(addr, data):
    with open(addr, "w") as f:
        json.dump(data, f)
    return 0


def sort_dict(a_dict, option="value"):
    if option in ["value", "key"]:
        result_dict = {}
        if option == "key":
            temp_list = list(a_dict.keys())
            temp_list.sort()
            for item in temp_list:
                result_dict[item] = a_dict[item]
        else:
            temp_value_list = list(a_dict.values())
            temp_key_list = list(a_dict.keys())
            for i in range(len(temp_key_list)):
                for j in range(len(temp_key_list) - i - 1):
                    if temp_value_list[j] > temp_value_list[j + 1]:
                        temp = temp_key_list[j]
                        temp_key_list[j] = temp_key_list[j + 1]
                        temp_key_list[j + 1] = temp
                        temp = temp_value_list[j]
                        temp_value_list[j] = temp_value_list[j + 1]
                        temp_value_list[j + 1] = temp
            for key, value in zip(temp_key_list, temp_value_list):
                result_dict[key] = value
        return result_dict
    raise ValueError(option + " is not in option list——[key,value]")


def get_repeat_size(dict_all_new):
    dict_repeat_size = {}

    final_dict = {}
    for dict_name in dict_all_new:
        if len(dict_all_new[dict_name]) == 1:
            continue
        tmp_img = []
        tmp_size = 0
        for img in dict_all_new[dict_name]:
            tmp_size = img["size"]
            if img["img"] not in tmp_img:
                tmp_img.append(img["img"])
        if tmp_size == 0 or len(tmp_img) == 1:
            continue
        tmp_img.sort()
        name = ""
        for name_tmp in tmp_img:
            name = name + name_tmp + "_"
            if name_tmp not in dict_repeat_size:
                dict_repeat_size[name_tmp] = tmp_size
            else:
                dict_repeat_size[name_tmp] = dict_repeat_size[name_tmp] + tmp_size
        if name not in final_dict:
            final_dict[name] = tmp_size
        else:
            final_dict[name] = final_dict[name] + tmp_size
        # if len(dict_all_new[dict_name]) > 3:
        #    print(final_dict[name], dict_all_new[dict_name])
    dict_order = sort_dict(final_dict)
    more_onemb = []
    for i in dict_order:
        if dict_order[i] > 1:
            print("repeat", i, dict_order[i], "MB")
            more_onemb.append(i)
    # print(dict_repeat_size)
    return more_onemb


def del_file(filepath):
    try:
        os.remove(filepath)
        return
    except Exception as e:
        print("ERROR! DELETE FILE")
        shutil.rmtree(filepath)
        return


def sha256_update_from_file(filename, hash):
    assert Path(filename).is_file()
    with open(str(filename), "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash.update(chunk)
    return hash


def sha256_update_from_dir(directory, hash):
    assert Path(directory).is_dir()
    for path in sorted(Path(directory).iterdir()):
        hash.update(path.name.encode())
        if path.is_file():
            hash = sha256_update_from_file(path, hash)
        elif path.is_dir():
            hash = sha256_update_from_dir(path, hash)
    return hash


def file_hash_coding(file_path, is_md5=False):
    try:
        with open(file_path, "rb") as fp:
            data = fp.read()
            if fp.tell():
                return (
                    hashlib.md5(data).hexdigest()
                    if is_md5
                    else hashlib.sha256(data).hexdigest()
                )
            else:
                return 0
    except:
        return 0


def make_tarfile(source_dir, output_filename):
    os.popen("tar -cvf " + output_filename + " " + source_dir).read()


def get_file_size(path):
    try:
        link_stat = os.lstat(path)
        size = link_stat.st_size
        return round(size / 1024 / 1024, 2)  # 单位 MB
    except Exception as err:
        print("ERROR! GET SIZE", path)
        return 0


def load_json(addr):
    with open(addr, "r") as load_f:
        load_data = json.load(load_f)
        return load_data


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


def mp_copy_image(input_list):
    img = input_list[0]
    t1 = time.time()
    addr = "./input/images/" + img
    layer_order_addr = [
        addr + "/" + one_layer.split("/")[0] + "/"
        for one_layer in load_json(addr + "/manifest.json")[0]["Layers"]
    ]
    for addr in layer_order_addr:
        # copy_dirs(addr, './output/'+img+'/unit/base/')
        if not os.path.exists("./output/" + img + "/unit/base/"):
            os.makedirs("./output/" + img + "/unit/base/")
        shutil.copytree(
            addr,
            "./output/" + img + "/unit/base/" + addr.split("/")[-2],
            symlinks=True,
            copy_function=shutil.copy2,
        )
    t2 = time.time()
    print("%s构建合并镜像文件至output，用时%s" % (img, t2 - t1))
    return 0


def mp_reconstrucuting(input_list):
    img = input_list[0]
    dict_all = input_list[1]
    t1 = time.time()

    addr1 = "./output/" + img + "/unit/base/"
    valid_list = [file_id for file_id in dict_all]

    for root, dirs, files in os.walk(addr1):
        for file in files:
            file_addr = os.path.join(root, file)
            file_size = get_file_size(file_addr)
            if str(file_size) == "0.0":
                continue
            file_hash = file_hash_coding(file_addr)
            if str(file_hash) == "0":
                continue
            if file_hash + file + str(file_size) not in valid_list:
                continue
            if (
                len(dict_all[file_hash + file + str(file_size)]) > 1
                and dict_all[file_hash + file + str(file_size)][0]["size"] > 0
            ):
                unit_class = []
                for img_name in dict_all[file_hash + file + str(file_size)]:
                    if img_name["img"] not in unit_class:
                        unit_class.append(img_name["img"])
                if len(unit_class) <= 1:
                    continue
                unit_class.sort()
                unit_name = "__".join(unit_class)
                if not os.path.exists("./output/tmp/" + unit_name + "/unit_dir/"):
                    os.makedirs("./output/tmp/" + unit_name + "/unit_dir/")
                if not os.path.exists(
                    "./output/tmp/" + unit_name + "/unit_dir/" + file
                ):
                    shutil.move(
                        file_addr, "./output/tmp/" + unit_name + "/unit_dir/" + file
                    )
                else:
                    os.remove(file_addr)
                os.popen("ln -s " + "/unit_dir/" + file + " " + file_addr).read()

    t2 = time.time()
    return 0


def mp_main_tar(input_list):
    t1 = time.time()
    img = input_list[0]
    addr1 = "./output/" + img + "/unit/"
    make_tarfile(addr1 + "base", addr1 + "base.tar")
    shutil.rmtree(addr1 + "base")
    gz_hash_tar = file_hash_coding(addr1 + "base.tar")
    os.makedirs(addr1 + gz_hash_tar)
    shutil.move(addr1 + "base.tar", addr1 + gz_hash_tar + "/layer.tar")
    t2 = time.time()
    return 0


def split_image(dict_all, img_list):
    parallel_flag = True
    if TEST_FLAG == True:
        if parallel_flag:
            pool = Pool()
            pool.map(
                mp_copy_image, zip(img_list, [0 for null_value in range(len(img_list))])
            )
            pool.close()
            pool.join()
        else:
            for img in img_list:
                mp_copy_image([img, 0])

    if TEST_FLAG == True:

        if not os.path.exists("./output/tmp"):
            os.makedirs("./output/tmp")
        if parallel_flag:
            pool = Pool()
            pool.map(
                mp_reconstrucuting,
                zip(img_list, [dict_all for null_value in range(len(img_list))]),
            )
            pool.close()
            pool.join()
        else:
            for img in img_list:
                mp_reconstrucuting([img, dict_all])
        if parallel_flag:
            pool = Pool()
            pool.map(
                mp_main_tar, zip(img_list, [0 for null_value in range(len(img_list))])
            )
            pool.close()
            pool.join()
        else:
            for img in img_list:
                mp_main_tar([img, 0])

    for root, dirs, files in os.walk("./output/tmp/"):
        for dir in dirs:
            img_list = dir.split("__")
            make_tarfile("./output/tmp/" + dir, "./output/tmp/" + dir + ".tar")
            gz_hash_tar = file_hash_coding("./output/tmp/" + dir + ".tar")
            for img in img_list:
                addr1 = "./output/" + img + "/unit/"
                os.makedirs(addr1 + gz_hash_tar)
                shutil.copyfile(
                    "./output/tmp/" + dir + ".tar", addr1 + gz_hash_tar + "/layer.tar"
                )
        break
    shutil.rmtree("./output/tmp")
    os.popen("chmod 777 -R ./output").read()
    return


def classfy_file(img_list):
    t1 = time.time()
    dict_all = {}
    dict_all_new = {}
    if TEST_FLAG == True and os.path.exists("./input/DATA/all_hash_dict.txt"):
        os.remove("./input/DATA/all_hash_dict.txt")
    if os.path.exists("./input/DATA/all_hash_dict.txt"):
        dict_all_new = load_json("./input/DATA/all_hash_dict.txt")
    else:
        for img in img_list:
            hash_addr = "./input/DATA/" + img + "/hash_dict.txt"
            one_image_hash_dict = load_json(hash_addr)
            for key in one_image_hash_dict:
                for value_dict_index in range(len(one_image_hash_dict[key])):
                    one_image_hash_dict[key][value_dict_index]["img"] = img
            dict_all = dict_merge(dict_all, one_image_hash_dict)
        for key in dict_all:
            for value_dict in dict_all[key]:
                new_key = key + value_dict["name"] + str(value_dict["size"])
                if dict_all_new.__contains__(new_key):
                    dict_all_new[new_key].append(value_dict)
                else:
                    dict_all_new[new_key] = [value_dict]
        save_json("./input/DATA/all_hash_dict.txt", dict_all_new)
    dict_all_more_onemb = {}
    more_onemb = get_repeat_size(dict_all_new)
    for file_id in dict_all_new:
        file_list = dict_all_new[file_id]
        if len(file_list) == 1 or file_list[0]["size"] == 0:
            continue
        img_list_tmp = []
        for one_file in file_list:
            if one_file["img"] not in img_list_tmp:
                img_list_tmp.append(one_file["img"])
        if len(img_list_tmp) == 1:
            continue
        img_list_tmp.sort()
        img_str = ""
        for one_img in img_list_tmp:
            img_str = img_str + one_img + "_"

        if img_str in more_onemb:
            dict_all_more_onemb[file_id] = file_list
    # print('dict_all_new', dict_all_new)
    # print('more_onemb', more_onemb)
    split_image(dict_all_more_onemb, img_list)
    t2 = time.time()
    return


if __name__ == "__main__":
    img_list = [
        "python",
        "golang",
        "openjdk",
        "alpine",
        "ubuntu",
        "memcached",
        "httpd",
        "mysql",
        "mariadb",
        "redis",
        "postgres",
        "rabbitmq",
        "registry",
        "wordpress",
        "ghost",
        "node",
        "flink",
        "cassandra",
        "eclipse-mosquitto",
    ]
