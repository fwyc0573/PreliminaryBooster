import os
import tarfile
import stat
import hashlib
import sys
import json
import shutil


def write_to_file(my_dict, save_path):
    dict_json_form = json.dumps(my_dict)
    with open(save_path, "w") as f:
        f.write(dict_json_form)


def get_file_size(path):
    try:
        size = os.path.getsize(path)
        return round(size / 1024 / 1024, 2)
    except Exception as err:
        return 0


def read_from_file(save_path):
    with open(save_path, "r") as f:
        raw_file = f.read()
    my_dict = json.loads(raw_file)
    return my_dict


def creat_empty_file(file_list, path):
    for each_file in file_list:
        f = open(path + "/" + each_file, "w")
        f.close()


def creat_folder(path):
    try:
        os.mkdir(path)
    except Exception as e:
        print(e)


def snapshot(
    my_layers_dict, copy_basic_path, storage_basic_path="D:/raw_docker_extract/"
):
    creat_folder(copy_basic_path)
    for layer in my_layers_dict:
        print("now layer:", layer)
        for menu in my_layers_dict[layer]:
            print("now menu in layer:", menu)
            if menu == "":
                now_path = copy_basic_path + layer + "/"
                creat_folder(now_path)
                print("menu == " "" + now_path)
                if my_layers_dict[layer][menu]:
                    creat_empty_file(my_layers_dict[layer][menu], now_path)
            else:
                now_path = copy_basic_path + layer + menu  # 目录自带/符号了，不需要加；layer未带
                creat_folder(now_path)
                creat_empty_file(my_layers_dict[layer][menu], now_path)


def copy_dirs(addr1, addr2):
    if not os.path.exists(addr2):
        os.makedirs(addr2)
    if os.path.exists(addr2):
        init_root = ""
        for root, dirs, files in os.walk(addr1):
            init_root = root
            break
        for root, dirs, files in os.walk(addr1):
            for dir in dirs:
                addr_dir = addr2 + root.split(init_root)[1] + "/" + dir
                if not os.path.exists(addr_dir):
                    os.makedirs(addr_dir)
            for file in files:
                src_file = os.path.join(root, file)
                target_path = addr2 + root.split(init_root)[1] + "/" + file
                try:
                    shutil.copy(src_file, target_path)
                except:
                    pass
    return 0


def file_hash_coding(file_path, is_md5=True):
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
        pass


def del_file(filepath):
    try:
        os.remove(filepath)
        return
    except Exception as e:
        shutil.rmtree(filepath)
        return


def load_json(addr):
    with open(addr, "r") as load_f:
        load_data = json.load(load_f)
        return load_data


def clean_diff_hash_repeat(addr):
    layer_order = [
        one_layer.split("/")[0]
        for one_layer in load_json(addr + "/manifest.json")[0]["Layers"]
    ]
    file_all_size = 0.0
    addr_file_dict = {}
    for root, dirs, files in os.walk(addr):
        for file in files:
            file_path = os.path.join(root, file).replace("\\", "/")
            file_layer = file_path.split("/")[4]
            if len(file_layer) != 64 and len(file_path.split("/")) == 5:
                continue
            file_path_inlayer = file_path.split(file_layer)[1]

            if addr_file_dict.__contains__(file_path_inlayer):
                if layer_order.index(
                    addr_file_dict[file_path_inlayer][0]
                ) > layer_order.index(file_layer):
                    del_file(file_path)
                if layer_order.index(
                    addr_file_dict[file_path_inlayer][0]
                ) < layer_order.index(file_layer):
                    addr_file_dict[file_path_inlayer] = [file_layer, file_path]
                    del_file(addr_file_dict[file_path_inlayer][1])
            else:
                addr_file_dict[file_path_inlayer] = [file_layer, file_path]
    return file_all_size


def get_dict(image_list):
    os.popen("rm -r ./input/DATA/").read()
    if not os.path.exists("./input/DATA/"):
        os.makedirs("./input/DATA/")
    for image in image_list:
        all_size = 0
        # 清理自冗余文件
        clean_diff_hash_repeat("./input/images/" + image)
        fp = r"./input/images/" + image + "/"  # 目标文件夹
        file_ordir_list = os.walk(fp)

        layer_dict = {}
        hash_dict = {}

        is_first = True
        for file_paths, dir_names, file_names in file_ordir_list:
            file_paths = file_paths.replace("\\", "/")
            if is_first:
                is_first = False
                major_path = file_paths
                continue

            if len(file_paths) - len(major_path) == 64:
                whole_layer_path = file_paths
                now_layer = file_paths[-64:]
                layer_dict[now_layer] = {}

            file_structure = file_paths.replace(whole_layer_path, "")
            file_names_filter = []  # 修正过值的文件名
            for file in file_names:
                file_path = file_paths + "/" + file
                file_hash = file_hash_coding(file_path)

                if file_hash:
                    file_size = get_file_size(file_paths + "/" + file)
                    all_size = all_size + file_size
                    if file_hash in hash_dict.keys():
                        hash_dict[file_hash].append(
                            {
                                "name": file,
                                "path": "." + file_paths.split("./input/images")[1],
                                "size": file_size,
                            }
                        )  # {"ID": [{""},{}]}
                    else:
                        hash_dict[file_hash] = []
                        hash_dict[file_hash].append(
                            {
                                "name": file,
                                "path": "." + file_paths.split("./input/images")[1],
                                "size": file_size,
                            }
                        )
                    file = file_hash
                file_names_filter.append(file)
            layer_dict[now_layer][file_structure] = file_names_filter
        print("all_size", all_size)
    return 0


if __name__ == "__main__":
    image_list = ["registry"]
