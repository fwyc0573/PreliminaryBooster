
import copy
import csv
import datetime
import hashlib
import json
import os
import shutil
import subprocess
import tarfile
import time

import docker

empty_json = '{"created":"1970-01-01T00:00:00Z","container_config":{"Hostname":"","Domainname":"","User":"","AttachStdin":false, \
	"AttachStdout":false,"AttachStderr":false,"Tty":false,"OpenStdin":false, "StdinOnce":false,"Env":null,"Cmd":null,"Image":"", \
	"Volumes":null,"WorkingDir":"","Entrypoint":null,"OnBuild":null,"Labels":null}}'


def file_hash_coding(file_path, is_md5=True):
    with open(file_path, 'rb') as fp:
        data = fp.read()
        if fp.tell():
            # print(fp.tell())
            return hashlib.md5(data).hexdigest() if is_md5 else hashlib.sha256(data).hexdigest()
        else:
            return 0


def reconstruct_layer_v1():
    images_path = "./output/"

    for each_image in os.listdir(images_path):
        print(each_image, " -> is going to reconstruct now...")
        my_image_inner_path = os.path.join(images_path, each_image)
        metadata_path = os.path.join(my_image_inner_path, "metadata")
        layer_path = os.path.join(my_image_inner_path, "layer")
        unit_path = os.path.join(my_image_inner_path, "unit")

        try:
            os.remove(os.path.join(layer_path, each_image +".tar"))
            for item in os.listdir(layer_path):
                shutil.move(os.path.join(layer_path, item), metadata_path) if os.path.isfile(
                    os.path.join(layer_path, item)) else 0
        except:
            print("元数据存储位置正确...")

        with open(os.path.join(metadata_path, "manifest.json"), encoding='utf-8') as f:
            manifest_dict = json.load(f)
        config = manifest_dict[0]["Config"]
        raw_layer_list = manifest_dict[0]["Layers"]
        layer_list = [i[:-10] for i in raw_layer_list]
        with open(os.path.join(metadata_path, config), encoding='utf-8') as f:
            config_dict = json.load(f)
        diff_id_list = config_dict["rootfs"]["diff_ids"]  # 带sha256前缀
        with open(os.path.join(metadata_path, "json"), encoding='utf-8') as f:
            unit_dict = json.load(f)
        for unit_id in unit_dict:
            from_layer = unit_dict[unit_id][0]
            pos = layer_list.index(from_layer)
            print(unit_id, "(unit) was clipped from  ", from_layer, "(layer) which order is ", pos)

            """
                iteration for config_json and manifest update---------------------------
            """
            # print("before changing the old diff_id_list: ", diff_id_list[pos])
            # diff_id_list[pos] = "sha256:" + file_hash_coding(os.path.join(layer_path, from_layer, "layer.tar"), False)
            # print("after changing the old diff_id_list[pos]: ", diff_id_list[pos])

            # add the new layer diff_id_list into id.json and the new layer uuid into manifest
            do_move_back = True
            print("before inserting, diff_id_list: ", diff_id_list, " layer_list: ", layer_list)
            if pos != len(diff_id_list) - 1:
                diff_id_list.insert(pos + 1, "sha256:" + unit_id)
                layer_list.insert(pos +1, unit_id)
            else:
                diff_id_list.insert(pos, "sha256:" + unit_id)
                layer_list.insert(pos, unit_id)
                do_move_back = False
            print("after inserting, diff_id_list: ", diff_id_list, " layer_list: ", layer_list)

            """
                add files of the new layer and update the structure of its son----------
            """
            # version
            version_from = os.path.join(layer_path, from_layer, "VERSION")
            target_path = os.path.join(unit_path, unit_id, "VERSION")
            shutil.copyfile(version_from, target_path)
            print("unit -> layer: version creating.")

            # json config
            json_obj = json.loads(empty_json)
            json_obj['id'] = unit_id  # here, we use diff_id_list as uuid

            # FIXME: here, we assume that raw images has at least 2 layers.
            if do_move_back:
                parent_uuid = layer_list[pos]
                son_layer_uuid = layer_list[pos + 2]
            else:
                parent_uuid = layer_list[pos-2]
                son_layer_uuid = layer_list[pos]

            json_obj['parent'] = parent_uuid
            with open(os.path.join(unit_path, unit_id, "json"), 'w') as file:
                file.write(json.dumps(json_obj))
            print("unit -> layer: json creating.")

            basic_dir = layer_path if os.path.exists(os.path.join(layer_path, son_layer_uuid)) else unit_path
            with open(os.path.join(basic_dir, son_layer_uuid, "json"), encoding='utf-8') as file:
                son_layer_json_dict = json.load(file)
            son_layer_json_dict["parent"] = unit_id
            with open(os.path.join(basic_dir, son_layer_uuid, "json"), 'w') as file:
                file.write(json.dumps(son_layer_json_dict))
            print("old layer/unit -> new layer/unit: structure json updating.")

        """
            final diff_id check and config_json and manifest creat------------
        """
        if not os.path.exists(os.path.join(images_path, each_image, "reconstruct")):
            os.mkdir(os.path.join(images_path, each_image, "reconstruct"))
        for item in os.listdir(layer_path):
            pos = layer_list.index(item)
            # calculate sha256sum of each layer.tar and update the diff_id for each layer(only old layer)
            diff_id_list[pos] = "sha256:" + file_hash_coding(os.path.join(layer_path, item, "layer.tar"), False)
            shutil.move(os.path.join(layer_path, item), os.path.join(images_path, each_image, "reconstruct"))
        for item in os.listdir(unit_path):
            shutil.move(os.path.join(unit_path, item), os.path.join(images_path, each_image, "reconstruct"))

        # layer_list and diff_id_list change the value in the iteration.
        manifest_dict[0]["Layers"] = [i + "/layer.tar" for i in layer_list]
        with open(os.path.join(images_path, each_image, "reconstruct", "manifest.json"), 'w') as file:
            file.write(json.dumps(manifest_dict))
        with open(os.path.join(images_path, each_image, "reconstruct", config), 'w') as file:
            file.write(json.dumps(config_dict))
        shutil.move(os.path.join(metadata_path, "repositories"), os.path.join(images_path, each_image, "reconstruct"))
        break

def reconstruct_layer_v2():
    images_path = "./output/"
    input_path = "./input/images"
    sum_size = 0

    for each_image in os.listdir(images_path):
        print(each_image, " -> is going to reconstruct now...")

        input_path_image = os.path.join(input_path, each_image)
        my_image_inner_path = os.path.join(images_path, each_image)
        unit_path = os.path.join(my_image_inner_path, "unit")
        try:
            for item in os.listdir(input_path_image):
                # shutil.copy(os.path.join(input_path_image, item), unit_path) if os.path.isfile(
                #     os.path.join(input_path_image, item)) else 0
                if os.path.isfile(os.path.join(input_path_image, item)):
                    shutil.copy(os.path.join(input_path_image, item), unit_path)
                    # print(f"input_path_image:{os.path.join(input_path_image, item)}, unit_path:{unit_path}")
        except:
            print("元数据存储位置正确...")

        # 读取manifest
        with open(os.path.join(unit_path, "manifest.json"), encoding='utf-8') as f:
            manifest_dict = json.load(f)
        config = manifest_dict[0]["Config"]
        RepoTags = manifest_dict[0]["RepoTags"][0]
        raw_layer_list = manifest_dict[0]["Layers"]
        layer_list = [i[:-10] for i in raw_layer_list]
        print("已完成manifest文件的读取...")

        with open(os.path.join(unit_path, config), encoding='utf-8') as f:
            config_dict = json.load(f)
        diff_id_list = config_dict["rootfs"]["diff_ids"]

        unit_index = 0
        layer_list = [item for item in os.listdir(unit_path) if os.path.isdir(os.path.join(unit_path, item))]

        for unit_layer in layer_list:
            if unit_index == len(layer_list) - 1:
                layer_json_creat(unit_path, unit_layer, is_last=True, is_first=False,
                                 parent_id=layer_list[unit_index - 1], config_json=copy.deepcopy(config_dict))
            elif unit_index == 0:
                layer_json_creat(unit_path, unit_layer, is_last=False, is_first=True, parent_id=None,
                                 config_json=None)
            else:
                layer_json_creat(unit_path, unit_layer, is_last=False, is_first=False,
                                 parent_id=layer_list[unit_index - 1], config_json=None)
            unit_index += 1

        manifest_dict[0]["Layers"] = [item+"/layer.tar" for item in layer_list]
        manifest_dict[0]["RepoTags"][0] = "fengyicheng/" + RepoTags if "fengyicheng/" not in RepoTags else RepoTags
        print(f"manifest_dict[0] = {manifest_dict[0]['RepoTags'][0]}")

        with open(os.path.join(unit_path, "manifest.json"), 'w') as file:
            file.write(json.dumps(manifest_dict))

        config_dict["rootfs"]["diff_ids"] = ["sha256:"+item for item in layer_list if "sha256:" not in item]
        del config_dict['history']
        with open(os.path.join(unit_path, config), 'w') as file:
            file.write(json.dumps(config_dict))

        tar_path = os.path.join(unit_path, config[:-5]+".tar")
        tar = tarfile.open(tar_path, "w")
        tar.add(unit_path + "/", arcname=os.path.sep)
        tar.close()

        t1_time = time.time()
        print("Start docker loading ...")
        pro = subprocess.Popen("docker load -i " + tar_path, shell=True, stdout=subprocess.PIPE)
        pro.wait()
        print("Finish docker loading ...")
        t2_time = time.time()

        sum_size += int(os.path.getsize(tar_path) / 1024 / 1024)

        upload_to_hub = True
        repo_tags = manifest_dict[0]["RepoTags"][0]
        if upload_to_hub:
            pro = subprocess.Popen("docker push " + repo_tags, shell=True, stdout=subprocess.PIPE)
            pro.wait()
        t3_time = time.time()

        t_load = t2_time - t1_time
        t_push = t3_time - t2_time
        my_size = int(os.path.getsize(tar_path) / 1024 / 1024)
        now = datetime.datetime.now()
        formatted_time = now.strftime('%Y-%m-%d %H:%M:%S')
        data = [[formatted_time, repo_tags, t_load, t_push, my_size]]
        file_name = 'mine_push_load.csv'
        with open(file_name, mode='a', newline='') as file:
            writer = csv.writer(file)
            for row in data:
                writer.writerow(row)

        os.remove(tar_path)
        pro = subprocess.Popen("docker rmi -f " + repo_tags, shell=True, stdout=subprocess.PIPE)
        pro.wait()



def reconstruct_layer_accelerate():
    tmp_folder_path = "/home/request/python"
    client = docker.from_env()

    with open(os.path.join(tmp_folder_path, "manifest.json"), encoding='utf-8') as f:
        manifest_dict = json.load(f)
    config = manifest_dict[0]["Config"]
    raw_layer_list = manifest_dict[0]["Layers"]
    layer_list = [i[:-10] for i in raw_layer_list]
    layer_list_final = layer_list.copy()

    with open(os.path.join(tmp_folder_path, config), encoding='utf-8') as f:
        config_dict = json.load(f)
    diff_id_list = config_dict["rootfs"]["diff_ids"]  # 带sha256前缀

    layer_exit_list = []
    for item in os.listdir(tmp_folder_path):
        layer_exit_list.append(item) if os.path.isdir(os.path.join(tmp_folder_path, item)) else 0

    for index in range(len(layer_list)):
        if layer_list[index] not in layer_exit_list:
            layer_list_final.remove(layer_list[index])
            diff_id_list.remove("sha256:" + layer_list[index])

    for index in range(len(layer_list_final)):
        layer_id = layer_list_final[index]
        with open(os.path.join(tmp_folder_path, layer_id, "json"), 'r', encoding='utf-8') as f:
            json_dict = json.load(f)
            if index == 0:
                json_dict['parent'] = ""
            else:
                json_dict['parent'] = layer_list_final[index-1]
        with open(os.path.join(tmp_folder_path, layer_id, "json"), 'w', encoding='utf-8') as f:
            f.write(json.dumps(json_dict))

    manifest_dict[0]["Layers"] = [item+"/layer.tar" for item in layer_list_final]
    manifest_dict[0]["RepoTags"] = [origin_name + "_tmpImage" for origin_name in manifest_dict[0]["RepoTags"] if
                                    "_tmpImage" not in origin_name]
    with open(os.path.join(tmp_folder_path, "manifest.json"), 'w') as file:
        file.write(json.dumps(manifest_dict))

    try:
        del config_dict['history']
    except:
        pass
    with open(os.path.join(tmp_folder_path, config), 'w') as file:
        file.write(json.dumps(config_dict))

    image_id = config.strip(".json")
    tar_make(tmp_folder_path, image_id, client)


def tar_make(tmp_folder_path, image_id, client):
    tar_path = os.path.join("/home/request", image_id + ".tar")
    tar = tarfile.open(tar_path, "w")
    tar.add(tmp_folder_path, arcname=os.path.sep)
    tar.close()

    t_start = time.time()
    compress_cmd = "docker load -i " + tar_path
    pro = subprocess.Popen(compress_cmd, shell=True, stdout=subprocess.PIPE)
    pro.wait()


def layer_json_creat(basic_path, my_id, is_last, is_first, parent_id=None, config_json=None):
    with open(os.path.join(basic_path, my_id, "json"), "w") as file:
        if is_last:
            json_obj = config_json
            del json_obj['history']
            try:
                del json_obj['rootfs']
            except:
                del json_obj['rootfS']
        else:
            json_obj = json.loads(empty_json)

        if is_first:
            json_obj['parent'] = ""
        else:
            json_obj['parent'] = parent_id

        json_obj['id'] = my_id
        file.write(json.dumps(json_obj))


if __name__ == '__main__':
    # reconstruct_layer()
    # reconstruct_layer_v2()
    reconstruct_layer_accelerate()