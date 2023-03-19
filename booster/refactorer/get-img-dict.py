import os
import tarfile
import hashlib
import sys
import json
import shutil
def write_to_file(my_dict, save_path):
    dict_json_form = json.dumps(my_dict)
    with open(save_path, 'w') as f:
        f.write(dict_json_form)
def get_file_size(path):
    try:
        link_stat = os.lstat(path)
        size = link_stat.st_size
        return round(size / 1024 / 1024, 2)
    except Exception as err:
        return 0

def read_from_file(save_path):
    with open(save_path, 'r') as f:
        raw_file = f.read()
    my_dict = json.loads(raw_file)
    return my_dict


def creat_empty_file(file_list, path):
    for each_file in file_list:
        f = open(path + '/' + each_file, 'w')
        f.close()

def creat_folder(path):
    try:
        os.mkdir(path)
    except Exception as e:
        print(e)

def snapshot(my_layers_dict, copy_basic_path, storage_basic_path="D:/raw_docker_extract/"):
    creat_folder(copy_basic_path)
    for layer in my_layers_dict:
        print("now layer:", layer)
        for menu in my_layers_dict[layer]:
            print("now menu in layer:", menu)
            if menu == "":
                now_path = copy_basic_path + layer + '/'
                creat_folder(now_path)
                print("menu == """ + now_path)
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
                try:
                    shutil.copy(src_file, target_path)
                except:
                    pass
    return 0

def file_hash_coding(file_path, is_md5=False):
    # file_path = "C:/Users/goodgoodstudy2018/Desktop\ATC测试/raw_docker_extract/405e6854c379e5344413a6dba54ddd2c4519cf86f7c531500e93fa7b314030c4/usr/lib/python3/dist-packages/_dbus_bindings.cpython-37m-x86_64-linux-gnu.so"
    try:
        with open(file_path, 'rb') as fp:
            data = fp.read()
            if fp.tell():
                # print(fp.tell())
                return hashlib.md5(data).hexdigest() if is_md5 else hashlib.sha256(data).hexdigest()
            else:
                return 0
    except:
        # print(file_path)
        pass

def get_img_remove_softlink(version_list):
    os.popen("rm -r ./input/images/").read()
    os.popen("rm -r ./input/images_tmp/").read()
    if not os.path.exists('./input/images'):
        os.makedirs('./input/images')
    if not os.path.exists('./input/images_tmp'):
        os.makedirs('./input/images_tmp')
    for image in version_list:
        os.popen("docker pull "+image+':'+version_list[image]).read()
        if not os.path.exists("./input/images_tmp/"+image):
            os.makedirs("./input/images_tmp/"+image)
        os.popen("docker save -o ./input/images_tmp/"+image+'/'+image+'.tar '+image+':'+version_list[image]).read()
        os.popen("tar -xvf ./input/images_tmp/"+image+'/'+image+'.tar  -C '+ " ./input/images_tmp/"+image+'/').read()
        os.popen("rm " + "./input/images_tmp/" + image + '/' + image + '.tar').read()

        fp = r"./input/images_tmp/"+image
        fileOrdirlist = os.walk(fp)
        layer_list = []
        for filepaths, dirnames, filenames in fileOrdirlist:
            layer_list = dirnames
            break
        for layer in layer_list:
            # fp = r"D:/raw_docker_extract/"  # 目标文件夹
            fp = r"./input/images_tmp/"+image+"/"+layer+"/"  # 目标文件夹
            fileOrdirlist = os.walk(fp)

            for filepaths, dirnames, filenames in fileOrdirlist:
                if "layer.tar" not in filenames:
                    continue
                else:
                    # 解压layer的tar压缩包
                    tar = tarfile.open(filepaths + "layer.tar")
                    names = tar.getnames()
                    # print(names)
                    for name in names:
                        tar.extract(name, filepaths)
                    tar.close()
                    os.remove(filepaths + "layer.tar")
        os.remove('./input/images_tmp/'+image+'/'+image+'.tar')
    copy_dirs('./input/images_tmp', './input/images')
    os.popen('rm -r ./input/images_tmp').read()
    os.popen('chmod 777 -R ./input').read()
    return 0
def get_img(version_list):
    os.popen("rm -r ./input/images/").read()
    if not os.path.exists('./input/images'):
        os.makedirs('./input/images')
    for image in version_list:
        if not os.path.exists("./input/images/"+image):
            os.makedirs("./input/images/"+image)
        os.popen("docker pull "+image+':'+version_list[image]).read()
        os.popen("docker save -o ./input/images/"+image+'/'+image+'.tar '+image+':'+version_list[image]).read()
        os.popen("tar -xvf ./input/images/"+image+'/'+image+'.tar  -C '+ " ./input/images/"+image+'/').read()
        os.popen("rm " + "./input/images/" + image + '/' + image + '.tar').read()
        fp = r"./input/images/"+image
        fileOrdirlist = os.walk(fp)
        layer_list = []
        for filepaths, dirnames, filenames in fileOrdirlist:
            layer_list = dirnames
            break
        for layer in layer_list:
            # fp = r"D:/raw_docker_extract/"  # 目标文件夹
            fp = r"./input/images/"+image+"/"+layer+"/"  # 目标文件夹
            fileOrdirlist = os.walk(fp)

            for filepaths, dirnames, filenames in fileOrdirlist:
                if "layer.tar" not in filenames:
                    continue
                else:
                    # 解压layer的tar压缩包
                    tar = tarfile.open(filepaths + "layer.tar")
                    names = tar.getnames()
                    # print(names)
                    for name in names:
                        tar.extract(name, filepaths)
                    tar.close()
                    os.remove(filepaths + "layer.tar")
    os.popen('chmod 777 -R ./input').read()
    return 0

def del_file(filepath):
    try:
        os.remove(filepath)
        return
    except Exception as e:
        shutil.rmtree(filepath)
        return
def load_json(addr):
    with open(addr, 'r') as load_f:
        load_data = json.load(load_f)
        return load_data
# 清理自冗余文件
def clean_diff_hash_repeat(addr):
    layer_order = [one_layer.split('/')[0] for one_layer in load_json(addr + '/manifest.json')[0]['Layers']]
    file_all_size = 0.0
    addr_file_dict = {}
    for root, dirs, files in os.walk(addr):
        for file in files:
            file_path = os.path.join(root, file).replace('\\', '/')
            file_layer = file_path.split('/')[4]
            # 整个layer都重复的话，不计入减少的冗余量
            if len(file_layer) != 64 and len(file_path.split('/')) == 5:
                continue
            file_path_inlayer = file_path.split(file_layer)[1]

            if addr_file_dict.__contains__(file_path_inlayer):
                print('!!!',file_path)
                if layer_order.index(addr_file_dict[file_path_inlayer][0]) > layer_order.index(file_layer):
                    del_file(file_path)
                if layer_order.index(addr_file_dict[file_path_inlayer][0]) < layer_order.index(file_layer):
                    addr_file_dict[file_path_inlayer] = [file_layer, file_path]
                    del_file(addr_file_dict[file_path_inlayer][1])
            else:
                addr_file_dict[file_path_inlayer] = [file_layer, file_path]
    return file_all_size

def get_dict(image_list):
    all_size = 0
    os.popen("rm -r ./input/DATA/").read()
    if not os.path.exists("./input/DATA/"):
        os.makedirs("./input/DATA/")
    for image in image_list:
        # 清理自冗余文件
        image_all_size = 0
        clean_diff_hash_repeat('./input/images/' + image)
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
                # print("major_path", major_path)
                continue
            if len(file_paths) - len(major_path) == 64:
                whole_layer_path = file_paths
                now_layer = file_paths[-64:]
                layer_dict[now_layer] = {}
            file_structure = file_paths.replace(whole_layer_path, "")
            file_names_filter = []
            for file in file_names:
                # print(file_paths + "/" + file)
                file_path = file_paths + "/" + file
                file_hash = file_hash_coding(file_path)

                if file_hash and os.path.islink(file_path) == False:
                    file_size = get_file_size(file_paths + '/' + file)
                    image_all_size = image_all_size + file_size
                    if file_hash in hash_dict.keys():
                        hash_dict[file_hash].append(
                            {"name": file, "path": '.'+file_paths.split('./input/images')[1], "size": file_size})  # {"ID": [{""},{}]}
                    else:
                        hash_dict[file_hash] = []
                        hash_dict[file_hash].append({"name": file, "path":  '.'+file_paths.split('./input/images')[1], "size": file_size})
                    file = file_hash
                file_names_filter.append(file)
            layer_dict[now_layer][file_structure] = file_names_filter
        print('SIZE', image, image_all_size)
        all_size = all_size + image_all_size
        if not os.path.exists("./input/DATA/" + image):
            os.makedirs("./input/DATA/" + image)
        write_to_file(layer_dict, "./input/DATA/" + image + "/layer_dict.txt")
        write_to_file(hash_dict, "./input/DATA/" + image + "/hash_dict.txt")
        os.popen('chmod 777 -R ./input').read()
    print("ALL", all_size)
    return 0



if __name__ == '__main__':
    if not os.path.exists('./input'):
        os.makedirs('./input')
    version_list = {'python': '3.9.3', 'golang': '1.16.2', 'openjdk': '11.0.11-9-jdk', 'alpine':'3.13.4', 'ubuntu':'focal-20210401', 'memcached':'1.6.8', 'nginx':'1.19.10', 'httpd':'2.4.43', 'mysql':'8.0.23', 'mariadb':'10.5.8', 'redis':'6.2.1', 'mongo':'4.0.23', 'postgres':'13.1', 'rabbitmq':'3.8.13', 'registry':'2.7.0', 'wordpress':'php7.3-fpm', 'ghost':'3.42.5-alpine', 'node':'16-alpine3.11', 'flink':'1.12.3-scala_2.11-java8', 'cassandra':'3.11.9', 'eclipse-mosquitto':'2.0.9-openssl'}
    # image_list = [img for img in version_list]

    image_list = ["rabbitmq", "wordpress", "node", "ghost", "openjdk",  "flink", "redis", "mysql", "ubuntu",
                   "python", "eclipse-mosquitto", "golang", "alpine",  "postgres", "cassandra", "mariadb",
                   "memcached", "httpd", "registry"]

