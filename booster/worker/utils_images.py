import csv
import io
import os
import subprocess
import sys
import gzip
from io import BytesIO
import json
import hashlib
import shutil
import requests
import tarfile
import urllib3
import datetime
urllib3.disable_warnings()
auth_url = 'https://auth.docker.io/token'
reg_service = 'registry.docker.io'
empty_json = '{"created":"1970-01-01T00:00:00Z","container_config":{"Hostname":"","Domainname":"","User":"","AttachStdin":false, \
	"AttachStdout":false,"AttachStderr":false,"Tty":false,"OpenStdin":false, "StdinOnce":false,"Env":null,"Cmd":null,"Image":"", \
	"Volumes":null,"WorkingDir":"","Entrypoint":null,"OnBuild":null,"Labels":null}}'


# Get Docker token (this function is useless for unauthenticated registries like Microsoft)
def get_auth_head(type, repository):
    resp = requests.get('{}?service={}&scope=repository:{}:pull'.format(auth_url, reg_service, repository),
                        verify=False, timeout=(2, 5))
    access_token = resp.json()['token']
    auth_head = {'Authorization': 'Bearer ' + access_token, 'Accept': type}
    return auth_head


# Get Docker authentication endpoint when it is required
def get_resp_authentication(registry):
    global auth_url, reg_service
    resp = requests.get('https://{}/v2/'.format(registry), verify=False, timeout=(2, 5))
    if resp.status_code == 401:
        auth_url = resp.headers['WWW-Authenticate'].split('"')[1]
        try:
            reg_service = resp.headers['WWW-Authenticate'].split('"')[3]
        except IndexError:
            reg_service = ""
    return resp


def get_resp_manifest(registry, repository, tag, auth_head):
    resp = requests.get('https://{}/v2/{}/manifests/{}'.format(registry, repository, tag), headers=auth_head,
                 verify=False, timeout=(2, 5))
    # error check
    if resp.status_code != 200:
        print('[-] Cannot fetch manifest for {} [HTTP {}]'.format(repository, resp.status_code))
        print(resp.content)
        auth_head = get_auth_head('application/vnd.docker.distribution.manifest.list.v2+json')
        resp = requests.get('https://{}/v2/{}/manifests/{}'.format(registry, repository, tag), headers=auth_head,
                            verify=False, timeout=(2, 5))
        if resp.status_code == 200:
            print('[+] Manifests found for this tag (use the @digest format to pull the corresponding image):')
            manifests = resp.json()['manifests']
            for manifest in manifests:
                for key, value in manifest["platform"].items():
                    sys.stdout.write('{}: {}, '.format(key, value))
                print('digest: {}'.format(manifest["digest"]))
        exit(1)
    # else:
    #     print(resp.json())
    return resp


# Docker style progress bar
def progress_bar(ublob, nb_traits):
    sys.stdout.write('\r' + ublob[7:19] + ': Downloading [')
    for i in range(0, nb_traits):
        if i == nb_traits - 1:
            sys.stdout.write('>')
        else:
            sys.stdout.write('=')
    for i in range(0, 49 - nb_traits):
        sys.stdout.write(' ')
    sys.stdout.write(']')
    sys.stdout.flush()


def creat_json_file(config, registry, repository, auth_head, my_menu_path):
    confresp = requests.get('https://{}/v2/{}/blobs/{}'.format(registry, repository, config), headers=auth_head,
                            verify=False, timeout=(2, 5))
    file = open(my_menu_path + '{}/{}.json'.format(config[7:], config[7:]), 'wb')
    file.write(confresp.content)
    file.close()


def creat_repositories_file(config, img_parts, img, tag, my_menu_path):
    if len(img_parts[:-1]) != 0:
        content = {'/'.join(img_parts[:-1]) + '/' + img: {tag: config[7:]}}
    else:  # when pulling only an img (without repo and registry)
        content = {img: {tag: config[7:]}}
    file = open(my_menu_path + config[7:] + '/repositories', 'w')
    file.write(json.dumps(content))
    file.close()


def creat_manifest_file(config, resp, img_parts, img, tag, my_menu_path):
    content = [{
        'Config': config[7:] + '.json',
        'RepoTags': [],
        'Layers': []
    }]
    if len(img_parts[:-1]) != 0:
        content[0]['RepoTags'].append('/'.join(img_parts[:-1]) + '/' + img + ':' + tag)
    else:
        content[0]['RepoTags'].append(img + ':' + tag)
    for layer in resp.json()['layers']:
        ublob = layer['digest']
        content[0]['Layers'].append(ublob + '/layer.tar')  # 使用原始digest作为id序号而非再次进行sha256编码

    file = open(my_menu_path + config[7:] + '/manifest.json', 'w')
    file.write(json.dumps(content))
    file.close()



def init_menu_structure(path_menu, nodes_list):
    os.mkdir(path_menu, mode=0o777)
    os.mkdir(path_menu + "image_metadata/", mode=0o777)
    os.mkdir(path_menu + "distribution/", mode=0o777)
    for node in nodes_list:
        os.mkdir(path_menu + "distribution/" + node, mode=0o777)
        os.mkdir(path_menu + "distribution/" + node + "/file/", mode=0o777)
        os.mkdir(path_menu + "distribution/" + node + "/layer/", mode=0o777)
    os.mkdir(path_menu + "file_metadata/", mode=0o777)
    os.mkdir(path_menu + "tmp_storage/", mode=0o777)


def init_system_metadata(images_name_list, nodes_list, my_menu_path):
    """
        初始化目录与metadata
    """

    print("Init of the major menu pulling process starts...")
    tag = 'latest'
    # repo = 'library'
    for image in images_name_list:
        # TODO: pull -> storage three menu -> finish
        img_parts = image.split('/')
        try:
            img, tag = img_parts[-1].split('@')
        except ValueError:
            try:
                img, tag = img_parts[-1].split(':')
            except ValueError:
                img = img_parts[-1]
        # print(img_parts, img, tag)

        if len(img_parts) > 1 and ('.' in img_parts[0] or ':' in img_parts[0]):
            registry = img_parts[0]
            repo = '/'.join(img_parts[1:-1])
        else:
            registry = 'registry-1.docker.io'
            if len(img_parts[:-1]) != 0:
                repo = '/'.join(img_parts[:-1])
            else:
                repo = 'library'
        repository = '{}/{}'.format(repo, img)
        # resp = get_resp_authentication(registry)

        # Fetch manifest v2 and get image layer digests
        auth_head = get_auth_head('application/vnd.docker.distribution.manifest.v2+json', repository)
        resp = get_resp_manifest(registry, repository, tag, auth_head)
        config = resp.json()['config']['digest']

        # my_menu_path = "/home/master70/cluster_menu/"
        if not os.path.exists(my_menu_path):
            init_menu_structure(my_menu_path, nodes_list)

        my_image_metadata_path = my_menu_path + "image_metadata/"
        if not os.path.exists(my_image_metadata_path + config[7:]):
            os.mkdir(my_image_metadata_path + config[7:], mode=0o777)
            # Create JSON file
            creat_json_file(config, registry, repository, auth_head, my_image_metadata_path)

            # Create repositories file
            creat_repositories_file(config, img_parts, img, tag, my_image_metadata_path)

            # Create manifest file
            creat_manifest_file(config, resp, img_parts, img, tag, my_image_metadata_path)
        else:
            continue


def get_single_layer_from_hub(ublob, images_name, save_path, tmp_path):
    ublob = "sha256:" + ublob if "sha256" not in ublob else ublob  # "sha256:" is needed in this func
    layer_dir_construct = save_path + ublob[7:]
    if os.path.exists(layer_dir_construct) or os.path.exists(os.path.join(tmp_path[:-5], "layer", ublob[7:])):
        print(layer_dir_construct, "已存在，跳过拉取...")
        return False
    try:
        os.mkdir(save_path)
    except OSError:
        pass
    layer_dir_tmp = tmp_path + ublob[7:]
    os.mkdir(layer_dir_tmp, mode=0o777)
    img_parts = images_name.split('/')
    try:
        img, tag = img_parts[-1].split('@')
    except ValueError:
        try:
            img, tag = img_parts[-1].split(':')
        except ValueError:
            img = img_parts[-1]
    # Docker client doesn't seem to consider the first element as a potential registry unless there is a '.' or ':'
    if len(img_parts) > 1 and ('.' in img_parts[0] or ':' in img_parts[0]):
        registry = img_parts[0]
        repo = '/'.join(img_parts[1:-1])
    else:
        registry = 'registry-1.docker.io'
        if len(img_parts[:-1]) != 0:
            repo = '/'.join(img_parts[:-1])
        else:
            repo = 'library'
    repository = '{}/{}'.format(repo, img)
    # print("repository:", repository)
    sys.stdout.write(ublob[7:19] + ': Downloading...')
    sys.stdout.flush()
    auth_head = get_auth_head(
        'application/vnd.docker.distribution.manifest.v2+json', repository)  # refreshing token to avoid its expiration
    bresp = requests.get('https://{}/v2/{}/blobs/{}'.format(registry, repository, ublob), headers=auth_head,
                         stream=True, verify=False, timeout=(2, 5))

    # Stream download and follow the progress
    bresp.raise_for_status()  # print check if an error has occurred
    unit = int(bresp.headers['Content-Length']) / 50
    acc = 0
    nb_traits = 0
    progress_bar(ublob, nb_traits)

    with open(layer_dir_tmp + '/layer_gzip.tar', "wb") as file:
        for chunk in bresp.iter_content(chunk_size=8192):
            if chunk:
                file.write(chunk)
                acc = acc + 8192
                if acc > unit:
                    nb_traits = nb_traits + 1
                    progress_bar(ublob, nb_traits)
                    acc = 0
    sys.stdout.write("\r{}: Extracting...{}".format(ublob[7:19], " " * 50))  # Ugly but works everywhere
    sys.stdout.flush()

    with open(layer_dir_tmp + '/layer.tar', "wb") as file:  # 新layer.tar文件
        unzLayer = gzip.open(layer_dir_tmp + '/layer_gzip.tar', 'rb')
        shutil.copyfileobj(unzLayer, file)
        unzLayer.close()
    os.remove(layer_dir_tmp + '/layer_gzip.tar')  # 移除gzip
    shutil.move(layer_dir_tmp, save_path)
    return int(os.path.getsize(save_path + ublob[7:] + '/layer.tar')) / 1024 / 1024  # MB


def get_image_layer_from_hub(ublob_list, images_name, save_path):
    need_ublob_list = []
    for ublob in ublob_list:
        ublob = "sha256:" + ublob if "sha256" not in ublob else ublob
        if not os.path.exists(save_path + ublob[7:]):
            need_ublob_list.append(ublob)

    if len(need_ublob_list) == 0:
        return False
    else:
        try:
            os.mkdir(save_path)
        except OSError:
            pass
        img_parts = images_name.split('/')
        print("img_parts:", img_parts)
        try:
            img, tag = img_parts[-1].split('@')
        except ValueError:
            try:
                img, tag = img_parts[-1].split(':')
            except ValueError:
                img = img_parts[-1]
        # Docker client doesn't seem to consider the first element as a potential registry unless there is a '.' or ':'
        if len(img_parts) > 1 and ('.' in img_parts[0] or ':' in img_parts[0]):
            registry = img_parts[0]
            repo = '/'.join(img_parts[1:-1])
        else:
            registry = 'registry-1.docker.io'
            if len(img_parts[:-1]) != 0:
                repo = '/'.join(img_parts[:-1])
            else:
                repo = 'library'
        repository = '{}/{}'.format(repo, img)
        auth_head = get_auth_head('application/vnd.docker.distribution.manifest.v2+json',
                                  repository)  # refreshing token to avoid its expiration
        return {"need_ublob_list": need_ublob_list, "save_path": save_path, "registry": registry,
                "repository": repository, "auth_head": auth_head}


def compress_layerFile(my_path, tmp_path, file_name=""):
    # compress_cmd = "tar -czPf " + tmp_path + ".tar.gz" + " " + my_path  # -P 使用绝对路径（默认为相对路径）

    target_name = tmp_path + ".tar.gz"
    with tarfile.open(target_name, "w:gz") as archive:
        archive.add(my_path, arcname=file_name)
    return target_name


def remove_layerFile(rm_list):
    for each_lf in rm_list:
        try:
            os.remove(each_lf)
        except Exception as e:
            print("remove_layerFile:", e)
            raise 0

def set_permissions(tarinfo):
    tarinfo.mode = 0o777  # for example
    return tarinfo


def decompress_layerFile(thread_args):
    """

    :param thread_args: flowing params...
    :param my_path: ->  "construct/" + image_id + "/"
    :param tmp_path: -> where lf save temporarily
    :param pure_lf_name: -> lf name(with .tar.gz), so get with [:-7]
    :param granularity: -> “layer” / "file"
    :param pure_tmp_path -> ..tmp/
    :returns dict -> {"download": [layer or file, lf_digest]}
    """

    my_path, tmp_path, pure_lf_name, granularity, pure_tmp_path = thread_args
    if not os.path.exists(my_path):
        try:
            os.mkdir(my_path, mode=0o777)
        except:
            pass
    decompress_path = pure_tmp_path + pure_lf_name[:-7]
    with tarfile.open(tmp_path, "r:gz") as archive:
        archive.extractall(path=pure_tmp_path)
    # print("\r{}: Pull complete [{}]".format(ublob[7:19], bresp.headers['Content-Length']))

    os.remove(tmp_path)
    shutil.move(decompress_path, my_path)
    return [granularity, pure_lf_name[:-7]]


def rm_single_layerFile(thread_args):
    obj_path, granularity, my_id = thread_args
    try:
        shutil.rmtree(obj_path)
    except Exception as e:
        print("rm_single_layerFile:", e)
    return ["rm", granularity, my_id]


def layer_list_get(path):
    with open(path, encoding='utf-8') as f:
        result = json.load(f)
        raw_list = result[0]["Layers"]
    if "sha" in raw_list[0]:
        return [i[7:-10] for i in raw_list]
    else:
        return [i[:-10] for i in raw_list]


def gzip_decompress_rm(target_path, gzip_path, remove=False):
    # Decompress gzip response
    with open(target_path + '/layer.tar', "wb") as file:
        unzLayer = gzip.open(gzip_path + '/layer_gzip.tar', 'rb')
        shutil.copyfileobj(unzLayer, file)
        unzLayer.close()
    if remove:
        os.remove(gzip_path + '/layer_gzip.tar')
    return target_path + '/layer.tar'

def id_json_last_layer(my_path):
    with open(my_path, encoding='utf-8') as f:
        result = json.load(f)
    return result

def layer_json_creat(my_path, my_id, layer_list, image_id):
    with open(my_path + my_id + "/" + 'json', "w") as file:
        if my_id == layer_list[-1]:
            json_obj = id_json_last_layer(my_path + image_id + '.json')
            try:
                del json_obj['history']
            except:
                pass
            try:
                del json_obj['rootfs']
            except:  # Because Microsoft loves case insensitiveness
                del json_obj['rootfS']
        else:
            json_obj = json.loads(empty_json)

        if my_id == layer_list[0]:  # 第一层
            json_obj['parent'] = ""
        else:
            json_obj['parent'] = layer_list[layer_list.index(my_id) - 1]

        json_obj['id'] = my_id  # 写入id
        file.write(json.dumps(json_obj))

def deploy_tar_creat(basic_construct_path, image_id):
    tar_path = basic_construct_path + image_id + ".tar"
    tar = tarfile.open(tar_path, "w")
    tar.add(basic_construct_path + image_id + "/", arcname=os.path.sep)
    tar.close()
    # shutil.rmtree(basic_construct_path + image_id + "/")
    return tar_path

def get_folder_size(path_var):
    size = 0
    item_lst = os.listdir(path_var)
    for i in item_lst:
        path_item = os.path.join(path_var, i)
        if os.path.isfile(path_item):
            size += os.path.getsize(path_item)
        elif os.path.isdir(path_item):
            size += get_folder_size(path_item)
    return int(size / 1024 / 1024)

def cache_copy_compress_rename(layer_path, target_path, old_name, new_name):
    shutil.copytree(layer_path, target_path) if not os.path.exists(target_path) else 0
    old_file = os.path.join(target_path, old_name)
    new_file = os.path.join(target_path, new_name)
    with open(new_file, 'wb') as pw, open(old_file, 'rb') as pr:
        pw.write(gzip.compress(pr.read()))
    os.remove(old_file)

def record_service_pull_time(node_name, image_id, image_name, pull_time):
    now = datetime.datetime.now()
    formatted_time = now.strftime('%Y-%m-%d %H:%M:%S')
    data = [[formatted_time, node_name, image_id, image_name, pull_time]]
    file_name = 'pull_time_record.csv'
    with open(file_name, mode='a', newline='') as file:
        writer = csv.writer(file)
        for row in data:
            writer.writerow(row)

def delete_k8s_service(node_name, image_id, image_name):
    pro = subprocess.Popen("kubectl delete " + image_name + ".yaml", shell=True, stdout=subprocess.PIPE)
    pro.wait()