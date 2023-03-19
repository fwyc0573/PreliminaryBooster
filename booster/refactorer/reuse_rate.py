import os
import json
# 获取文件大小的函数
def get_file_size(path):
    try:
        link_stat = os.lstat(path)
        size = link_stat.st_size
        return size / 1024 / 1024  # 单位 MB
    except Exception as err:
        print("ERROR! GET SIZE", path)
        return 0

def get_folder_size(folder_path):
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(folder_path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            size = get_file_size(fp)
            total_size += size
    return total_size

def load_json(addr):
    with open(addr, 'r') as load_f:
        load_data = json.load(load_f)
        return load_data

def get_input_reuse(img_list):
    layerid_img_dict = {}
    for img in img_list:
        path = './input/images/'+img+'/manifest.json'
        one_img_dict = load_json(path)
        layer_inoneimg_list = [one_layer.split('/')[0] for one_layer in one_img_dict[0]['Layers']]
        for layer in layer_inoneimg_list:
            if layer not in [layer_tmp for layer_tmp in layerid_img_dict]:
                layerid_img_dict[layer] = [img]
            else:
                layerid_img_dict[layer].append(img)

    reuse_size = {}
    for layer in layerid_img_dict:
        if len(layerid_img_dict[layer])>1:
            path = './input/images/' + layerid_img_dict[layer][0] + '/'+layer
            print(layer, layerid_img_dict[layer], get_folder_size(path))
            for img_name in layerid_img_dict[layer]:
                if img_name not in [img_name_tmp for img_name_tmp in reuse_size]:
                    file_size = get_folder_size(path)
                    reuse_size[img_name] = file_size*(len(layerid_img_dict[layer])-1)
                else:
                    file_size = get_folder_size(path)
                    reuse_size[img_name] += file_size*(len(layerid_img_dict[layer])-1)
    print(reuse_size)

def get_output_reuse(img_list):
    layerid_img_dict = {}
    for img in img_list:
        path = './output/'+img+'/unit/'
        file_names = os.listdir(path)
        layer_inoneimg_list = [name for name in file_names if os.path.isdir(os.path.join(path, name))]
        # break
        for layer in layer_inoneimg_list:
            if layer not in [layer_tmp for layer_tmp in layerid_img_dict]:
                layerid_img_dict[layer] = [img]
            else:
                layerid_img_dict[layer].append(img)

    reuse_size = {}
    for layer in layerid_img_dict:
        if len(layerid_img_dict[layer])>1:
            path = './output/' + layerid_img_dict[layer][0] + '/unit/'+layer
            print(layer, layerid_img_dict[layer], get_folder_size(path))
            for img_name in layerid_img_dict[layer]:
                if img_name not in [img_name_tmp for img_name_tmp in reuse_size]:
                    file_size = get_folder_size(path)
                    reuse_size[img_name] =file_size*(len(layerid_img_dict[layer])-1)
                else:
                    file_size = get_folder_size(path)
                    reuse_size[img_name] += file_size*(len(layerid_img_dict[layer])-1)
    print(reuse_size)

if __name__ == '__main__':
    img_list = ['python', 'golang', 'openjdk', 'alpine', 'ubuntu', 'memcached', 'httpd', 'mysql', 'mariadb', 'redis',
                'postgres', 'rabbitmq', 'registry', 'wordpress', 'ghost', 'node', 'flink', 'cassandra',
                'eclipse-mosquitto']
