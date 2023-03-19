import json


def load_json(addr):
    with open(addr, 'r') as load_f:
        load_data = json.load(load_f)
        return load_data


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
def get_repeat_size(addr):
    dict_all_new = load_json(addr)
    dict_repeat_size = {}

    final_dict = {}
    for dict_name in dict_all_new:
        if len(dict_all_new[dict_name]) == 1:
            continue
        tmp_img = []
        tmp_size = 0
        for img in dict_all_new[dict_name]:
            tmp_size = img['size']
            if img['img'] not in tmp_img:
                tmp_img.append(img['img'])
        if tmp_size == 0 or len(tmp_img)==1:
            continue
        tmp_img.sort()
        name = ''
        for name_tmp in tmp_img:
            name = name + name_tmp+'_'
            if name_tmp not in dict_repeat_size:
                dict_repeat_size[name_tmp] =tmp_size
            else:
                 dict_repeat_size[name_tmp] = dict_repeat_size[name_tmp] + tmp_size
        if name not in final_dict:
            final_dict[name] = tmp_size
        else:
            final_dict[name] = final_dict[name] + tmp_size
        if len(dict_all_new[dict_name]) > 3:
            print(final_dict[name], dict_all_new[dict_name])
    dict_order = sort_dict(final_dict)
    for i in dict_order:
        print("repeat", i, dict_order[i])
    print(dict_repeat_size)
    return 0
if __name__ == '__main__':
    addr = './input/DATA/all_hash_dict.txt'
    
