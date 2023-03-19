import json

def load_json(addr):
    with open(addr, 'r') as load_f:
        load_data = json.load(load_f)
        return load_data

if __name__ == '__main__':
    data_init = load_json('./trace/stage-dal09-logstash-2017.07.10-0.json')
    url_dict = {}
    for one_record in data_init:
        if one_record['http.request.method'] == "GET":
            cur_url = one_record["http.request.uri"]
            if cur_url not in [url_tmp for url_tmp in url_dict]:
                url_dict[cur_url]=[one_record]
            else:
                url_dict[cur_url].append(one_record)
    dis = [len(url_dict[i]) for i in url_dict]
    dis.sort(reverse=True)
    print(dis, sum([len(url_dict[i]) for i in url_dict]), len([len(url_dict[i]) for i in url_dict]))
    print('!!!', sum(dis[:int(0.01*len(dis))]))