import datetime
import pandas as pd

# tp = 0: erro
# tp = 1: "result": [val]
# tp = 2: "result": { "k": [key], "v": [val] }

######################################################
## Build dataframe from data received
######################################################
def build_dataframe(data_json, dataframe_fields):
    id = None
    tp = None
    total_ms = 0.0

    # check if data_frame should be build
    if not dataframe_fields: return id, tp, data_json, total_ms

    columns = []
    for _, fields in dataframe_fields.items():
        columns += fields

    data = []

    count = -1
    for obj in data_json:
        if obj["data"]["tp"] == 0: continue

        if id is None: id = obj["data"]["id"]
        if tp is None: tp = obj["data"]["tp"]
        
        total_ms = total_ms + obj["data"]["ms"]

        row_prefix = []
        for item in dataframe_fields["info"]:
            row_prefix.append(obj[item])

        for result_data in obj["data"]["result"]:
            row = []
            count += 1
            for i,item in enumerate(result_data["k"]):
                # first k is timestamp (epoch)
                if i == 0:
                    row.append(datetime.datetime.fromtimestamp(item))
                else:
                    row.append(item)
            
            for item in result_data["v"]:
                row.append(item)

            row = row_prefix+row
            if len(row) == len(columns):
                data.append(row)
            else:
                print(row)
    
    data_frame = None
    try:
        data_frame = pd.DataFrame(data, columns=columns)
    except Exception as e:
        print(e)
        return None

    return id, tp, data_frame, total_ms

######################################################
## Build str from data received
######################################################
def build_str(data_json, row_fields, delimeter=','):
    id = None
    tp = None
    total_ms = 0.0

    # check if data_frame should be build
    if not dataframe_fields: return id, tp, data_json, total_ms

    str_arr = []
    
    for obj in data_json:       
        # data_json ordinary keys
        if obj["data"]["tp"] == 0: continue
        if id is None: id = obj["data"]["id"]
        if tp is None: tp = obj["data"]["tp"]
        total_ms = total_ms + obj["data"]["ms"]

        line_prefix = ""
        for item in row_fields["info"]:
            if len(line_prefix) == 0:
                line_prefix += str(obj[item])
            else:
                line_prefix += delimiter + str(obj[item])

        for result_data in obj["data"]["result"]:
            line = ""
            
            for i,item in enumerate(result_data["k"]):
                # first k is timestamp (epoch)
                if i == 0:
                    line += delimiter + str(datetime.datetime.fromtimestamp(item))
                else:
                    line += delimiter + str(item)
            
            for value in result_data["v"]:
                line += delimeter + str(value)
            
            str_arr.append(line_prefix + line)
    
    return id, tp, str_arr, total_ms
    

######################################################
## Output to Run-length Encoding(rle)
######################################################
def output_to_rle(callable_output):
    rle = []
    count = 0
    actual_val = None
    for item in callable_output:
        val = item[2]
        if actual_val is not None:
            if val == actual_val:
                count += 1
            else:
                elem = [count, val]
                rle.append(elem)
                count = 0
        else:
            actual_val = val
            count += 1

    if count != 0:
        elem = [count, val]
        rle.append(elem)
    
    return rle



######################################################
## Build TC pattern response from processing response
######################################################
def build_tc_response(response, keys=[], output_res=[]):
    curr_keys = keys.copy()
    for i, item in enumerate(response):
        if (type(response) == dict): # item is a key
            if i == 0: curr_keys.append(item)
            else: curr_keys[-1] = item

            build_tc_response(response[item], curr_keys, output_res)
        else: # item is a value
            v = item
            if (type(v) != list): v = [item]

            if i == 0:
                output_res.append({"k": curr_keys, "v": [v]})
            else:
                output_res[-1]["v"].append(v)
        
    
    return output_res


def get_tc_result_info(ts_result):
    id = None
    tp = None
    total_ms = 0.0
    
    def get_info(result):
        info = [None, None, None]

        if "id" in result:
            info[0] = result["id"]

        if "tp" in result:
            info[1] = result["tp"]

        if "ms" in result:
            info[2] = result["ms"]
        
        return info

    if type(ts_result) == list:
        for result in ts_result:
            info = None
            if "data" in result:
                info = get_info(result["data"])
            else:
                info = get_info(result)
            
            if id is None and info[0]: id = info[0]
            if tp is None and info[1]: tp = info[1]
            if info[2]: total_ms += info[2]
    
    elif type(ts_result) == dict:
        info = get_info(ts_result)

        if id is None: id = info[0]
        if tp is None: tp = info[1]
        if info[2]: total_ms += info[2]
    
    return id, tp, total_ms


if __name__ == "__main__":
    import sys
    import json

    if len(sys.argv) < 2:
        print("Missing argument <input_file>")
        sys.exit(1)

    FIELDS = {
        "stat": ["pop_src", "service", "timestamp", "time"],
        "ml": [ 
            "data", "dia_semana", "hora", "minuto",
            "id_client", "ip_origem_REQ_DNS", "dist_REQ_DNS", "TTL_REQ_DNS", 
            "ip_destino_REQ_DNS", "ip-id_REQ_DNS", "flag_+_REQ_DNS", "flag_%_REQ_DNS",
            "flag_*_RESP_DNS", "flag_-_RESP_DNS", "flag_|_RESP_DNS", "flag_$_RESP_DNS",
            "ip_orig_REQ_WEB", "dist_REQ_WEB", "TTL_REQ_WEB", "port_dest_REQ_WEB",
            "serv", "ip_dest_REQ_WEB", "query_fei_REQ_DNS_acess_REQ_WEB", "deltatime"
        ]
    }

    data = ""
    filename = sys.argv[1]
    with open(filename, "r") as fin:
        data_json = json.load(fin)
        
        id, tp, data_frame, total_ms = build_dataframe(data_json, FIELDS["stat"])
        data_frame.to_csv(filename[:filename.rfind(".")]+".csv")

        id, tp, data_str, total_ms = build_str(data_json, ["pop", "service"])
        with open(filename[:filename.rfind(".")]+"_str.csv", "w") as fout:
            for line in data_str:
                print(line, file=fout)