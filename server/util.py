import datetime
import pandas as pd

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


# tp = 0: erro
# tp = 1: "result": [val]
# tp = 2: "result": { "k": [key], "v": [val] }

###############################################
## Build dataframe from data received
###############################################
def build_dataframe(data_json, dataframe_fields):
    id = None
    tp = None
    total_ms = 0.0
    frame_dict = {} # dict that will be transformed in data frame

    count = -1
    for obj in data_json:
        pop = obj["pop"]
        service = obj["service"]
        
        if obj["data"]["tp"] == 0: continue

        if id is None: id = obj["data"]["id"]
        if tp is None: tp = obj["data"]["tp"]
        
        total_ms = total_ms + obj["data"]["ms"]

        for result_data in obj["data"]["result"]:
            count += 1
            # convert epoch to timestamp
            date = datetime.datetime.fromtimestamp(result_data["k"][0])
            frame_dict[count] = [pop, service, date, result_data["v"][0]]

    try:
        data_frame = pd.DataFrame.from_dict(data=frame_dict, orient='index', columns=dataframe_fields)
    except:
        return None
    
    return id, tp, data_frame, total_ms


###############################################
## Output to Run-length Encoding(rle)
###############################################
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






if __name__ == "__main__":
    import sys
    import json

    if len(sys.argv) < 2:
        print("Missing argument <input_file>")
        sys.exit(1)

    data = ""
    filename = sys.argv[1]
    with open(filename, "r") as fin:
        for line in fin:
            data += line

    fields = ["pop_src", "service", "timestamp", "time"]
    id, tp, data_frame, total_ms = build_dataframe(json.loads(data), fields)
    data_frame.to_csv(filename[:-3]+"csv")