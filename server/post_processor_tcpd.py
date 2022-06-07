import socket
import json
import machine_learning as ml
import pickle
import threading
import util
import my_stat

HOSTNAME = "127.0.0.1" # "localhost"
SERVER_PORT = 8080
BUFFER_SIZE = 4096
MAX_DATA_SIZE = 50 * 10**6 # 50MB
DEFAULT_MODEL = "/ml/isolation-forest"
MODELS = {
        "ml": { # Machine Learning Models
            "isolation-forest": {
                "loaded_model": None,
                "model_file": "models/isolation_forest.sav",
                "model_function": lambda data_frame: ml.callable(data_frame, MODELS["ml"]["isolation-forest"]["loaded_model"]),
                "dataframe_fields": util.FIELDS["ml"]
            },
        },
        "stat": { # Statistical Models
            "all": {"model_function": lambda data_frame: my_stat.process_df(data_frame, my_stat.all), "dataframe_fields": util.FIELDS["stat"] },
            "cdf": {"model_function": lambda data_frame: my_stat.process_df(data_frame, my_stat.cdf), "dataframe_fields": util.FIELDS["stat"] },
            "ccdf": {"model_function": lambda data_frame: my_stat.process_df(data_frame, my_stat.ccdf), "dataframe_fields": util.FIELDS["stat"] },
            "pdf": {"model_function": lambda data_frame: my_stat.process_df(data_frame, my_stat.pdf), "dataframe_fields": util.FIELDS["stat"] },
        }
}




###############################################
## Error Function
###############################################
def error(error_desc):
    response = {"id": 0, "tp": 0, "result": error_desc}
   
    return response


###############################################
## Get choosen model function and fields
###############################################
def get_model_function(model):
    try:
        if model == "/":
            model = DEFAULT_MODEL

        model_type, model_name = model[1:].split("/")
        model_function = MODELS[model_type][model_name]["model_function"]
        dataframe_fields = MODELS[model_type][model_name]["dataframe_fields"]
    
    except:
        return None, None
    
    
    return model_function, dataframe_fields


###############################################
## Process Received Data
###############################################
def process_data(data, model, version=None):
    print("Process data", model)
    try:
        data_json = json.loads(data)
    except:
        return error("Data received isn't a valid JSON.")
    
    model_function, dataframe_fields = get_model_function(model)
    if model_function is None:
        return error('Unknow model ' + model + '.')
    
    if len(data_json) > 0:
        result = util.build_dataframe(data_json, dataframe_fields)

        if result is None:
            return error("Unable to build dataframe from data received. Data must be compatible with choosen model: " + model)
        
        id, tp, data_frame, total_ms = result
        js_result = model_function(data_frame) # process data frame
    else:
        return error("No data to be processed.")

    response_json = {"id": id, "tp": tp, "result": js_result, "ms": total_ms}
    return response_json


###############################################
## Load Machine Learning Models
###############################################
def load_models(reload=False):
    print("Loading models...")

    for model_item in MODELS["ml"].items():
        model = model_item[1]
        if model["loaded_model"] is None or reload:
            model["loaded_model"] = pickle.load(open(model["model_file"], 'rb'))

            print(model_item[0][1:], "Ok.")


###############################################
## Protocol Header Parser
###############################################
def header_parser(str):
    version = ""
    i = 2 # index begins at 2 because str begins with "# "
    
    # must start with version number
    if not ("0" <= str[i] <= "9"):
        return None
    version += str[i]

    i += 1
    while True: # get rest of version number(float)
        if ("0" <= str[i] <= "9") or str[i] == ".":
            version += str[i]
            i += 1
        else:
            break # end of version number
    
    try:
        float(version)
    except ValueError:
        return None
    
    if str[i] != " ":
        return None
    i += 1 # consume whitespace

    # must be "/" to indentify model
    if str[i] != "/":
        return None
    
    while i < len(str):
        if str[i] == "\r":
            i += 1
            if str[i] == "\n":
                return i # end of the HEADER
            else:
                return None
        
        i += 1
    
    return None


###############################################
## Task to be executed by each thread
###############################################
def conn_task(conn, addr, thread_count):
    with conn: # new thread
        print('Connected by', addr, "-> Thread", thread_count)

        # Connection variables
        data = "" # total data received
        header_end = None # header end position
        version = None # protocol version
        model = None # desired model
        buffer_str = None # current string in buffer
        response_json = None # awnser that will be sent
        
        while True:
            buffer = conn.recv(BUFFER_SIZE)
            buffer_str = buffer.decode("utf-8")

            if buffer_str[:2] == "# ":
                header_end = header_parser(buffer_str)

                if header_end is None:
                    response_json = error("Header Error. Expected \"# <version> <path>\"")

                    # sending error message
                    response_str = json.JSONEncoder().encode(response_json)
                    response_bytes = response_str.encode("utf-8")
                    conn.sendall(response_bytes)
                    
                    # closing connection
                    break
                
                _, version, model = buffer_str[:header_end].split(" ") # "# <version> <model>"
                model = model.strip().lower()
                buffer_str = buffer_str[header_end+1:]

            if buffer_str[-4:] == "\r\n\r\n":
                if header_end is None:
                    response_json = error("Missing Header. Expected \"# <version> <path>\"")
                else:
                    data += buffer_str
                    # processing data
                    response_json = process_data(data, model, version)

                # sending awnser
                response_str = json.JSONEncoder().encode(response_json)
                response_bytes = response_str.encode("utf-8")
                conn.sendall(response_bytes)
                
                # closing connection
                break 

            data += buffer_str

            if len(data) > MAX_DATA_SIZE:
                break

        print("Closed connection with", addr, "-> Thread", thread_count)




if __name__ == "__main__":
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((HOSTNAME, SERVER_PORT))
        s.listen()
        
        load_models()
        print("TCP Server Started!!!")

        queue = [] # [(conn, addr)]

        while True:
            conn, addr = s.accept()
    
            t = threading.Thread(target=conn_task, args=(conn, addr, threading.active_count()))
            t.start()

            


    print("TCP Server stopped.")
