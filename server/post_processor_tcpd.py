import socket
import json
import pickle
import threading
import time
import importlib

# server utilitie libraries
import util


# globals
HOST = None
SERVER_PORT = None
MAX_DATA_SIZE = None
MODELS = None
DEFAULT_MODEL = None
BUFFER_SIZE = 4096




###############################################
## Error Function
###############################################
def error(error_desc):
    response = {"id": 0, "tp": 0, "result": error_desc}
   
    return response


def know_models(model):
    try:
        model_type, model_name = model[1:].split("/")
        return model_type, model_name
    except:
        for know_model_type in MODELS:
            for know_model_name in MODELS[know_model_type]:
                if know_model_name == model:
                    return know_model_type, know_model_name
    
        return None, None



###############################################
## Get choosen model function and fields
###############################################
def get_model_function(model):
    try:
        if model == "/":
            model = DEFAULT_MODEL
      
        model_type, model_name = know_models(model)
        model_function = MODELS[model_type][model_name]["model_function"]
        dataframe_fields = MODELS[model_type][model_name]["dataframe_fields"]
        loaded_model = None
        
        if "loaded_model" in MODELS[model_type][model_name]:
            loaded_model = MODELS[model_type][model_name]["loaded_model"]
    
    except Exception as e:
        print(e)
        return None, None, None, None
    
    
    return model_name, model_function, loaded_model, dataframe_fields


###############################################
## Process Received Data
###############################################
def process_data(data, model, version=None):
    print("Process data", model)
    
    start = time.process_time()
    
    try:
        data_json = json.loads(data)
    except:
        return error("Data received isn't a valid JSON.")
    
    model_name, model_function, loaded_model, dataframe_fields = get_model_function(model)
    if model_function is None:
        return error('Unknow model ' + model + '.')
    
    if len(data_json) > 0:
        result = util.build_dataframe(data_json, dataframe_fields)

        if result is None:
            return error("Unable to build dataframe from data received. Data must be compatible with choosen model: " + model)
        
        id, tp, data_frame, total_ms = result
        if loaded_model:
            js_result = model_function(data_frame, model_function) # processed data frame
        else:
            js_result = model_function(data_frame) # processed data frame

        if js_result is None:
            return error("Unable to apply model '" + model + "' in received data.")
    else:
        return error("No data to be processed.")

    elapsed = (time.process_time() - start)*1000 # multiply by 1000 to convert to milliseconds
    response_json = {"id": id, "tp": tp, "result": js_result, "model": model_name, "ms": total_ms + elapsed}
    return response_json


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
    #if str[i] != "/":
        #return None
    
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



###############################################
## Load Machine Learning Models
###############################################
def load_models(reload=False):
    print("Loading models...")

    for model_item in MODELS["ml"].items():
        model = model_item[1]
        if ("loaded_model" in model and not model["loaded_model"]) or reload:
            model["loaded_model"] = pickle.load(open(model["model_file"], 'rb'))

            print("-", model_item[0], "Ok.")
    print("Finished!")



###############################################
## Load server conf
###############################################
def load_conf(conf_file="server_conf.json"):
    global HOST, SERVER_PORT, MAX_DATA_SIZE, DEFAULT_MODEL, MODELS

    server_conf = None
    with open(conf_file, "r") as f:
        server_conf = json.load(f)

    try:
        HOST = server_conf["host"]
        SERVER_PORT = server_conf["port"]
        MAX_DATA_SIZE = server_conf["max_data_sz"]
        MODELS = server_conf["models"]
        for model_type in MODELS:
            for model_name in MODELS[model_type]:
                model_options = MODELS[model_type][model_name]                                 
                m = MODELS[model_type][model_name]

                module = importlib.import_module(model_options.pop("py_module"))
                m["model_function"] = getattr(module, model_options.pop("py_function"))

                # model that has to be loaded
                if "model_file" in m:
                    m["loaded_model"] = False

                if DEFAULT_MODEL is None:
                    DEFAULT_MODEL = "/" + model_type + "/" + model_name
                
    except Exception as e:
        print(e)
        return False

    print(HOST, SERVER_PORT, MAX_DATA_SIZE, DEFAULT_MODEL)
    return True



if __name__ == "__main__":
    import sys

    if not load_conf():
        print("Failed to load server conf!")
        sys.exit(1)

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((HOST, SERVER_PORT))
        s.listen()
        
        load_models()
        print("TCP Server Started ({}, {})!!!".format(HOST, SERVER_PORT))

        queue = [] # [(conn, addr)]

        while True:
            conn, addr = s.accept()
    
            t = threading.Thread(target=conn_task, args=(conn, addr, threading.active_count()))
            t.start()

            


    print("TCP Server stopped.")
