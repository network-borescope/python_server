import socket
import json
import pickle
import threading
import time
import importlib
import os
import subprocess

# server utilitie libraries
import util


# globals
HOST = None
SERVER_PORT = None
MAX_DATA_SIZE = None
CONN_TIMEOUT_S = None
MODELS = None
# DEFAULT_MODEL = None
BUFFER_SIZE = 4096




###############################################
## Error Function
###############################################
def error(error_desc):
    response = {"id": 0, "tp": 0, "result": error_desc}
   
    return response


###############################################
## Search "model" in MODELS dict (know models)
###############################################
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
        # if model == "/":
        #     model = DEFAULT_MODEL
      
        model_type, model_name = know_models(model)
        model_function = MODELS[model_type][model_name]["model_function"]
        dataframe_fields = MODELS[model_type][model_name]["dataframe_fields"]
        loaded_model = None
        
        if "loaded_model" in MODELS[model_type][model_name]:
            loaded_model = MODELS[model_type][model_name]["loaded_model"]

        function_params = MODELS[model_type][model_name]["function_params"]
    
    except Exception as e:
        print(e)
        return None, None, None, None, None
    
    
    return model_name, model_function, loaded_model, dataframe_fields, function_params


###############################################
## Process Received Data
###############################################
def process_data(data, model, version=None):
    print("Process data", model)
    tc_format = True # response in tinycubes format
    
    start = time.process_time()
    
    try:
        data_json = json.loads(data)
    except:
        return error("Data received isn't a valid JSON.")
    
    model_name, model_function, loaded_model, dataframe_fields, function_params = get_model_function(model)
    if model_function is None:
        return error('Unknow model ' + model + '.')
    
    if len(data_json) > 0:
        result = None
        id, tp, data_frame, total_ms = None, None, None, None
        js_result = None
        if "result" not in data_json: # build dataframe
            result = util.build_dataframe(data_json, dataframe_fields)

            if result is None:
                return error("Unable to build dataframe from data received. Data must be compatible with choosen model: " + model)
            
            id, tp, data_frame, total_ms = result

            js_result = model_function(data_frame)
        else: # do not build dataframe
            #result = util.build_dataframe(data_json["result"], dataframe_fields)
            if function_params is None:
                js_result = model_function(data_json["result"])
            else:
                f_args = []
                for item in function_params:
                    if item not in data_json and function_params[item]:
                        return error("Missing key \"" + item + "\" in query")
                    
                    f_args.append(data_json[item])
                
                def wrapper(*args):
                    return model_function(args)
                
                js_result = wrapper(f_args)

            tc_format = False

        # if result is None:
        #     return error("Unable to build dataframe from data received. Data must be compatible with choosen model: " + model)
        
        # id, tp, data_frame, total_ms = result
        # if loaded_model:
        #     js_result = model_function(data_frame, model_function) # processed data frame
        # else:
        #     js_result = model_function(data_frame) # processed data frame

        if js_result is None:
            return error("Unable to apply model '" + model + "' in received data.")
    else:
        return error("No data to be processed.")

    elapsed = (time.process_time() - start)*1000 # multiply by 1000 to convert to milliseconds
    
    response_json = None
    if tc_format:
        response_json = {"id": id, "tp": tp, "result": js_result, "model": model_name, "ms": total_ms + elapsed}
    else:
        response_json = {"result": js_result, "model": model_name}
    return response_json


###############################################
## Protocol Header Parser
###############################################
def header_parser(header):
    if header[:2] != "# ":
        return None

    version = ""
    i = 2 # index begins at 2 because str begins with "# "
    
    # must start with version number
    if not ("0" <= header[i] <= "9"):
        return None
    version += header[i]

    i += 1
    while True: # get rest of version number(float)
        if ("0" <= header[i] <= "9") or header[i] == ".":
            version += header[i]
            i += 1
        else:
            break # end of version number
    
    try:
        float(version)
    except ValueError:
        return None
    
    if header[i] != " ":
        return None
    i += 1 # consume whitespace

    # must be "/" to indentify model
    #if header[i] != "/":
        #return None
    
    while i < len(header):
        if header[i] == "\r":
            i += 1
            if header[i] == "\n":
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

        # process header
        buffer_str = conn.recv(BUFFER_SIZE).decode("utf-8")
        header_end = header_parser(buffer_str)

        if header_end:
            _, version, model = buffer_str[:header_end].split(" ") # "# <version> <model>"
            model = model.strip().lower()
            buffer_str = buffer_str[header_end+1:]

        while True:
            try:
                if buffer_str[-4:] == "\r\n\r\n":
                    if header_end is None:
                        response_json = error("Missing Header. Expected '# <version> <path>'.")
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
                
                buffer_str = conn.recv(BUFFER_SIZE).decode("utf-8")
            
            except socket.timeout as e:
                response_json = error("Connection timeout.")
                response_str = json.JSONEncoder().encode(response_json)
                response_bytes = response_str.encode("utf-8")
                conn.sendall(response_bytes)
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
    global HOST, SERVER_PORT, MAX_DATA_SIZE, CONN_TIMEOUT_S, DEFAULT_MODEL, MODELS

    server_conf = None
    with open(conf_file, "r") as f:
        server_conf = json.load(f)

    try:
        HOST = server_conf["host"]
        SERVER_PORT = server_conf["port"]
        MAX_DATA_SIZE = server_conf["max_data_sz"]
        CONN_TIMEOUT_S = server_conf["conn_timeout_sec"]
                
    except Exception as e:
        print("server_conf.json file error", e)
        return False

    # search for models(modules)
    MODELS = {}
    imported_modules = {} # imported python files

    # scan current dir for python models
    for item in os.listdir():
        if item[0] == '.' or item[:2] == "__" or not os.path.isdir(item):
            continue
        
        conf_file = item+"/conf.json"
        if not os.path.exists(conf_file):
            continue
        
        # look for conf file of the model
        with open(conf_file, "r") as f:
            models_conf = json.load(f)

            # install module dependencies from requirements file
            req_file = "{}/requirements.txt".format(item)
            if os.path.exists(req_file):
                cmd = subprocess.run(["pip", "install", "-r", req_file])
                if cmd:
                    print(">>> Failed to install (or already met) requirements of package/model \"{}\".".format(item))

            for model_name in models_conf:
                model = models_conf[model_name]

                py_module = "{}.{}".format(item, model.pop("py_module"))
                
                module = None # obj representing imported python file
                if py_module not in imported_modules:
                    module = importlib.import_module(py_module)
                    imported_modules[py_module] = module
                else:
                    module = imported_modules[py_module]

                model["model_function"] = getattr(module, model.pop("py_function"))

                if item not in MODELS:
                    MODELS[item] = {}
                
                MODELS[item][model_name] = model

                # if DEFAULT_MODEL is None:
                #     DEFAULT_MODEL = "/{}/{}".format(item, model_name)

    # print(HOST, SERVER_PORT, MAX_DATA_SIZE, DEFAULT_MODEL)
    print(HOST, SERVER_PORT, MAX_DATA_SIZE)
    return True



if __name__ == "__main__":
    import sys

    if not load_conf():
        print("Failed to load server conf!")
        sys.exit(1)

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((HOST, SERVER_PORT))
        s.listen()
        
        # load_models()
        print("TCP Server Started ({}, {}).".format(HOST, SERVER_PORT))

        queue = [] # [(conn, addr)]

        while True:
            conn, addr = s.accept()
            conn.settimeout(CONN_TIMEOUT_S)
    
            t = threading.Thread(target=conn_task, args=(conn, addr, threading.active_count()))
            t.start()

            


    print("TCP Server stopped.")
