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
        model_dir, model_name = model[1:].split("/")
        return model_dir, model_name
    except:
        for know_model_dir in MODELS:
            for know_model_name in MODELS[know_model_dir]:
                if know_model_name == model:
                    return know_model_dir, know_model_name
    
        return None, None



###############################################
## Get choosen model function and fields
###############################################
def get_model_function(model):
    model_info = None
    try:
        # if model == "/":
        #     model = DEFAULT_MODEL
        model_info = {}
      
        model_dir, model_name = know_models(model)
        model_function = MODELS[model_dir][model_name]["model_function"]
        
        dataframe_fields = None
        if "dataframe_fields" in MODELS[model_dir][model_name]:
            dataframe_fields = MODELS[model_dir][model_name]["dataframe_fields"]
        
        function_params = None
        if "function_params" in MODELS[model_dir][model_name]:
            function_params = MODELS[model_dir][model_name]["function_params"]
        
        model_info = {
            "name": model_name,
            "dir": model_dir,
            "function": model_function,
            "function_params": function_params,
            "dataframe_fields": dataframe_fields
        }
    
    except Exception as e:
        print(e)
        return None
    
    
    return model_info


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
    
    model_info = get_model_function(model)
    if model_info is None:
        return error('Unknow model ' + model + '.')
    
    if len(data_json) == 0:
        return error("Data length equals to 0, no data to be processed.")
    else:
        js_result = None

        id, tp, total_ms = util.get_tc_result_info(data_json)
        print(id, tp, total_ms)
        if tp == 0:
            error("Received data with tp 0.")
        
        # the only parameter in the "model function" is the data
        if model_info["function_params"] is None:
            if type(data_json) == dict:
                js_result = model_info["function"](data_json["result"])
            else:
                js_result = model_info["function"](data_json)
        
        else: # complex function
            f_args = []
            for item in model_info["function_params"]:
                if item in data_json:
                    #f_args[item] = data_json[item]
                    f_args.append(data_json[item])

                elif model_info["function_params"][item]:
                    return error("Missing key \"" + item + "\" in query")
                
            #def wrapper(args_dict):
                #return model_function(**args_dict)
            
            def wrapper(args_list):
                os.chdir(model_info["dir"])
                result = model_info["function"](*args_list)
                os.chdir('../')
                return result
            
            js_result = wrapper(f_args)

        if js_result is None:
            return error("Unable to apply model '" + model + "' in received data.")

    elapsed = (time.process_time() - start)*1000 # multiply by 1000 to convert to milliseconds
    
    # use tc_format only if has id, tp and total_ms
    if not (id and tp and total_ms):
        tc_format = False
    
    response_json = None
    if tc_format:
        response_json = {"id": id, "tp": tp, "result": js_result, "model": model_info["name"], "ms": total_ms + elapsed}
    else:
        response_json = {"result": js_result, "model": model_info["name"]}
    
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
