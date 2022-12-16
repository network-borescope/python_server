import json

MAX_LEN = 100

def get_kv(data):
    kv = {}
    #obj = json.loads(data)
    #lst = list(obj.values())[2]
    lst = data

    # Get only list tail of length MAX_LEN 
    if len(lst) > MAX_LEN:
        lst = lst[len(lst)-MAX_LEN:]
    # Build list where key=timestamp and value=latency
    for x in lst:
        kv[x['k'][0]] = x['v'][0]
    return kv