import pandas as pd
import numpy as np
from tensorflow.keras.models import load_model
import json

STATES_FROM_NUMBER = {1: "ac",
                      2: "al",
                      3: "am",
                      4: "ap",
                      5: "ba",
                      6: "ce",
                      7: "df",
                      8: "es",
                      9: "go",
                      10: "ma",
                      11: "mg",
                      12: "ms",
                      13: "mt",
                      14: "pa",
                      15: "pb",
                      16: "pe",
                      17: "pi",
                      18: "pr",
                      19: "rj",
                      20: "rn",
                      21: "ro",
                      22: "rr",
                      23: "rs",
                      24: "sc",
                      25: "se",
                      26: "sp",
                      27: "to"}


def x_from_json(f):
    #d = json.load(f)
    d = f
    df = pd.json_normalize(d['result'])
    df['v'] = df['v'].apply(lambda x: x[0])
    X = df[['v']].iloc[-24:].to_numpy().reshape((1, 24, 1))
    return X


def x_from_args(data):
    lst_x = []
    for js in data:
        x_i = x_from_json(js)
        lst_x.append(x_i)
    return np.concatenate(lst_x, axis=2)


#def bore_model(model_name, from_state, to_state, lst_json, path="models/"):
def bore_model(model_name, data, model_file):
    #f_state = STATES_FROM_NUMBER[from_state]
    #t_state = STATES_FROM_NUMBER[to_state]
    #if model_name == 'hour_model':
        #model = load_model(f"{path}rtt_up_{f_state}_to_{t_state}_target_rtt_avg.h5")
    if model_name == "lstm-1h":
        model = load_model(model_file)
        x = x_from_args(data)
        result = model.predict(x)
        if result[0][0] > 0.5:
            return [1]
        else:
            return [0]
