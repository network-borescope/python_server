###############################################
## Statistical Analysis Module
###############################################

import pandas as pd
import numpy as np
import math

# CDF
def cdf(lista):
    x_cdf = lista
    y_cdf = 1. * np.arange(len(lista)) / (len(lista) - 1)

    result = {}
    for i in range(len(lista)):
        result[int(x_cdf[i])] = float(y_cdf[i])

    return list(result.items())

# CCDF
def ccdf(lista):
    x_ccdf = lista
    y_ccdf = 1 - (1. * np.arange(len(lista)) / (len(lista) - 1))

    result = {}
    for i in range(len(lista)):
        if int(x_ccdf[i]) not in result:
            result[int(x_ccdf[i])] = float(y_ccdf[i])

    return list(result.items())


# PDF
def pdf(lista):
    x_pdf = lista

    # Media
    soma = 0
    for x in x_pdf:
        soma += x

    avg = soma / len(x_pdf)

    # desvio padrão
    soma2 = 0
    for x in x_pdf:
        soma2 = soma2 + math.pow((x - avg), 2)

    var = soma2 / len(x_pdf)
    desv = math.sqrt(var)

    result = {}
    for x in x_pdf:
        y_pdfi = (1.0 / desv*math.sqrt(2*math.pi))*(math.exp(-0.5*math.pow((x-avg)/desv,2)))
        result[int(x)] = float(y_pdfi)

    return list(result.items())


def all(data_list):
    results = [None, None, None]

    results[0] = cdf(data_list)
    results[1] = ccdf(data_list)
    results[2] = pdf(data_list)

    return results


def build_dataframe(data):
    df_columns = ["pop", "service", "timestamp", "time"]
    df_rows = []

    def extract_info(obj):
        if obj["data"]["tp"] == 0: return

        row_prefix = [obj["pop"], obj["service"]]

        for result_data in obj["data"]["result"]:
            row = [result_data["k"][0], result_data["v"][0]]

            row = row_prefix+row
            
            if len(row) == len(df_columns):
                df_rows.append(row)
            else:
                print(row)
    
    if type(data) == list:
        for obj in data:
            extract_info(obj)
    else:
        extract_info(data)

    df = None
    try:
        df = pd.DataFrame(df_rows, columns=df_columns)
    except Exception as e:
        print(e)
        return None
    
    return df


def process(data, processfunction):
    df = build_dataframe(data)
    
    if df is None: return

    response = {}
    pops = list(map(int, df["pop"].unique()))
    servs = list(map(int, df["service"].unique()))
  
    for pop_id in pops:
        for serv_id in servs:
            df_req = df.loc[(df['pop'] == pop_id) & (df['service'] == serv_id)]

            if df_req.empty: continue

            time = np.sort(df_req["time"].tolist()) # get sorted "time" list

            if not pop_id in response:
                response[pop_id] = {}

            response[pop_id][serv_id] = processfunction(time)

    return response


def process_all(data): return process(data, all)

def process_cdf(data): return process(data, cdf)

def process_ccdf(data): return process(data, ccdf)

def process_pdf(data): return process(data, pdf)