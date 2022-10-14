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


def process_df(df, processfunction):
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


def process_all(df): return process_df(df, all)

def process_cdf(df): return process_df(df, cdf)

def process_ccdf(df): return process_df(df, ccdf)

def process_pdf(df): return process_df(df, pdf)