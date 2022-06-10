###############################################
## Statistical Analysis Module
###############################################

import pandas as pd
import numpy as np
import seaborn as sns



#CDF
def cdf(data_list):
    x_cdf=np.sort(data_list)
    y_cdf = 1. * np.arange(len(data_list)) / (len(data_list) - 1)

    result = []

    for i in range(len(data_list)):
        result.append((int(x_cdf[i]),float(y_cdf[i])))

    return result


#CCDF
def ccdf(data_list):
    x_ccdf=np.sort(data_list)
    y_ccdf = 1 - (1. * np.arange(len(data_list)) / (len(data_list) - 1))

    result = []

    for i in range(len(data_list)):
        result.append((int(x_ccdf[i]),float(y_ccdf[i])))

    return result


#PDF
def pdf(data_list):
    x_pdf,y_pdf=sns.distplot(data_list).get_lines()[0].get_data()

    result = []

    for i in range(len(x_pdf)):
        result.append((x_pdf[i],y_pdf[i]))

    return result


def all(data_list):
    results = [None, None, None]

    results[0] = cdf(data_list)
    results[1] = ccdf(data_list)
    results[2] = pdf(data_list)
    
    return results


def process_df(df, processfunction):
    response={}

    for pop_id in range(0, 28):
        for serv_id in range(0, 32):
            df_req = df.loc[(df['pop_src'] == pop_id) & (df['service'] == serv_id)]

            if df_req.empty: continue

            time = df_req['time'].tolist() #convert into a list

            if not pop_id in response:
                response[pop_id] = {}

            response[pop_id][serv_id] = processfunction(time)

    return response