import numpy as np
import pandas as pd
from datetime import timedelta
from sklearn.metrics.pairwise import cosine_similarity

pops = 27
servs = 30

def cf_user_based(data, tg_line, tg_col, sim_th = 3):
    mean = data.mean()
    data = np.subtract(data,mean)
    data[tg_line][tg_col] = 0

    tg = [data[tg_line]]
    sim = cosine_similarity(tg,data)

    total_w = 0
    w_val = 0

    sim_data = np.insert(data,len(data[0]), sim, axis=1)

    sim_data = sim_data[sim_data[:, len(data[0])].argsort()[::-1]]
    sim_data = np.delete(sim_data,0, 0)

    for i in range(sim_th):
        w_val += sim_data[i][tg_col] * sim_data[i][len(sim_data[0])-1]
        total_w += sim_data[i][len(sim_data[0])-1]

    sim = sim*-1
    sim.sort()
    sim = sim*-1
    sim = np.delete(sim,0, 1)

    max_sim = sim.item(0)
    mean_sim = sim.mean()
    sd_sim = 0
    for i in range(len(sim)):
        sd_sim += np.power(sim.item(i)-mean_sim, 2)
    sd_sim = np.sqrt(sd_sim/len(sim))



    if total_w == 0:
        return 0.0
    else:
        return round(w_val/total_w + mean, 5), max_sim+ mean_sim+ 1/sd_sim

def cf_serv_based(data, tg_line, tg_col, sim_th = 3):
    mean = data.mean()
    data = np.subtract(data,mean)
    data[tg_line][tg_col] = 0


    data_t = data.transpose()
    tg = [data_t[tg_col]]
    sim = cosine_similarity(tg,data_t)

    total_w = 0
    w_val = 0

    sim_data = np.insert(data_t,len(data_t[0]), sim, axis=1)

    sim_data = sim_data[sim_data[:, len(data_t[0])].argsort()[::-1]]
    sim_data = np.delete(sim_data,0, 0)

    for i in range(sim_th):
        w_val += sim_data[i][tg_line] * sim_data[i][len(sim_data[0])-1]
        total_w += sim_data[i][len(sim_data[0])-1]

    sim = sim*-1
    sim.sort()
    sim = sim*-1
    sim = np.delete(sim,0, 1)

    max_sim = sim.item(0)
    mean_sim = sim.mean()
    sd_sim = 0

    for i in range(len(sim)):
        sd_sim += np.power(sim.item(i)-mean_sim, 2)
    sd_sim = np.sqrt(sd_sim/len(sim))



    if total_w == 0:
        return 0.0
    else:
        return round(w_val/total_w + mean, 5), max_sim+ mean_sim+ 1/sd_sim


def cf_hybrid_pred(data, tg_line, tg_col, sim_th = 3):
    u_pred, Q_user = cf_user_based(data, tg_line, tg_col)
    s_pred, Q_serv = cf_serv_based(data, tg_line, tg_col)

    return (Q_user * u_pred + Q_serv * s_pred)/(Q_user + Q_serv)

def cf_predict_values(data_matrix):
    mat = np.array(data_matrix)
    for ind in np.argwhere(mat == 0):
        mat[ind[0]][ind[1]] = cf_hybrid_pred(mat, ind[0], ind[1])

    return mat

def cf_predict_hours(data):
    pops_ct = list(map(int, data.pop_src.unique()))
    servs_ct = list(map(int, data.service.unique()))
    if len(pops_ct) > 1 and len(servs_ct) > 1:
        resp = []
        init = data.min()["timestamp"]
        init.replace(minute=0, second=0)
        last = data.max()["timestamp"]
        offset = timedelta(hours=1)
        end = init + offset
        while init <= last:
            df = data.loc[(data["timestamp"] >= init) & (data["timestamp"] < end)]
            if not df.empty:
                mat = np.zeros((pops,servs))
                for i in range(len(mat)):
                    for j in range(len(mat[i])):
                        mat[i][j] = df.loc[(df["pop_src"] ==  i) & (df["service"] == j)]["time"].mean() if not df.loc[(df["pop_src"] ==  i) & (df["service"] == j)].empty else 0

                for i in range(len(mat)):
                    for j in range(len(mat[i])):
                        if(mat[i][j] == 0): resp.append(init.strftime('%Y-%m-%d %H:%M:%S') +","+ str(i) +","+ str(j) +","+ "pred" +","+ str(cf_hybrid_pred(mat, i, j)))
                        else: resp.append(init.strftime('%Y-%m-%d %H:%M:%S') +","+ str(i) +","+ str(j) +","+ "orig" +","+ str(mat[i][j]))

            init = end
            end = end + offset

        return resp
    return None
