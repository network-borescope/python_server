import numpy as np
import pandas as pd
from datetime import timedelta
from sklearn.metrics.pairwise import cosine_similarity

pops = 27
servs = 30

# Modelo de filtro colaborativo baseado em usuário
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
        return round(w_val/total_w + mean, 5), max_sim, mean_sim, 1/sd_sim

# Modelo de filtro colaborativo baseado em item
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
        return round(w_val/total_w + mean, 5), max_sim, mean_sim, 1/sd_sim

# Modelo de filtro colaborativo híbrido, abordagem HAPA
def cf_hybrid_pred(data, tg_line, tg_col, sim_th = 3):
    u_pred, ms_user, as_user, rsd_user = cf_user_based(data, tg_line, tg_col)
    s_pred, ms_serv, as_serv, rsd_serv = cf_serv_based(data, tg_line, tg_col)

    qms_u = ms_user/(ms_user+ms_serv)
    qms_s = ms_serv/(ms_user+ms_serv)

    qas_u = as_user/(as_user+as_serv)
    qas_s = as_serv/(as_user+as_serv)

    qrsd_u = rsd_user/(rsd_user+rsd_serv)
    qrsd_s = rsd_serv/(rsd_user+rsd_serv)

    Q_user = qms_u + qas_u + qrsd_u
    Q_serv = qms_s + qas_s + qrsd_s

    return (Q_user * u_pred + Q_serv * s_pred)/(Q_user + Q_serv)








# Completa todos os zeros da data_matrix
def cf_predict_values(data_matrix):
    mat = np.array(data_matrix)
    for ind in np.argwhere(mat == 0):
        mat[ind[0]][ind[1]] = cf_hybrid_pred(mat, ind[0], ind[1])

    return mat


# Gera uma série temporal da relação entre pops e serviços com intervalos de 1h
# imputs-> data_frame: pandas data frame(id,pop_src,service,timestamp,time), n_pop: número de pops (default = 27), n_servs: número de serviços (default = 30)
# output format-> String: timestamp,pop,serv,boolean(pred,orig),value\n...
def cf_predict_hours(data_frame, n_pops=pops, n_servs=servs):
    pops_ct = list(map(int, data_frame.pop_src.unique()))
    servs_ct = list(map(int, data_frame.service.unique()))
    if len(pops_ct) > 1 and len(servs_ct) > 1:
        resp = []
        init = data_frame.min()["timestamp"]
        init.replace(minute=0, second=0)
        last = data_frame.max()["timestamp"]
        offset = timedelta(hours=1)
        end = init + offset
        while init <= last:
            df = data_frame.loc[(data_frame["timestamp"] >= init) & (data_frame["timestamp"] < end)]
            if not df.empty:
                mat = np.zeros((n_pops,n_servs))
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

# Completa matriz de relações genéricas referente a um delta time gerada por array de array
# imputs-> arr: array [[pop,serv,value],...], n_pop: número de pops (default = 27), n_servs: número de serviços (default = 30)
# output format-> array[[pop,serv,value,boolean(0,1)],...]; 0: valor original, 1:valor artificial
def cf_predict_delta_matrix(arr, n_pops=pops,n_servs=servs):
    resp = []
    mat = np.zeros((n_pops,n_servs))
    for line in arr:
        mat[line[0]-1][line[1]-1] = line[2]

    for i in range(len(mat)):
        for j in range(len(mat[0])):
            if mat[i][j] == 0:
                resp.append([i+1,j+1,cf_hybrid_pred(mat,i,j),1])
            else:
                resp.append([i+1,j+1,mat[i][j],0])

    return resp

# Completa matriz de relações internas referente a um delta time gerada por array de array
# imputs-> arr: array [[pop,serv,value],...], n_pop: número de pops (default = 27), n_servs: número de serviços (default = 30)
# output format-> array[[pop,serv,value,boolean(0,1)],...]; 0: valor original, 1:valor artificial
def cf_predict_delta_automatrix(arr,n_pops=pops):
    resp = []
    mat = np.zeros((n_pops,n_pops))
    for line in arr:
        mat[line[0]-1][line[1]-1] = line[2]

    for i in range(len(mat)):
        for j in range(len(mat[0])):
            if mat[i][j] == 0 and i!=j:
                resp.append([i+1,j+1,cf_hybrid_pred(mat,i,j),1])
            else:
                resp.append([i+1,j+1,mat[i][j],0])

    return resp
