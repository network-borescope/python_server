import numpy as np

def callable(df_teste, loaded_model):

    datetime_teste=df_teste['data'].tolist()
    dist_dns_teste=df_teste['dist_REQ_DNS'].tolist()
    dist_http_teste=df_teste['dist_REQ_WEB'].tolist()
    #tupla_teste=df_teste['Tupla'].tolist()
    id_cliente_teste=df_teste['id_client'].tolist()
    ttl_dns_teste=df_teste['TTL_REQ_DNS'].tolist()
    ttl_http_teste=df_teste['TTL_REQ_WEB'].tolist()

    #Merge duas listas em uma lista de lista
    new_list_teste = [list(x) for x in zip(id_cliente_teste,ttl_dns_teste,dist_dns_teste, ttl_http_teste, dist_http_teste)]


    new_data_teste = np.array(new_list_teste)
    lista=loaded_model.predict(new_data_teste)

    #print("list:",lista)

    anomalias=[]

    # normal=0 / anomalia=1

    for i in range(0,len(lista)):
      if lista[i]==1:
        anomalias.append(0)
      if lista[i]==-1:
        anomalias.append(1)

    #print("anomalias:",anomalias)
    #Merge duas listas em uma lista de lista
    #output = [list(x) for x in zip(datetime_teste, id_cliente_teste,anomalias)]
    #return output

    return anomalias
