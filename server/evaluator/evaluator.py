#------------------------------------------ development
from copyreg import pickle
import pandas as pd
import numpy as np
import math

from sklearn.neighbors import KNeighborsClassifier
from sklearn.svm import SVC
from sklearn.neural_network import MLPClassifier
from sklearn import tree
from sklearn.model_selection import cross_val_predict

from sklearn.model_selection import train_test_split, KFold, GridSearchCV
import sklearn.metrics as metrics
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score

import json

def knn(x_train,y_train):
  
  #Validação Cruzada do KNN (k-nearest neighbors)

  k_range = list(range(1, 31))
  param_grid = dict(n_neighbors=k_range)
    
  # defining parameter range
  grid = GridSearchCV(KNeighborsClassifier(), param_grid, cv=5, scoring='accuracy', return_train_score=False,verbose=1)
    
  # fitting the model for grid search
  grid_search=grid.fit(x_train, y_train)

  neighbors = grid_search.best_params_
  n = neighbors['n_neighbors']

  #Treino com o Classificador KNN (k-nearest neighbors)

  model_knn = KNeighborsClassifier(n_neighbors = n)
  model_knn.fit(x_train, y_train)
 
  return model_knn

def svm(x_train,y_train):
  c_list = [0.01, 0.1, 1, 5, 10, 50, 100]
  gamma_list = [0.00000001, 0.0000001, 0.000001, 0.00001, 0.0001, 0.001]
  param_grid = [{'C':c_list, 'gamma': gamma_list},]
    
  # defining parameter range
  grid = GridSearchCV(SVC(), param_grid, cv=5, scoring='accuracy', return_train_score=False, verbose=1, refit=True, n_jobs=-1)
    
  # fitting the model for grid search
  grid_search=grid.fit(x_train, y_train)

  n = grid_search.best_params_

  # fitting the model SVC (C-Support Vector Classification)
  model_svc=SVC(C=1, gamma=0.00000001)
  model_svc.fit(x_train, y_train)
 
  return model_svc

def decisionTree(x_train,y_train):
  param_grid = {"criterion": ["gini", "entropy"],
        "min_samples_split": [5, 10, 20],
        "max_depth": [6, 8],
        "min_samples_leaf": [4, 6, 10],
        "max_leaf_nodes": [5, 10, 20],
    }
  # defining parameter range
  grid = GridSearchCV(tree.DecisionTreeClassifier(), param_grid, cv=5, scoring='accuracy', return_train_score=False, verbose=1, refit=True, n_jobs=-1)
    
  # fitting the model for grid search
  grid_search=grid.fit(x_train, y_train)

  n = grid_search.best_params_
  #print(n)

  #fitting model
  model_tree = tree.DecisionTreeClassifier(criterion='gini', max_depth=6, max_leaf_nodes=5, min_samples_leaf=6, min_samples_split=20)
  model_tree.fit(x_train, y_train)

  return model_tree


def process(algorithm, df):
  #Entrada e Saída

  x = np.array(df.drop(['ts', 'th', 'cla'], axis=1))
  y = np.array(df['cla'])

  #Divisão da Base de Treino e Teste

  x_train, x_test, y_train, y_test = train_test_split(x, y, train_size=2/3, random_state=0)

  model = algorithm(x_train, y_train)

  result_algorithm = model.predict(x_test)
  # Use the loaded pickled model to make predictions

  metricsAlgorithm=[]
  metricsAlgorithm.append(accuracy_score(y_test, result_algorithm))
  metricsAlgorithm.append(precision_score(y_test, result_algorithm, average='micro'))
  metricsAlgorithm.append(precision_score(y_test, result_algorithm, average='macro'))
  metricsAlgorithm.append(precision_score(y_test, result_algorithm, average='weighted'))
  metricsAlgorithm.append(recall_score(y_test, result_algorithm, average='micro'))
  metricsAlgorithm.append(recall_score(y_test, result_algorithm, average='macro'))
  metricsAlgorithm.append(recall_score(y_test, result_algorithm, average='weighted'))
  metricsAlgorithm.append(f1_score(y_test, result_algorithm, average='micro'))
  metricsAlgorithm.append(f1_score(y_test, result_algorithm, average='macro'))
  metricsAlgorithm.append(f1_score(y_test, result_algorithm, average='weighted'))

  return metricsAlgorithm


def build_dataframe(data_json):
  if len(data_json[0]['result']) != len(data_json[1]['result']):
    return

  data = []

  for i in range(len(data_json[0]['result'])):

    ts = data_json[0]['result'][i]['k'][0]
    pr = data_json[0]['result'][i]['v'][0]
    th = data_json[1]['result'][i]['v'][0]
    
    data.append([ts,th,pr])

  df = pd.DataFrame(data, columns=['ts', 'th', 'pr'])

  cla=[]
  
  for i in range (len(df['th'])):
    if df['th'][i] >= (0.8*math.pow(10, 6)): # 0.8 kbps * 10^6: >= 0.8 Gbps
      cla.append(2)
    elif df['th'][i] <= (0.2*math.pow(10, 6)): # 0.2 kbps * 10^6: <= 0.2 Gbps
      cla.append(0)
    else:
      cla.append(1)
  
  df['cla'] = cla

  if len(df['cla'].unique()) == 1:
    return

  return df

def evaluate(algorithm, data):
  df = build_dataframe(data)
  if df is None: return

  algorithm = algorithm.lower()
  result = []

  if algorithm ==  "knn":
    result = process(knn, df)

  elif algorithm ==  "svm":
    result = process(svm, df)

  elif algorithm ==  "decisiontree":
    result = process(decisionTree, df)
  
  elif algorithm ==  "all":
    result.append(process(knn, df))
    result.append(process(svm, df))
    result.append(process(decisionTree, df))

  return result


def process_knn(data): return evaluate("knn", data)

def process_svm(data): return evaluate("svm", data)

def process_decisionTree(data): return evaluate("decisionTree", data)

def process_all(data): return evaluate("all", data)


#------------------------------------------ running/code test
if __name__=="__main__":
  
  # Opening JSON file
  f = open('daniel.json')

  # returns JSON object as a dictionary
  df_json = json.load(f)
  
  print(evaluate('decisionTree',df_json))

  #Chamada dos algoritmos
  #print(process(df_json,knn))
  #print(process(df_json,svm))
  #print(process(df_json,decisionTree))
 
  # Closing file
  f.close()