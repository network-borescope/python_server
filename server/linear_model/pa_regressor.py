from river import linear_model
from river import preprocessing
from river import optim

from .utils import get_kv

class PARegressor:
    def predict_next(data_in_jsons):
        kv = get_kv(data_in_jsons)

        scale = preprocessing.StandardScaler()
        learn_pa = linear_model.PARegressor(
                C=0.15,
                mode=1,
                eps=0.01,
                learn_intercept=True
            )

        model = scale | learn_pa

        y_values = []
        y_pred_values = []

        y_pred = 0
        y_latency = 0
        prev_latency = 0
        prev_diff = 0
        for k, v in kv.items():

            y_latency = v/1000    
            
            x = {'time': k, 'diff': prev_diff}
            y = y_latency - prev_latency
            y_values.append(y_latency)
            
            y_pred = model.predict_one(x)
            y_pred_values.append(y_latency+y_pred)
            model.learn_one(x, y)
        
            prev_diff = y_latency - prev_latency
            prev_latency = y_latency

        resp = []
        resp.append(y_latency + y_pred)
        return resp