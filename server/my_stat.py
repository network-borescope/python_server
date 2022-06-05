###############################################
## Statistical Analysis Functions
###############################################


def cdf(data_frame):
    return "CDF of dataframe"

def foo1(data_frame):
    return "FOO1 of dataframe"

def foo2(data_frame):
    return "FOO2 of dataframe"

def all(data_frame):
    results = [None, None, None]

    results[0] = cdf(data_frame)
    results[1] = foo1(data_frame)
    results[2] = foo2(data_frame)
    
    return results