from logs import logDecorator as lD
import jsonref, pprint
'''import csv'''
import numpy as np
import pandas as pd

config = jsonref.load(open('../config/config.json'))
logBase = config['logging']['logBase'] + '.modules.LinearRegression.readData'


@lD.log(logBase + '.performLR')
def readCSV_X(logger, fileName):
    data = []
    try:
        data = pd.read_csv(fileName)
        X = data.iloc[:, 0].values.reshape(-1,1)
    except Exception as e:
        logger.error(f'Unable to read data: {e}')
        return []

    return X
    
@lD.log(logBase + '.performLR')
def readCSV_Y(logger, fileName):
    data = []
    try:
        data = pd.read_csv(fileName)
        Y = data.iloc[:, 1].values.reshape(-1,1)
    except Exception as e:
        logger.error(f'Unable to read data: {e}')
        return []

    return Y
