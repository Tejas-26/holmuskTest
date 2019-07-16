from logs import logDecorator as lD
import jsonref, pprint, csv
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from psycopg2.sql import SQL, Identifier, Literal
from lib.databaseIO import pgIO
from collections import Counter, defaultdict

from multiprocessing import Pool
from tqdm import tqdm
from time import sleep

comorbidAnalysisConf
    = jsonref.load(open('../config/modules/comorbidAnalysis.json'))
config = jsonref.load(open('../config/config.json'))
@lD.log(logBase + 'numRowsCol')
def numRowsCol(logger):
    try:
        queryNumColumn = '''
        SELECT COUNT (*) column_name
        FROM information_schema.columns
        WHERE table_schema = 'raw_data'
        AND table_name =  'background'
        '''
        queryNumRows = '''
        SELECT COUNT (*)
        FROM raw_data.background
        '''
        #extracting the column name elements from the tuple
        numRowsCol = []
        for tuple in pgIO.getAllData(queryNumRows):
            numRowsCol.append(tuple[0])
        for tuple in pgIO.getAllData(queryNumColumn):
            numRowsCol.append(tuple[0])
    except Exception as e:
        logger.error(f'Unable to get data: {e}')
    return numRowsCol

@lD.log(logBase + 'colNames')
def colNames(logger):
    try:
        queryColumnNames = '''
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'raw_data'
        AND table_name =  'background'
        '''
        #extracting the column name elements from the tuple
        columnNames = []
        for tuple in pgIO.getAllData(queryColumnNames):
            columnNames.append(tuple[0])
    except Exception as e:
        logger.error(f'Unable to get data for the column names: {e}')
    return columnNames


@lD.log(logBase + '.topValuesCol')
def topValuesColBackground(logger, d):
    topVals = {}
    try:
        for column in d["columns"]:
            topVals[column] = {}
            query = SQL('''
            SELECT
                distinct {},
                count({})
            FROM
                raw_data.background
            WHERE
                {} ~ '[^0-9]+$'
            GROUP BY
                {}
            ORDER BY
            	count({}) DESC
            ''').format(
                Identifier(column),
                Identifier(column),
                Identifier(column),
                Identifier(column),
                Identifier(column)
            )
            dataCol = pgIO.getAllData(query)
    except Exception as e:
        logger.error(f'Unable to get data for the column names: {e}')
    return topVals

@lD.log(logBase + '.topValuesCol')
def topValuesColPatientType(logger, d):
    topVals = {}
    try:
        for column in d["columns"]:
            topVals[column] = {}
            query = SQL('''
            SELECT
                distinct {},
                count({})
            FROM
                raw_data.typepatient
            WHERE
                {} ~ '[^0-9]+$'
            GROUP BY
                {}
            ORDER BY
            	count({}) DESC
            ''').format(
                Identifier(column),
                Identifier(column),
                Identifier(column),
                Identifier(column),
                Identifier(column)
            )
            dataCol = pgIO.getAllData(query)
    except Exception as e:
        logger.error(f'Unable to get data for the column names: {e}')
    return topVals

#Creates a json object from CSV file in column based data 
@lD.log(logBase + '.readCSVDictLists')
def getCSVDictLists(logger, filePath):
    dict_lists = {}
    for record in csv.DictReader(open(filePath)):
        for k, v in record.items():
                if(len(v) > 0):
                        dict_lists[k].append(v)

    return dict_lists
