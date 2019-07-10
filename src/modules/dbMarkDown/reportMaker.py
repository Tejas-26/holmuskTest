from logs import logDecorator as lD
import jsonref, pprint
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from psycopg2.sql import SQL, Identifier, Literal
from lib.databaseIO import pgIO
from collections import Counter

from tqdm import tqdm
from multiprocessing import Pool
from time import sleep

dbMarkDownConf = jsonref.load(open('../config/modules/dbMarkDown.json'))
config = jsonref.load(open('../config/config.json'))
logBase = config['logging']['logBase'] + '.modules.dbMarkDown.reportMaker'


@lD.log(logBase + '.makeIntro')
def makeIntro(logger, r):

    report = f'''
# Report on Table Summariser Module
## Abstract:
This report will give a summary of the target table: raw_data.background of the MindLinc database.
## Summary of Table:
This table has information on {r['totalNumUser']} users, with {r['totalNumColumns']} attribute columns.
## Description of Table:
        '''
    with open('..markdownReport.md', 'w+') as f:
        f.write( report )

@lD.log(logBase + '.makeCols')
def makeCols(logger, r):

    report = f'''
|No.| Attribute   |
|---|-------------|'''

    i = 1
    for columnName in r['columnNames']:
        report = report + f'''
|{i}|{columnName} |'''
        i = i+1

    with open('markdownReport.md', 'a+') as f:
        f.write( report )

    return

@lD.log(logBase + '.makeTop')
def makeTop(logger, sect):

    report = f'''
### Top values for each attribute sorted in descending order'''

    for columnName in sect:
        report =  report + f'''
The top 10 most commonly occurring values of the column {columnName} is:
|Attribute Value|Value Count|
|---------------|-----------|'''

        for value in sect[columnName]:
            report =  report + f'''
|{value}        |{sect[columnName][value]}|'''

    with open('../markdownReport.md', 'a+') as f:
        f.write( report )

    return

@lD.log(logBase + '.maritalDistPP')
def maritalDistPP(logger, data):

    try:
        data = [d[0] for d in data]
        counter = Counter(data)
        return counter
    except Exception as err:
        logger.error(f'{err}')
        return Counter([])

    return

#parallel processing, used in conjunction with above
@lD.log(logBase + '.maritalDistParallel')
def maritalDistParallel(logger):

    p = Pool()

    result = Counter([])
    try:
        query = '''
        SELECT marital from raw_data.background
        '''

        dataIter = pgIO.getDataIterator(query, chunks= 1000)

        for c in tqdm(p.imap(getMaritalDistPP, dataIter), total=501):
            result.update(c)

        return result

    except Exception as e:
        logger.error(f'Unable to generate result: {e}')


    p.close()

    return result

#How to use the above two functions with SQL Querying
    # for col, n in zip(['marital', 'id'], [10, 20]): #create config file with column names

    #     query = SQL('''
    #         SELECT
    #             {}
    #         from
    #             {}.{}
    #         limit {}
    #         ''').format(
    #             Identifier(col),
    #             Identifier('raw_data'),
    #             Identifier('background'),
    #             Literal(n)
    #         )

    #     data = [d[0] for d in pgIO.getAllData(query)]
    #     print(data)

    # maritalDist = utils.getMaritalDistParallel()
    # print(maritalDist)

@lD.log(logBase + '.colDistPP')
def colDistPP(logger, data):

    try:
        data = [d[0] for d in data]
        counter = Counter(data)
        return counter
    except Exception as err:
        logger.error(f'{err}')
        return Counter([])

    return

#parallel processing, used in conjunction with above
@lD.log(logBase + '.colDistParallel')
def colDistParallel(logger, col):

    p = Pool()

    result = Counter([])
    try:
        query = SQL('''
        SELECT
            {}
        FROM
            raw_data.background
        ''').format(
            Identifier(col)
        )
        dataIter = pgIO.getDataIterator(query, chunks= 1000)

        for c in tqdm(p.imap(getMaritalDistPP, dataIter), total=501):
            result.update(c)
        return result
    except Exception as e:
        logger.error(f'Unable to generate result: {e}')
    p.close()

    return result


@lD.log(logBase + 'maritalDist')
def maritalDist(logger):

    result = Counter([])
    try:
        query = '''
        SELECT marital from raw_data.background
        '''

        for data in tqdm(pgIO.getDataIterator(query, chunks= 1000), total=501):
            data = [d[0] for d in data]
            c = Counter(data)
            result.update(c)


        return result

    except Exception as e:
        logger.error(f'Unable to generate result: {e}')

    return result
