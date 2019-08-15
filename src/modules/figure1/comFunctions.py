from logs import logDecorator as lD
import jsonref, pprint
import json
import numpy as np
import matplotlib.pyplot as plt
import csv

from psycopg2.sql import SQL, Identifier, Literal
from lib.databaseIO import pgIO
from collections import Counter
from textwrap import wrap

from tqdm import tqdm
from multiprocessing import Pool

config = jsonref.load(open('../config/config.json'))
fig1_config = jsonref.load(open('../config/modules/tejasF1.json'))
logBase = config['logging']['logBase'] + '.modules.figure1.comFunctions'

@lD.log(logBase + '.genDiagCount')
def genDiagCount(logger, filePath):
    '''

    This function generates the percentage of users per race that has a certain diagnosis

    Decorators:
        lD.log

    Arguments:
        logger {logging.Logger}
        filePath {str}

    Returns:
        dict -- dictionary containing the results
    '''
    try:
        resultsDict = {
            "mood": [],
            "anxiety": [],
            "adjustment": [],
            "adhd": [],
            "sud": [],
            "psyc": [],
            "pers": [],
            "childhood": [],
            "impulse": [],
            "cognitive": [],
            "eating": [],
            "smtf": [],
            "disso": [],
            "sleep": [],
            "fd": []
        }

        with open(filePath) as json_file:
            table1results = json.load(json_file)

        for category in resultsDict:
            for race in fig1_config["inputs"]["races"]:
                query = SQL('''
                SELECT count(*)
                FROM tejas.restofusers t1
                INNER JOIN tejas.race_age_t1new t2
                ON t1.siteid = t2.siteid
                AND t1.backgroundid = t2.backgroundid
                WHERE t1.{} is true
                AND t2.race = {}
                ''').format(
                    Identifier(category),
                    Literal(race)
                )
                data = [d[0] for d in pgIO.getAllData(query)]
                data = round((data[0]/table1results[race])*100, 1)
                resultsDict[category].append(data) #percentages

        json_file.close()

    except Exception as e:
        logger.error('Failed to generate count {}'.format(e))

    return resultsDict
