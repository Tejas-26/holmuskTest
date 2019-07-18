from logs import logDecorator as lD
import jsonref, pprint

import matplotlib.pyplot as plt
from tqdm import tqdm
import operator
import csv
import json

from psycopg2.sql import SQL, Identifier, Literal

from lib.databaseIO import pgIO
from modules.table1 import comFunctions as cf

table1_config = jsonref.load(open('../config/modules/tejasT1.json'))
config = jsonref.load(open('../config/config.json'))
logBase = config['logging']['logBase'] + '.modules.table1.table1'


@lD.log(logBase + '.main')
def main(logger, resultsDict):
    '''main function for module1

    This function finishes all the tasks for the
    main function. This is a way in which a
    particular module is going to be executed.

    Parameters
    ----------
    logger : {logging.Logger}
        The logger used for logging error information
    resultsDict: {dict}
        A dintionary containing information about the
        command line arguments. These can be used for
        overwriting command line arguments as needed.
    '''

    print('='*30)
    print('Main function of comorbidAnalysis module')
    print('='*30)
    countDict = {
        "AA": [],
        "NHPI":[],
        "MR":[]
    }
    cf.relabelVar()

    raceCounts = cf.countMainRace()
    countDict["AA"].append(raceCounts[0])
    countDict["NHPI"].append(raceCounts[1])
    countDict["MR"].append(raceCounts[2])

    raceAgeCounts = cf.countRaceAge()
    countDict["AA"].append(raceAgeCounts[0])
    countDict["NHPI"].append(raceAgeCounts[1])
    countDict["MR"].append(raceAgeCounts[2])

    raceSexCounts = cf.countRaceSex()
    countDict["AA"].append(raceSexCounts[0])
    countDict["NHPI"].append(raceSexCounts[1])
    countDict["MR"].append(raceSexCounts[2])

    raceSettingCounts = cf.countRaceSetting()
    countDict["AA"].append(raceSettingCounts[0])
    countDict["NHPI"].append(raceSettingCounts[1])
    countDict["MR"].append(raceSettingCounts[2])

    print(countDict)

    obj = json.dumps(countDict)
    f = open("../data/final/sampleCount.json","w+")
    f.write(obj)
    f.close()
    print('Getting out of comorbidAnalysis module')
    print('-'*30)

    return
