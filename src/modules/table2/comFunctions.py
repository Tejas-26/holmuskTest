from logs import logDecorator as lD
import jsonref, pprint
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
table1_config = jsonref.load(open('../config/modules/tejasT2.json'))
logBase = config['logging']['logBase'] + '.modules.comFunctions.comFunctions'

#NO NEED TO USE THIS FUNCTION AGAIN AFTER KEYS HAVE BEEN GENERATED
@lD.log(logBase + '.genSUDUserKeys')
def genSUDUserKeys(logger):
    '''
    This function generates a .csv file for each SUD user's (siteid, backgroundid)

    Parameters
    ----------
    logger : {logging.Logger}
        The logger used for logging error information
    '''
    try:
        query = '''
        SELECT
            siteid, backgroundid
        FROM
            tejas.restofusers
        WHERE
            sud = true
        GROUP BY siteid, backgroundid
        '''

        data = pgIO.getAllData(query)

        csvfile = "../data/raw_data/SUDUser_keys.csv"

        with open(csvfile,'w+') as output:
            csv_output = csv.writer(output)

            for row in data:
                csv_output.writerow(row)
        output.close()

    except Exception as e:
        logger.error('Failed to generate list of SUD users because of {}'.format(e))

    return

@lD.log(logBase + '.createsudusersTable')
def createsudusersTable(logger):
    '''Creates sudusers

    This function creates the table tejas.sudusers, which contains boolean columns
    for each mental disorder.

    Decorators:
        lD.log

    Arguments:
        logger {logging.Logger} -- logs error information
    '''
    try:
        create_query = '''
        CREATE TABLE tejas.sudusers(
        siteid integer,
        background integer,
        alc bool,
        cannabis bool,
        amphe bool,
        halluc bool,
        nicotin bool,
        cocaine bool,
        opioids bool,
        sedate bool,
        others bool,
        polysub bool,
        inhalant bool,
        morethan2sud bool
        )
        '''
        print(pgIO.commitData(create_query))

    except Exception as e:
        logger. error('Failed to create sudusers table because of {}'.format(e))
    return

@lD.log(logBase + '.popsudusers')
def popsudusers(logger):
    '''Populates sudusers

    This function populates the table tejas.sudusers, which contains boolean columns
    for each mental disorder. If a user's row has True for that column, it means
    that he/she has that disorder, and vice versa.

    Decorators:
        lD.log

    Arguments:
        logger {logging.Logger} -- logs error information
    '''
    try:

        all_userkeys = "../data/raw_data/SUDUser_keys.csv"

        with open(all_userkeys) as f:
            readCSV = csv.reader(f, delimiter=",")

            for user in tqdm(readCSV):

                getQuery = SQL('''
                SELECT
                    siteid,
                    backgroundid,
                    array_agg(distinct cast(dsmno as text)) && array[{}] as alc,
                    array_agg(distinct cast(dsmno as text)) && array[{}] as cannabis,
                    array_agg(distinct cast(dsmno as text)) && array[{}] as amphe,
                    array_agg(distinct cast(dsmno as text)) && array[{}] as halluc,
                    array_agg(distinct cast(dsmno as text)) && array[{}] as nicotin,
                    array_agg(distinct cast(dsmno as text)) && array[{}] as cocaine,
                    array_agg(distinct cast(dsmno as text)) && array[{}] as opioids,
                    array_agg(distinct cast(dsmno as text)) && array[{}] as sedate,
                    array_agg(distinct cast(dsmno as text)) && array[{}] as others,
                    array_agg(distinct cast(dsmno as text)) && array[{}] as polysub,
                    array_agg(distinct cast(dsmno as text)) && array[{}] as inhalant
                FROM
                    raw_data.pdiagnose
                WHERE
                    siteid = {}
                AND
                    backgroundid = {}
                GROUP BY
                    siteid, backgroundid
                ''').format(
                    Literal(table2_config["params"]["sudcats"]["alc"]),
                    Literal(table2_config["params"]["sudcats"]["cannabis"]),
                    Literal(table2_config["params"]["sudcats"]["amphe"]),
                    Literal(table2_config["params"]["sudcats"]["halluc"]),
                    Literal(table2_config["params"]["sudcats"]["nicotin"]),
                    Literal(table2_config["params"]["sudcats"]["cocaine"]),
                    Literal(table2_config["params"]["sudcats"]["opioids"]),
                    Literal(table2_config["params"]["sudcats"]["sedate"]),
                    Literal(table2_config["params"]["sudcats"]["others"]),
                    Literal(table2_config["params"]["sudcats"]["polysub"]),
                    Literal(table2_config["params"]["sudcats"]["inhalant"]),
                    Literal(int(user[0])),
                    Literal(int(user[1]))
                )

                data = pgIO.getAllData(getQuery)

                pushQuery = '''
                INSERT INTO
                    tejas.sudusers(siteid, backgroundid, alc, cannabis, amphe, halluc, nicotin, cocaine, opioids, sedate, others, polysub, inhalant)
                VALUES
                    %s
                '''

                deleteDupliQuery = '''
                DELETE FROM tejas.sudusers a USING (
                    SELECT MAX(ctid) as ctid, patientid
                    FROM tejas.sudusers
                    GROUP BY patientid HAVING count(*) > 1
                    ) b
                WHERE a.patientid = b.patientid
                AND a.ctid <> b.ctid
                '''
                value = pgIO.commitData(deleteDupliQuery)
                if value == True:
                    print("Duplicate values succesfully deleted")

                print(pgIO.commitDataList(pushQuery, data))


    except Exception as e:
        logger. error('Failed to populate sudusers table because of {}'.format(e))
    return

@lD.log(logBase + '.divByAllAges')
def divByAllAges(logger, l):
    '''Divides by total sample of each race

    This function takes in a list of counts and returns a list (of similar structure)
    with the percentage of the counts over the total

    Decorators:
        lD.log
    Arguments:
        logger {logging.Logger} -- logs error information
        l {list} -- l[0] is the no. of AA , l[1] is the no. of NHPI, l[2] is the no. of MR
    '''
    resultList = []
    with open("../data/final/sampleCount.json") as json_file:
        table1results = json.load(json_file)

    allAA = table1results['AA'][0]
    allNHPI = table1results['NHPI'][0]
    allMR = table1results['MR'][0]

    resultList.append(round((l[0]/allAA)*100,1))
    resultList.append(round((l[1]/allNHPI)*100,1))
    resultList.append(round((l[2]/allMR)*100,1))

    json_file.close()

    return resultList

@lD.log(logBase + '.allAgesGeneralSUD')
def allAgesGeneralSUD(logger):
    '''

    Finds percentage of the total sample that has any SUD and more than 2 SUD

    Decorators:
        lD.log

    Arguments:
        logger {logging.Logger} -- logs error information
    '''
    try:

        countDict = {
            "any_sud": [],
            "morethan2_sud": []
        }

        # Find number of users in each race who have any SUD
        any_sud = []
        for race in table2_config["inputs"]["races"]:
            query = SQL('''
            SELECT
                count(*)
            FROM
                sarah.test2 t1
            INNER JOIN
                tejas.sudusers t2
            ON
                t1.patientid = t2.patientid
            WHERE
                t1.race = {}
            ''').format(
                Literal(race)
            )
            data = [d[0] for d in pgIO.getAllData(query)]
            countDict["any_sud"].append(data[0])

        # Find number of users in each race who have >2 SUD
        count = {
            "AA": 0,
            "NHPI": 0,
            "MR": 0
        }

        for race in table2_config["inputs"]["races"]:
            query = SQL('''
            SELECT
                t2.alc,
                t2.cannabis,
                t2.amphe,
                t2.halluc,
                t2.nicotin,
                t2.cocaine,
                t2.opioids,
                t2.sedate,
                t2.others,
                t2.polysub,
                t2.inhalant
            FROM
                sarah.test2 t1
            INNER JOIN
                tejas.sudusers t2
            ON
                t1.patientid = t2.patientid
            WHERE
                t1.race = {}
            ''').format(
                Literal(race)
            )
            data = pgIO.getAllData(query)
            for tuple in data:
                if sum(list(tuple))>=2:
                    count[race]+=1
        for race in count:
            countDict["morethan2_sud"].append(count[race])

        # Change counts to percentage of the race sample
        resultsDict = {}
        for row in countDict:
            resultsDict[row] = divByAllAges(countDict[row])

    except Exception as e:
        logger.error('Failed to find general SUD counts because of {}'.format(e))

    return resultsDict

@lD.log(logBase + '.allAgesCategorisedSUD')
def allAgesCategorisedSUD(logger):
    '''

    Finds percentage of the age-binned sample that have
    SUD of a particular substance

    Decorators:
        lD.log

    Arguments:
        logger {logging.Logger} -- logs error information
    '''
    try:
        countDict = {
            "alc":[],
            "cannabis":[],
            "amphe":[],
            "halluc":[],
            "nicotin":[],
            "cocaine":[],
            "opioids":[],
            "sedate":[],
            "others":[],
            "polysub":[],
            "inhalant":[]
        }

        for race in table2_config["inputs"]["races"]:
            for sudcat in table2_config["params"]["sudcats"]:
                query = SQL('''
                SELECT
                    count(*)
                FROM
                    sarah.test2 t1
                INNER JOIN
                    tejas.sudusers t2
                ON
                    t1.patientid = t2.patientid
                WHERE
                    t1.race = {}
                AND
                    t2.{} = true
                ''').format(
                    Literal(race),
                    Identifier(sudcat)
                )
                data = [d[0] for d in pgIO.getAllData(query)]
                countDict[sudcat].append(data[0])

        # Change counts to percentage of the race sample
        resultsDict = {}
        for row in countDict:
            resultsDict[row] = divByAllAges(countDict[row])

    except Exception as e:
        logger.error('Failed to find categorised SUD counts because of {}'.format(e))

    return resultsDict

@lD.log(logBase + '.divByAgeBins')
def divByAgeBins(logger, lol):
    '''Divide by no. of people of each race in a certain age bin

    This function takes in a list of lists called lol, where lol[0] is the list of AAs, lol[0][0] is for ages 1-11 and lol[0][1] is for ages 12-17 and so forth

    Arguments:
        logger {logging.Logger} -- logs error information
        lol {list of lists} --
    '''

    resultLoL = []
    with open("../data/final/sampleCount.json") as json_file:
        table1results = json.load(json_file)

    ageBinsAA = table1results['AA'][1]
    ageBinsNHPI = table1results['NHPI'][1]
    ageBinsMR = table1results['MR'][1]

    resultLoL.append([round((x/y)*100, 1) for x, y in zip(lol[0], ageBinsAA)])
    resultLoL.append([round((x/y)*100, 1) for x, y in zip(lol[1], ageBinsNHPI)])
    resultLoL.append([round((x/y)*100, 1) for x, y in zip(lol[2], ageBinsMR)])

    return resultLoL

@lD.log(logBase + '.ageBinnedGeneralSUD')
def ageBinnedGeneralSUD(logger):
    '''

    Finds percentage of the age-binned sample that has any SUD and more than 2 SUD

    Decorators:
        lD.log

    Arguments:
        logger {logging.Logger} -- logs error information
    '''
    try:

        countDict = {
            "any_sud": [],
            "morethan2_sud": []
        }

        # Find number of users in each race who have any SUD, separated into age bins
        any_sud = []
        for race in table2_config["inputs"]["races"]:
            counts = []
            for lower, upper in zip(['1', '12', '18', '35', '50'], ['11', '17', '34', '49', '100']):
                query = SQL('''
                SELECT
                    count(*)
                FROM
                    sarah.test2 t1
                INNER JOIN
                    sarah.test3 t2
                ON
                    t1.patientid = t2.patientid
                WHERE
                    t1.race = {}
                AND
                    t1.age BETWEEN {} AND {}
                AND
                    t2.sud = true
                ''').format(
                    Literal(race),
                    Literal(lower),
                    Literal(upper)
                )
                data = [d[0] for d in pgIO.getAllData(query)]
                counts.append(data[0])
            countDict["any_sud"].append(counts)

        # Find number of users in each race who have >2 SUD, separated into age bins
        count = {
            "AA": {
                "1": 0,
                "12": 0,
                "18": 0,
                "35": 0,
                "50": 0
            },
            "NHPI": {
                "1": 0,
                "12": 0,
                "18": 0,
                "35": 0,
                "50": 0
            },
            "MR": {
                "1": 0,
                "12": 0,
                "18": 0,
                "35": 0,
                "50": 0
            }
        }

        for race in table2_config["inputs"]["races"]:
            for lower, upper in zip(['1', '12', '18', '35', '50'], ['11', '17', '34', '49', '100']):

                query = SQL('''
                SELECT
                    t2.alc,
                    t2.cannabis,
                    t2.amphe,
                    t2.halluc,
                    t2.nicotin,
                    t2.cocaine,
                    t2.opioids,
                    t2.sedate,
                    t2.others,
                    t2.polysub,
                    t2.inhalant
                FROM
                    sarah.test2 t1
                INNER JOIN
                    tejas.sudusers t2
                ON
                    t1.patientid = t2.patientid
                WHERE
                    t1.race = {}
                AND
                    t1.age BETWEEN {} AND {}
                ''').format(
                    Literal(race),
                    Literal(lower),
                    Literal(upper)
                )
                data = pgIO.getAllData(query)
                for tuple in data:
                    if sum(list(tuple))>=2:
                        count[race][lower]+=1

        for race in count:
            countDict["morethan2_sud"].append(list(count[race].values()))

        # Change counts to percentage of the race sample
        resultsDict = {}
        for row in countDict:
            resultsDict[row] = divByAgeBins(countDict[row])

    except Exception as e:
        logger.error('Failed to find general SUD counts because of {}'.format(e))

    return resultsDict

@lD.log(logBase + '.ageBinnedCategorisedSUD')
def ageBinnedCategorisedSUD(logger):
    '''

    Finds percentage of the age-binned sample that has
    SUD of a particular substance

    Decorators:
        lD.log

    Arguments:
        logger {logging.Logger} -- logs error information
    '''
    try:
        countDict = {}

        for sudcat in table2_config["params"]["sudcats"].keys():
            list1 = []
            for race in table2_config["inputs"]["races"]:
                list2 = []
                for lower, upper in zip(['1', '12', '18', '35', '50'], ['11', '17', '34', '49', '100']):
                    query = SQL('''
                    SELECT
                        count(*)
                    FROM
                        sarah.test2 t1
                    INNER JOIN
                        tejas.sudusers t2
                    ON
                        t1.patientid = t2.patientid
                    WHERE
                        t1.race = {}
                    AND
                        t1.age BETWEEN {} AND {}
                    AND
                        t2.{} = true
                    ''').format(
                        Literal(race),
                        Literal(lower),
                        Literal(upper),
                        Identifier(sudcat)
                    )
                    data = [d[0] for d in pgIO.getAllData(query)]
                    list2.append(data[0])
                list1.append(list2)
            countDict[sudcat] = list1

        # Change counts to percentage of the race sample
        resultsDict = {}
        for row in countDict:
            resultsDict[row] = divByAgeBins(countDict[row])

    except Exception as e:
        logger.error('Failed to find categorised SUD counts because of {}'.format(e))

    return resultsDict
