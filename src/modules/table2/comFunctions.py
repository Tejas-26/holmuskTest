from logs import logDecorator as lD
import jsonref, pprint, json
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
table2_config = jsonref.load(open('../config/modules/tejasT2.json'))
logBase = config['logging']['logBase'] + '.modules.table2.comFunctions'
all_userkeys = "../data/raw_data/SUDUser_keys.csv"
all_ppl_counts = json.load(open("../data/final/sampleCount.json"))
sud_ppl_counts = json.load(open("../data/final/diagnosesCount.json"))

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

@lD.log(logBase + '.createsud_usersTable')
def createsud_usersTable(logger):
    '''Creates sud_users

    This function creates the table tejas.sud_users, which contains boolean columns
    for each mental disorder.

    Decorators:
        lD.log

    Arguments:
        logger {logging.Logger} -- logs error information
    '''
    try:
        create_query = '''
        CREATE TABLE tejas.sud_users(
        siteid text,
        backgroundid text,
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
        value = pgIO.commitData(create_query)
        if value == True:
            print("tejas.sud_users table has been created")

    except Exception as e:
        logger. error('Failed to create sud_users table because of {}'.format(e))
    return

@lD.log(logBase + '.popsud_users')
def popsud_users(logger):
    '''Populates sud_users

    This function populates the table tejas.sud_users, which contains boolean columns
    for each mental disorder. If a user's row has True for that column, it means
    that he/she has that disorder, and vice versa.

    Decorators:
        lD.log

    Arguments:
        logger {logging.Logger} -- logs error information
    '''
    try:
        with open(all_userkeys) as f:
            readCSV = csv.reader(f, delimiter=",")

            for user in tqdm(readCSV):
                # print(user)
                popTableQuery = SQL('''
                INSERT INTO
                    tejas.sud_users(siteid, backgroundid, alc, cannabis, amphe, halluc, nicotin, cocaine, opioids, sedate, others, polysub, inhalant)
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
                    Literal(user[0]),
                    Literal(user[1])
                )
                value = pgIO.commitData(popTableQuery)
    except Exception as e:
        logger. error('Failed to populate sud_users table because of {}'.format(e))
    return

@lD.log(logBase + '.delAllFalseSUDusers')
def delAllFalseSUDusers(logger):
    makeNewQry = '''
    CREATE TABLE tejas.sud_users_new (
        siteid text,
        backgroundid text,
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
    value = pgIO.commitData(makeNewQry)
    if value == True:
        print("tejas.sud_users_new table has been created")

    deleteDupliQuery = '''
    INSERT INTO tejas.sud_users_new (siteid, backgroundid, alc, cannabis, amphe, halluc, nicotin, cocaine, opioids, sedate, others, polysub, inhalant)
    SELECT
        siteid,
        backgroundid,
        alc,
        cannabis,
        amphe,
        halluc,
        nicotin,
        cocaine,
        opioids,
        sedate,
        others,
        polysub,
        inhalant
    FROM
        tejas.sud_users
    GROUP BY
        siteid,
        backgroundid,
        alc,
        cannabis,
        amphe,
        halluc,
        nicotin,
        cocaine,
        opioids,
        sedate,
        others,
        polysub,
        inhalant
    '''
    value = pgIO.commitData(deleteDupliQuery)
    if value == True:
        print("Duplicate values succesfully deleted")
    return

@lD.log(logBase + '.createJoined')
def createJoined(logger):
    makeNew = '''
    CREATE TABLE tejas.sud_race_age (
    siteid text,
    backgroundid text,
    age text,
    sex text,
    visit_type text,
    race text,
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
    value = pgIO.commitData(makeNew)
    if value == True:
        print("tejas.sud_race_age table has been created")
    return

@lD.log(logBase + '.popJoined')
def popJoined(logger):
    popQry = '''
    INSERT INTO tejas.sud_race_age (
    siteid, backgroundid, age, sex, visit_type, race, alc, cannabis, amphe,
    halluc, nicotin, cocaine, opioids, sedate, others, polysub, inhalant,
    morethan2sud)
    SELECT
        t1.siteid as siteid,
        t1.backgroundid as backgroundid,
        age, sex, visit_type, race,
        alc,
        cannabis,
        amphe,
        halluc,
        nicotin,
        cocaine,
        opioids,
        sedate,
        others,
        polysub,
        inhalant,
        morethan2sud
    FROM
        tejas.race_age_t1new t1
    INNER JOIN
        tejas.sud_users_new t2
    ON
        t1.siteid = t2.siteid
    AND
        t1.backgroundid = t2.backgroundid
    '''
    value = pgIO.commitData(popQry)
    if value == True:
        print("tejas.sud_race_age joined table has been populated")
    return

@lD.log(logBase + '.countRaceSUDppl')
def countRaceSUDppl(logger):
    result = {"AA":0, "NHPI":0, "MR":0}
    for race in table2_config["inputs"]["races"]:
        query = SQL('''
        WITH subQ AS (
        SELECT *
        FROM tejas.sud_race_age
        WHERE sud_race_age.race = {}
        )
        SELECT count(*) FROM subQ
        ''').format(Literal(race))
        data = [d[0] for d in pgIO.getAllData(query)]
        if data != None:
            result[race] = data[0]
    return result

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
            WITH subQ AS (
            SELECT *
            FROM
                tejas.sud_race_age
            WHERE
                sud_race_age.race = {}
            )
            SELECT count(*) FROM subQ
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
            SELECT alc, cannabis, amphe, halluc, nicotin, cocaine,
            opioids, sedate, others, polysub, inhalant
            FROM tejas.sud_race_age
            WHERE sud_race_age.race = {}
            ''').format(
                Literal(race)
            )
            data = pgIO.getAllData(query)
            for tuple in data:
                if sum(list(tuple))>=2:
                    count[race]+=1
        for race in count:
            countDict["morethan2_sud"].append(count[race])
    except Exception as e:
        logger.error('Cannot find general SUD counts because of {}'.format(e))
    return countDict

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

        for sudcat in table2_config["params"]["sudcats"]:
            for race in table2_config["inputs"]["races"]:
                query = SQL('''
                WITH subQ AS (
                SELECT * FROM tejas.sud_race_age
                WHERE sud_race_age.race = {}
                AND sud_race_age.{} = true
                ) SELECT count(*) from subQ
                ''').format(
                    Literal(race),
                    Identifier(sudcat)
                )
                data = [d[0] for d in pgIO.getAllData(query)]
                countDict[sudcat].append(data[0])

        # Change counts to percentage of the race sample
        resultsDict = {}

    except Exception as e:
        logger.error('Failed to find categorised SUD counts because of {}'.format(e))

    return countDict

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
            "any_sud": {
                "AA":[],
                "NHPI":[],
                "MR":[]
            },
            "morethan2_sud": {}
        }

        # Find number of users in each race who have any SUD, separated into age bins
        for race in table2_config["inputs"]["races"]:
            ageCount = 0
            ageCounts = [0,0,0,0,0]
            for lower, upper in zip(['1', '12', '18', '35', '50'], ['11', '17', '34', '49', '100']):
                query = SQL('''
                WITH subQ AS (
                SELECT * FROM tejas.sud_race_age
                WHERE
                sud_race_age.race = {}
                AND
                sud_race_age.age BETWEEN {} AND {}
                ) SELECT count(*) FROM subQ
                ''').format(
                    Literal(race),
                    Literal(lower),
                    Literal(upper)
                )
                data = [d[0] for d in pgIO.getAllData(query)]
                ageCounts[ageCount] = data[0]
                ageCount+=1
            countDict["any_sud"][race] = ageCounts

        # Find number of users in each race who have >2 SUD, separated into age bins
        count = {
            "AA": [],
            "NHPI": [],
            "MR": []
        }

        for race in table2_config["inputs"]["races"]:
            ageCount = 0
            ageCounts = [0,0,0,0,0]
            for lower, upper in zip(['1', '12', '18', '35', '50'], ['11', '17', '34', '49', '100']):
                query = SQL('''
                SELECT alc, cannabis, amphe, halluc, nicotin, cocaine,
                    opioids, sedate, others, polysub, inhalant
                FROM tejas.sud_race_age
                WHERE sud_race_age.age >= {}
                AND sud_race_age.age <= {}
                AND sud_race_age.race = {}
                ''').format(
                    Literal(lower),
                    Literal(upper),
                    Literal(race)
                )
                data = pgIO.getAllData(query)
                for tuple in data:
                    if sum(list(tuple))>=2:
                        ageCounts[ageCount]+=1
                ageCount+=1
            count[race] = ageCounts
        countDict["morethan2_sud"] = count
    except Exception as e:
        logger.error('Failed to find general SUD counts because of {}'.format(e))
    #print(resultsDict)
    return countDict

@lD.log(logBase + '.genPC')
def genPC(logger, n, d):
    if d == 0:
        return 0
    else:
        return round((n/d)*100, 1)

#attempt to automatically get percentages here
@lD.log(logBase + '.ageBinnedCategorisedSUD')
def ageBinnedCategorisedSUD(logger, race):
    '''

    Finds percentage of the age-binned sample that has
    SUD of a particular substance

    Decorators:
        lD.log

    Arguments:
        logger {logging.Logger} -- logs error information
    '''
    if race == "AA":
        x = 0
    elif race == "NHPI":
        x = 1
    elif race == "MR":
        x = 2
    allAgesCatSUD = allAgesCategorisedSUD()
    try:
        countDict = {}
        for sudcat in table2_config["params"]["sudcats"].keys():
            l = [0,0,0,0,0]
            i = 0
            for lower, upper in zip(['1', '12', '18', '35', '50'], ['11', '17', '34', '49', '100']):
                query = SQL('''
                WITH subQ AS (
                SELECT * FROM tejas.sud_race_age
                WHERE sud_race_age.race = {}
                AND sud_race_age.age >= {}
                AND sud_race_age.age <= {}
                AND sud_race_age.{} = true )
                SELECT count(*) from subQ
                ''').format(
                    Literal(race),
                    Literal(lower),
                    Literal(upper),
                    Identifier(sudcat)
                )
                data = [d[0] for d in pgIO.getAllData(query)]
                l[i] = data[0]
                i+=1
            for j in range(0,len(l)):
                l[j] = genPC(l[j], allAgesCatSUD[sudcat][x])
            countDict[sudcat] = l
    except Exception as e:
        logger.error('Failed to find categorised SUD counts because of {}'.format(e))
    return countDict
