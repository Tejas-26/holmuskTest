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
table1_config = jsonref.load(open('../config/modules/tejasT1.json'))
logBase = config['logging']['logBase'] + '.modules.comFunctions.comFunctions'

max_num = 501138

@lD.log(logBase + '.cleanUp')
def cleanUp(logger):
    dropQuery = SQL('''
    DROP TABLE IF EXISTS tejas.raceAgeT1A;
    DROP TABLE IF EXISTS tejas.raceaget1new;
    DROP TABLE IF EXISTS tejas.restofusers;
    ''')
    value = pgIO.commitData(dropQuery)
    if value == True:
        print("All tables have been successfully deleted")
    return

@lD.log(logBase + '.getCSVDictLists')
def getCSVDictLists(logger, filePath):
    dict_lists = {}
    for record in csv.DictReader(open(filePath)):
        for k, v in record.items():
                if(len(v) > 0):
                        dict_lists[k].append(v)

    return dict_lists

@lD.log(logBase + '.getRace')
def getRace(logger):
    '''Generates raceCount.csv

    This function was used to generate the data for the raceCount.csv file, which
    gets the race and count(race) for ALL the races in raw_data.background.
    After manual selection and grouping, the races under each race in the paper (AA, NHPI, MR) were manually entered into the json config file
    Function was deleted from the main after use.

    Parameters
    ----------
    logger : {logging.Logger}
        The logger used for logging error information
    '''

    try:

        query = '''
        SELECT
        race,
        COUNT(race)
        FROM raw_data.background
        GROUP BY race
        '''

        data = pgIO.getAllData(query)
        # data = [d[0] for d in data]

    except Exception as e:
        logger.error('getRace failed because of {}'.format(e))

    return data

# for table 1 stuff #
@lD.log(logBase + '.countMainRace')
def countMainRace(logger):
    '''generates a dictionary with the race str values under each race

    This function generates a dict with all the str values under a race from the raceCount CSV

    Decorators:
        lD.log

    Arguments:
        logger {logging.Logger} -- The logger used for logging error information
        inputCSV {filepath that contains the csv} -- first column "race" contains the race strings, second column "count" contains their counts, and the third column "paper_race" contains the overarching race specified in the paper
    Returns:
        raceDict -- dictionary that contains all the race strings under each specified race in the paper
    '''
    inputCSV = '../data/intermediate/Tejas\' raceCount - raceCount.csv'
    try:
        raceDict = {
                'AA': 0,
                'NHPI': 0,
                'MR': 0
        }

        with open(inputCSV) as f:
            readCSV = csv.reader(f, delimiter=',')
            for row in readCSV:
                for race in table1_config["inputs"]["races"]:
                    if row[2] == race:
                        raceDict[race] += int(row[1])

    except Exception as e:
        logger.error('countMainRace failed because of {}'.format(e))

    return raceDict

@lD.log(logBase + '.createRaceAgeT1A')
def createRaceAgeT1A(logger):
    '''Creates the table tejas.raceAgeT1A

    This function creates the table tejas.raceAgeT1A

    Parameters
    ----------
    logger : {logging.Logger}
        The logger used for logging error information
    '''

    try:
        createTableQuery = '''
        CREATE TABLE tejas.raceAgeT1A (
            age text,
            visit_type text,
            sex text,
            race text,
            siteid text,
            backgroundid text
        )
        '''

        value = pgIO.commitData(createTableQuery)
        if value == True:
            print("tejas.raceAgeT1A table has been created")

        popTableQuery = SQL('''
        INSERT INTO tejas.raceAgeT1A
        SELECT
        	t1.age,
        	t1.visit_type,
        	t2.sex,
        	t2.race,
        	t1.siteid,
        	t1.backgroundid
    	FROM
        	raw_data.typepatient t1
    	INNER JOIN
        	raw_data.background t2
    	ON
        	t1.backgroundid = t2.id
    	AND
        	t1.siteid = t2.siteid
    	WHERE
        	CAST (t1.age AS INTEGER) < 100
    	AND
        	t1.visit_type in {}
    	AND
        	t2.sex in {}
    	AND
        	t2.race in {}
    	''').format(
        	Literal(tuple(table1_config["params"]["settings"]["all"])),
        	Literal(tuple(table1_config["params"]["sexes"]["all"])),
        	Literal(tuple(table1_config["params"]["races"]["all"])))

        value = pgIO.commitData(popTableQuery)
        if value == True:
            print("tejas.raceAgeT1A table has been populated")
        makeNewQry = '''
        CREATE TABLE tejas.raceaget1new (
            age text,
            sex text,
            visit_type text,
            race text,
            siteid text,
            backgroundid text
        )
        '''

        value = pgIO.commitData(makeNewQry)
        if value == True:
            print("tejas.raceaget1new table has been created")
        deleteDupliQuery = '''
        INSERT INTO tejas.raceaget1new (age, sex, visit_type, race, siteid, backgroundid)
        SELECT
            (array_agg(distinct age))[1] as age,
            sex,
            visit_type,
            race,
            siteid,
            backgroundid
        FROM
            tejas.raceAgeT1A
        GROUP BY
        	siteid,
        	backgroundid,
        	sex,
        	visit_type,
        	race
        '''
        value = pgIO.commitData(deleteDupliQuery)
        if value == True:
            print("Duplicate values in tejas.raceAgeT1A has been deleted and the earliest age is taken")

    except Exception as e:
        logger.error('Failed to generate table {}'.format(e))
    return

@lD.log(logBase + '.createrestofusers')
def createrestofusers(logger):
    '''Creates tejas.restofusers table

    Decorators:
        lD.log

    Arguments:
        logger {logging.Logger} -- logs error information
    '''
    try:
        query = '''
        CREATE TABLE tejas.restofusers(
            siteid text,
            backgroundid text,
            mood bool,
            anxiety bool,
            adjustment bool,
            adhd bool,
            sud bool,
            psyc bool,
            pers bool,
            childhood bool,
            impulse bool,
            cognitive bool,
            eating bool,
            smtf bool,
            disso bool,
            sleep bool,
            fd bool
        )'''
        value = pgIO.commitData(query)
        if value == True:
            print("tejas.restofusers table has been successfully created")
    except Exception as e:
        logger.error('Unable to create table restofusers because {}'.format(e))
    return

@lD.log(logBase + '.popDiagCols')
def popDiagCols(logger):
    '''Populates tejas.restofusers table

    Using the .csv file of the first filtered users' [siteid, backgroundid], these users' DSM numbers are aggregated into an array and compared against the array of DSM numbers of a specific diagnosis.
    If the user's DSM numbers are found in the diagnosis array, the column representing the diagnosis is filled in with a "True" value for the user, and vice versa.
    The columns created are stored in a new table tejas.restofusers, then the second filter is completed by removing users that have all their diagnosis columns set to "False" (i.e. they have no mental disorder restofusers that we have specified)

    Decorators:
        lD.log

    Arguments:
        logger {logging.Logger} -- logs error information
    '''
    all_userkeys = "../data/raw_data/allUserKeys.csv"
    with open(all_userkeys, 'r') as f:
        readCSV = csv.reader(f, delimiter=",")

        for user in tqdm(readCSV):
            getQuery = SQL('''
            INSERT INTO
                tejas.restofusers(siteid,
                backgroundid,
                mood,
                anxiety,
                adjustment,
                adhd,
                sud,
                psyc,
                pers,
                childhood,
                impulse,
                cognitive,
                eating,
                smtf,
                disso,
                sleep,
                fd)
            SELECT
                siteid,
                backgroundid,
                array_agg(distinct cast(dsmno as text)) && array[{}] as mood,
                array_agg(distinct cast(dsmno as text)) && array[{}] as anxiety,
                array_agg(distinct cast(dsmno as text)) && array[{}] as adjustment,
                array_agg(distinct cast(dsmno as text)) && array[{}] as adhd,
                array_agg(distinct cast(dsmno as text)) && array[{}] as sud,
                array_agg(distinct cast(dsmno as text)) && array[{}] as psyc,
                array_agg(distinct cast(dsmno as text)) && array[{}] as pers,
                array_agg(distinct cast(dsmno as text)) && array[{}] as childhood,
                array_agg(distinct cast(dsmno as text)) && array[{}] as impulse,
                array_agg(distinct cast(dsmno as text)) && array[{}] as cognitive,
                array_agg(distinct cast(dsmno as text)) && array[{}] as eating,
                array_agg(distinct cast(dsmno as text)) && array[{}] as smtf,
                array_agg(distinct cast(dsmno as text)) && array[{}] as disso,
                array_agg(distinct cast(dsmno as text)) && array[{}] as sleep,
                array_agg(distinct cast(dsmno as text)) && array[{}] as fd
            FROM
                raw_data.pdiagnose
            WHERE
                siteid = {}
            AND
                backgroundid = {}
            GROUP BY
                siteid, backgroundid
            ''').format(
                Literal(table1_config["params"]["categories"]["mood"]),
                Literal(table1_config["params"]["categories"]["anxiety"]),
                Literal(table1_config["params"]["categories"]["adjustment"]),
                Literal(table1_config["params"]["categories"]["adhd"]),
                Literal(table1_config["params"]["categories"]["sud"]),
                Literal(table1_config["params"]["categories"]["psyc"]),
                Literal(table1_config["params"]["categories"]["pers"]),
                Literal(table1_config["params"]["categories"]["childhood"]),
                Literal(table1_config["params"]["categories"]["impulse"]),
                Literal(table1_config["params"]["categories"]["cognitive"]),
                Literal(table1_config["params"]["categories"]["eating"]),
                Literal(table1_config["params"]["categories"]["smtf"]),
                Literal(table1_config["params"]["categories"]["disso"]),
                Literal(table1_config["params"]["categories"]["sleep"]),
                Literal(table1_config["params"]["categories"]["fd"]),
                Literal(user[0]),
                Literal(user[1])
            )
            value = pgIO.commitData(getQuery)
    f.close()
    return

@lD.log(logBase + '.delAllFalserestofusers')
def delAllFalserestofusers(logger):
    '''Second filter of users from tejas.restofusers

    Deletes users who have no target mental disorder diagnoses.

    Decorators:
        lD.log

    Arguments:
        logger {logging.Logger} -- logs error information
    '''
    try:
        query = '''
        DELETE FROM
            tejas.restofusers
        WHERE
            mood = false and
            anxiety = false and
            adjustment = false and
            adhd = false and
            sud = false and
            psyc = false and
            pers = false and
            childhood = false and
            impulse = false and
            cognitive = false and
            eating = false and
            smtf = false and
            disso = false and
            sleep = false and
            fd = false'''
        value = pgIO.commitData(query)
        if value == True:
            print("Users with no diagnosis in tejas.restofusers table has been successfully deleted")
    except Exception as e:
        logger.error('Unable to delete from table restofusers because {}'.format(e))
    return

@lD.log(logBase + '.countRaceAge')
def countRaceAge(logger):
    '''

    This function queries the database and returns the counts of each main race: AA, NHPI, MR sorted into age bins.
    Parameters
    ----------
    logger : {logging.Logger}
        The logger used for logging error information
    '''

    try:
        total = []
        for race in table1_config["inputs"]["races"]:
            counts = []
            for lower, upper in zip(['1', '12', '18', '35', '50'], ['11', '17', '34', '49', '100']):
                query = SQL('''
                SELECT
                    count(*)
                FROM
                    tejas.raceaget1new t1
                INNER JOIN
                    tejas.restofusers t2
                ON
                    t1.siteid = t2.siteid
                AND
                    t1.backgroundid = t2.backgroundid
                WHERE
                    (cast (t1.age as int) >= {}) AND cast (t1.age as int) <= {} and t1.race = {}
                ''').format(
                    Literal(lower),
                    Literal(upper),
                    Literal(race)
                )
                data = [d[0] for d in pgIO.getAllData(query)]
                #print("age range: "+str(lower)+"-"+ str(upper)+" count: "+str(data))
                counts.append(data[0])
            total.append(counts)

    except Exception as e:
        logger.error('countRaceAge failed because of {}'.format(e))

    return total

@lD.log(logBase + '.countRaceSex')
def countRaceSex(logger):
    '''

    This function queries the database and returns the counts of each main race: AA, NHPI, MR sorted by sex.
    Parameters
    ----------
    logger : {logging.Logger}
        The logger used for logging error information
    '''

    try:
        total = []
        for race in table1_config["inputs"]["races"]:
            counts = []
            for sex in table1_config["inputs"]["sexes"]:
                query = SQL('''
                SELECT
                    count(*)
                FROM
                    tejas.raceaget1new t1
                INNER JOIN
                    tejas.restofusers t2
                ON
                    t1.siteid = t2.siteid
                AND
                    t1.backgroundid = t2.backgroundid
                WHERE
                    t1.sex = {} AND t1.race = {}
                ''').format(
                    Literal(sex),
                    Literal(race)
                )
                data = [d[0] for d in pgIO.getAllData(query)]
                counts.append(data[0])
            total.append(counts)

    except Exception as e:
        logger.error('countRaceSex failed because of {}'.format(e))

    return total

@lD.log(logBase + '.countRaceSetting')
def countRaceSetting(logger):
    '''

    This function queries the database and returns the counts of each main race: AA, NHPI, MR sorted by treatment setting.
    Parameters
    ----------
    logger : {logging.Logger}
        The logger used for logging error information
    '''

    try:
        total = []
        for race in table1_config["inputs"]["races"]:
            counts = []
            for setting in table1_config["inputs"]["settings"]:
                query = SQL('''
                SELECT
                    count(*)
                FROM
                    tejas.raceAgeT1A t1
                INNER JOIN
                    tejas.restofusers t2
                ON
                    t1.siteid = t2.siteid
                AND
                    t1.backgroundid = t2.backgroundid
                WHERE
                    t1.visit_type = {} AND t1.race = {}
                ''').format(
                    Literal(setting),
                    Literal(race)
                )
                data = [d[0] for d in pgIO.getAllData(query)]
                counts.append(data[0])
            total.append(counts)
    except Exception as e:
        logger.error('countRaceSetting failed because of {}'.format(e))

    return total

@lD.log(logBase + '.genAllKeys')
def genAllKeys(logger):
    '''

    This function generates a .csv file of (siteid, backgroundid) of users after the first filter of age, race, sex and setting are done.
    The .csv file will then be used for the second filter by dsmno.

    Parameters
    ----------
    logger : {logging.Logger}
        The logger used for logging error information
    '''
    all_userkeys = "../data/raw_data/allUserKeys.csv"
    with open(all_userkeys, 'w') as f:
        filewriter = csv.writer(f, delimiter=',')
        for race in table1_config["params"]["races"]["all"]:
            print("currently getting data for the " + race + " race")
            x = 0
            # and (cast (id as int) > 0) and (cast (id as int) < 1000)
            query = SQL('''
            select t1.siteid, t2.backgroundid
            from (
                select id, siteid
                from raw_data.background
    	        where race = {}
            ) as t1
            inner join raw_data.pdiagnose t2
            on t1.siteid = t2.siteid
            and t1.id = t2.backgroundid
            group by (t1.siteid, t2.backgroundid)
            '''
            ).format(
            Literal(race)
            )
            data = pgIO.getAllData(query)
            print("data is " + str(len(data)) + " items long")
            if len(data) > 0:
                # print("data is not none")
                for d in data:
                    filewriter.writerow([d[0], d[1]])
    f.close()
    return

@lD.log(logBase + '.relabelVar')
def relabelVar(logger):
    '''Relabels column values

    This function relabels Race and Settings values to standardised values.

    Decorators:
        lD.log

    Arguments:
        logger {logging.Logger} -- logs error information
    '''
    try:
        # relabel_sex_success = []
        # for sex in table1_config["inputs"]["sexes"]:
        #     sex_query = SQL('''
        #     UPDATE tejas.raceAgeT1A
        #     SET sex = {}
        #     WHERE sex in {}
        #     ''').format(
        #         Literal(sex),
        #         Literal(tuple(table1_config["params"]["sexes"][sex]))
        #         )
        #     relabel_sex_success.append(pgIO.commitData(sex_query))
        # if False in relabel_sex_success:
        #     print("Relabelling sex not successful!")

        relabel_race_success = []
        for race in table1_config["inputs"]["races"]:
            race_query = SQL('''
            UPDATE tejas.raceAgeT1A
            SET race = {}
            WHERE race in {}
            ''').format(
                Literal(race),
                Literal(tuple(table1_config["params"]["races"][race]))
                )
            relabel_race_success.append(pgIO.commitData(race_query))
        if False in relabel_race_success:
            print("Relabelling race not successful!")

        relabel_setting_success = []
        for setting in table1_config["inputs"]["settings"]:
            setting_query = SQL('''
            UPDATE tejas.raceAgeT1A
            SET visit_type = {}
            WHERE visit_type in {}
            ''').format(
                Literal(setting),
                Literal(tuple(table1_config["params"]["settings"][setting]))
                )
            relabel_setting_success.append(pgIO.commitData(setting_query))
        if False in relabel_setting_success:
            print("Relabelling setting not successful!")

    except Exception as e:
        logger.error('Failed to update table raceAgeT1A because {}'.format(e))
