from logs import logDecorator as lD
import jsonref, pprint
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import csv

from psycopg2.sql import SQL, Identifier, Literal
from lib.databaseIO import pgIO
from collections import Counter
from textwrap import wrap

import statsmodels.api as sm

from tqdm import tqdm
from multiprocessing import Pool

config = jsonref.load(open('../config/config.json'))
table3_config = jsonref.load(open('../config/modules/tejasT3.json'))
logBase = config['logging']['logBase'] + '.modules.table3.comFunctions'

all_userkeys = "../data/raw_data/allUserKeys.csv"

@lD.log(logBase + '.logRegress')
def logRegress(logger, df):
    '''Performs logistic regression

    This function gets the logistic regression coefficients for a dataframe that is passed in.

    Decorators:
        lD.log

    Arguments:
        logger {logging.Logger} -- logs error information
        df {dataframe} -- input dataframe where first column is 'sud'
    '''

    try:

        print("Performing Logistic Regression...")

        train_cols = df.columns[1:]
        logit = sm.Logit(df['sud'].astype(float), df[train_cols].astype(float))
        result = logit.fit()

        # Get odds, which are assessed by coeff[race/agebin/sex/setting]
        params = result.params
        conf = result.conf_int()
        conf['OR'] = params

        conf.columns = ['2.5%', '97.5%', 'OR']
        CI_OR_df = np.exp(conf)
        resultsDF = CI_OR_df[['OR']].join(CI_OR_df.ix[:,:'97.5%'])

    except Exception as e:
        logger.error('logRegress failed because of {}'.format(e))

    return resultsDF

@lD.log(logBase + '.addmorethan2sudcolumn')
def addmorethan2sudcolumn(logger):
    '''Populates the 'morethan2sud' column in tejas.sud_race_age

    This function counts the number of 'True' for each mental disorder
    for each user in tejas.sud_race_age. If they have more than 1 'True' value,
    their 'morethan2sud' column will be set to 'True'.

    Decorators:
        lD.log

    Arguments:
        logger {logging.Logger} -- logs error information
    '''
    try:
        query = '''
        SELECT
            siteid, backgroundid, alc, cannabis, amphe, halluc, nicotin,
            cocaine, opioids, sedate, others, polysub, inhalant
        FROM tejas.sud_race_age
        '''
        data = pgIO.getAllData(query)

        csvfile = '../data/raw_data/atleast2suduser_keys.csv'

        count = 0
        output = open(csvfile, 'w+')
        csv_output = csv.writer(output)
        for row in data:
            if sum(list(row[6:17])) >=2:
                csv_output.writerow(row)
        readCSV = csv.reader(open(csvfile), delimiter=",")
        for user in tqdm(readCSV):
            updateQuery = SQL('''
            UPDATE tejas.sud_race_age
            SET morethan2sud = true
            WHERE siteid = {}
            AND backgroundid = {}
            ''').format(Literal(user[0]), Literal(str(user[1])))
            value = pgIO.commitData(updateQuery)
            # print(type(user[0]))

        #Update column's null values to false
        updateQuery2 = '''
        UPDATE tejas.sud_race_age
        SET morethan2sud = false
        WHERE morethan2sud is null
        '''
        print(pgIO.commitData(updateQuery2))


    except Exception as e:
        logger.error('adding morethan2sud column to the database failed because of {}'.format(e))

    return

@lD.log(logBase + '.createrestofusersT3part1')
def createrestofusersT3part1(logger):
    try:
        query = '''
        CREATE TABLE tejas.restofusers_t3_p1(
            sud bool,
            race text,
            age text,
            sex text,
            visit_type text
        )'''
        value = pgIO.commitData(query)
        if value == True:
            print("tejas.restofusers_t3 table has been successfully created")
    except Exception as e:
        logger.error('Unable to create table restofusers because {}'.format(e))
    return

@lD.log(logBase + '.poprestofusersT3part1')
def poprestofusersT3part1(logger):
    getQuery = '''
    INSERT INTO tejas.restofusers_t3_p1(sud, race, age, sex, visit_type)
    SELECT t2.sud, t1.race, t1.age, t1.sex, t1.visit_type
    FROM tejas.race_age_t1new t1
    INNER JOIN tejas.restofusers t2
    ON t1.siteid = t2.siteid
    AND t1.backgroundid = t2.backgroundid
    WHERE (cast (age as int) >= 12)
    AND (cast (age as int) <= 100)
    GROUP BY (t1.siteid, t1.backgroundid, t2.sud, t1.race, t1.age, t1.sex,
    t1.visit_type)
    '''
    value = pgIO.commitData(getQuery)
    if value == True:
        print("successfully populated tejas.restofusers_t3")
    return

@lD.log(logBase + '.createDF_allRaces_anySUD')
def createDF_allRaces_anySUD(logger):
    '''Creates dataframe for total sample, dependent variable = any sud

    This function creates a dataframe for the total sample, where the
    dependent variable is any sud and the independent variables are:
    race, age, sex and setting.

    Decorators:
        lD.log

    Arguments:
        logger {logging.Logger} -- logs error information
    '''

    try:

        query = '''
        SELECT * from tejas.restofusers_t3_p1
        '''
        data = pgIO.getAllData(query)
        sud_data = [d[0] for d in data]
        race_data = [d[1] for d in data]
        age_data = [d[2] for d in data]
        sex_data = [d[3] for d in data]
        setting_data = [d[4] for d in data]

        d = {'sud': sud_data, 'race': race_data, 'age': age_data, 'sex': sex_data, 'setting': setting_data}
        main = pd.DataFrame(data=d)
        df = main.copy()

        # Change sud column to binary, dummify the other columns
        df.replace({False:0, True:1}, inplace=True)

        dummy_races = pd.get_dummies(main['race'])
        df = df[['sud']].join(dummy_races.ix[:, 'MR':])
        main.replace(to_replace=list(range(12, 18)), value="12-17", inplace=True)
        main.replace(to_replace=list(range(18, 35)), value="18-34", inplace=True)
        main.replace(to_replace=list(range(35, 50)), value="35-49", inplace=True)
        main.replace(to_replace=list(range(50, 100)), value="50+", inplace=True)
        dummy_ages = pd.get_dummies(main['age'])
        df = df[['sud', 'MR', 'NHPI']].join(dummy_ages.ix[:, :'50+'])
        dummy_sexes = pd.get_dummies(main['sex'])
        df = df[['sud', 'AA', 'MR', 'NHPI', '12-17', '18-34', '35-49', '50+']].join(dummy_sexes.ix[:, 'M':])
        # dummy_setting = pd.get_dummies(main['setting'])
        # df = df[['sud', 'MR', 'NHPI', '12-17', '18-34', '35-49', 'M']].join(dummy_setting.ix[:, :'Hospital'])
        #
        # df['intercept'] = 1.0

    except Exception as e:
        logger.error('createDF_allRaces_anySUD failed because of {}'.format(e))
    return df

@lD.log(logBase + '.createDF_allRaces_morethan2SUD')
def createDF_allRaces_morethan2SUD(logger):
    '''Creates dataframe for total sample, dependent variable = at least 2 sud

    This function creates a dataframe for the total sample, where the
    dependent variable is >=2 sud and the independent variables are:
    race, age, sex and setting.

    Decorators:
        lD.log

    Arguments:
        logger {logging.Logger} -- logs error information
    '''

    try:

        query = '''
        SELECT morethan2sud, race, age, sex, visit_type
        FROM tejas.sud_race_age
        WHERE (cast (age as int) >= 12)
        AND (cast (age as int) <= 100)
        '''

        data = pgIO.getAllData(query)
        sud_data = [d[0] for d in data]
        race_data = [d[1] for d in data]
        age_data = [d[2] for d in data]
        sex_data = [d[3] for d in data]
        setting_data = [d[4] for d in data]

        d = {'sud': sud_data, 'race': race_data, 'age': age_data, 'sex': sex_data, 'setting': setting_data}
        main = pd.DataFrame(data=d)
        df = main.copy()

        # Change sud column to binary, dummify the other columns
        df.replace({False:0, True:1}, inplace=True)

        dummy_races = pd.get_dummies(main['race'])
        df = df[['sud']].join(dummy_races.ix[:, 'MR':])

        main.replace(to_replace=list(range(12, 18)), value="12-17", inplace=True)
        main.replace(to_replace=list(range(18, 35)), value="18-34", inplace=True)
        main.replace(to_replace=list(range(35, 50)), value="35-49", inplace=True)
        main.replace(to_replace=list(range(50, 100)), value="50+", inplace=True)
        dummy_ages = pd.get_dummies(main['age'])
        df = df.reindex(columns = list(df) + ['MR','NHPI'])
        df = df[['sud', 'MR', 'NHPI']].join(dummy_ages.ix[:, :'35-49'])

        dummy_sexes = pd.get_dummies(main['sex'])
        df = df.reindex(columns = list(df) + ['12-17', '18-34', '35-49'])
        df = df[['sud', 'MR', 'NHPI', '12-17', '18-34', '35-49']].join(dummy_sexes.ix[:, 'M':])

        dummy_setting = pd.get_dummies(main['setting'])
        df = df.reindex(columns = list(df) + ['M'])
        df = df[['sud', 'MR', 'NHPI', '12-17', '18-34', '35-49', 'M']].join(dummy_setting.ix[:, :'Hospital'])

        df['intercept'] = 1.0

    except Exception as e:
        logger.error('createDF_allRaces_morethan2SUD failed because of {}'.format(e))

    return df

@lD.log(logBase + '.createDF_byRace_anySUD')
def createDF_byRace_anySUD(logger, race):
    '''Creates dataframe for a sample from a specified race,
    dependent variable = any sud

    This function creates a dataframe for a sample from a specified race,
    where the  dependent variable is any sud and the independent variables
    are: age, sex and setting.

    Decorators:
        lD.log

    Arguments:
        logger {logging.Logger} -- logs error information
        race {str} -- 'AA', 'NHPI', or 'MR'
    '''

    try:

        query = SQL('''
        SELECT
            t2.sud, t1.age, t1.sex, t1.visit_type
        FROM
            tejas.sud_race_age t1
        INNER JOIN
            tejas.restofusers t2
        ON
            t1.siteid = t2.siteid
        AND
            t1.backgroundid = t2.backgroundid
        WHERE
            t1.race = {}
        AND
            (cast (age as int) >= 12)
        AND
            (cast (age as int) <= 100)
        GROUP BY
            (t1.siteid, t1.backgroundid, t2.sud, t1.age, t1.sex,
            t1.visit_type)
        ''').format(
            Literal(race)
        )

        data = pgIO.getAllData(query)
        sud_data = [d[0] for d in data]
        age_data = [d[1] for d in data]
        sex_data = [d[2] for d in data]
        setting_data = [d[3] for d in data]

        d = {'sud': sud_data, 'age': age_data, 'sex': sex_data, 'setting': setting_data}
        main = pd.DataFrame(data=d)
        df = main.copy()

        # Change sud column to binary, dummify the other columns
        df.replace({False:0, True:1}, inplace=True)

        main.replace(to_replace=list(range(12, 18)), value="12-17", inplace=True)
        main.replace(to_replace=list(range(18, 35)), value="18-34", inplace=True)
        main.replace(to_replace=list(range(35, 50)), value="35-49", inplace=True)
        main.replace(to_replace=list(range(50, 100)), value="50+", inplace=True)
        dummy_ages = pd.get_dummies(main['age'])
        df = df.reindex(columns = list(df) + ['MR','NHPI'])
        df = df[['sud']].join(dummy_ages.ix[:, :'35-49'])

        dummy_sexes = pd.get_dummies(main['sex'])
        df = df.reindex(columns = list(df) + ['12-17', '18-34', '35-49'])
        df = df[['sud', '12-17', '18-34', '35-49']].join(dummy_sexes.ix[:, 'M':])

        dummy_setting = pd.get_dummies(main['setting'])
        df = df.reindex(columns = list(df) + ['M'])
        df = df[['sud', '12-17', '18-34', '35-49', 'M']].join(dummy_setting.ix[:, :'Hospital'])

        df['intercept'] = 1.0

    except Exception as e:
        logger.error('createDF_byRace_anySUD failed because of {}'.format(e))

    return df

@lD.log(logBase + '.createDF_byRace_morethan2SUD')
def createDF_byRace_morethan2SUD(logger, race):
    '''Creates dataframe for a sample from a specified race,
    dependent variable = at least 2 sud

    This function creates a dataframe for a sample from a specified race,
    where the  dependent variable is >=2 sud and the independent variables
    are: age, sex and setting.

    Decorators:
        lD.log

    Arguments:
        logger {logging.Logger} -- logs error information
        race {str} -- 'AA', 'NHPI', or 'MR'
    '''

    try:

        query = SQL('''
        SELECT morethan2sud,age,sex,visit_type
        FROM tejas.sud_race_age
        WHERE (cast (age as int) >= 12)
        AND (cast (age as int) <= 100)
        AND race = {}
        ''').format(
            Literal(race)
        )

        data = pgIO.getAllData(query)
        sud_data = [d[0] for d in data]
        age_data = [d[1] for d in data]
        sex_data = [d[2] for d in data]
        setting_data = [d[3] for d in data]

        d = {'sud': sud_data, 'age': age_data, 'sex': sex_data, 'setting': setting_data}
        main = pd.DataFrame(data=d)
        df = main.copy()

        # Change sud column to binary, dummify the other columns
        df.replace({False:0, True:1}, inplace=True)

        main.replace(to_replace=list(range(12, 18)), value="12-17", inplace=True)
        main.replace(to_replace=list(range(18, 35)), value="18-34", inplace=True)
        main.replace(to_replace=list(range(35, 50)), value="35-49", inplace=True)
        main.replace(to_replace=list(range(50, 100)), value="50+", inplace=True)
        dummy_ages = pd.get_dummies(main['age'])
        df = df.reindex(columns = list(df) + ['MR','NHPI'])
        df = df[['sud']].join(dummy_ages.ix[:, :'35-49'])

        dummy_sexes = pd.get_dummies(main['sex'])
        df = df.reindex(columns = list(df) + ['12-17', '18-34', '35-49'])
        df = df[['sud', '12-17', '18-34', '35-49']].join(dummy_sexes.ix[:, 'M':])

        dummy_setting = pd.get_dummies(main['setting'])
        df = df.reindex(columns = list(df) + ['M'])
        df = df[['sud', '12-17', '18-34', '35-49', 'M']].join(dummy_setting.ix[:, :'Hospital'])

        df['intercept'] = 1.0

    except Exception as e:
        logger.error('createDF_byRace_morethan2SUD failed because of {}'.format(e))

    return df
