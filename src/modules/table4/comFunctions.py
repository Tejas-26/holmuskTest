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
table4_config = jsonref.load(open('../config/modules/tejasT4.json'))
logBase = config['logging']['logBase'] + '.modules.table4.comFunctions'

@lD.log(logBase + '.logRegress')
def logRegress(logger, df):
    '''Performs logistic regression for any sud

    This function returns the logistic regression coefficients for a dataframe that is
    passed in, and also returns the mental disorders that are dropped due to a small
    sample.
    Decorators:
        lD.log

    Arguments:
        logger {logging.Logger} -- logs error information
        df {dataframe} -- input dataframe with the first column as 'sud'
    '''

    try:

        print("Performing Logistic Regression...")

        # Drop columns who do not meet the minimum percentage of people diagnosed with the disorder
        row_count = df.shape[0]
        columns_to_drop = []
        copy_of_df = df.copy()

        for column, count in copy_of_df.apply(lambda column: (column == 1).sum()).iteritems():
            if count/row_count <= 0.005:
                columns_to_drop.append(column)
        print("These columns are dropped: " + str(columns_to_drop))
        df.drop(columns_to_drop, axis=1, inplace=True)

        train_cols = df.columns[1:]
        logit = sm.Logit(df['sud'], df[train_cols])
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

    return resultsDF, columns_to_drop

@lD.log(logBase + '.createDF_byRace_anySUD')
def createDF_byRace_anySUD(logger, race):
    '''Creates a dataframe with comorbid mental disorder diagnoses with SUD

    This function creates a dataframe of users from each race, with the first
    column being having any sud, the dependent variable, and the rest of the
    columns being the independent variables of the other mental disorder diagnoses.

    Decorators:
        lD.log

    Arguments:
        logger {logging.Logger} -- logs error information
        race {str} -- 'AA', 'NHPI', or 'MR'
    '''

    try:

        query = SQL('''
        SELECT
            t2.sud,
            t2.mood,
            t2.anxiety,
            t2.adjustment,
            t2.adhd,
            t2.psyc,
            t2.pers,
            t2.childhood,
            t2.impulse,
            t2.cognitive,
            t2.eating,
            t2.smtf,
            t2.disso,
            t2.sleep,
            t2.fd
        FROM
            tejas.race_age_t1new t1
        INNER JOIN
            tejas.restofusers t2
        ON
            t1.siteid = t2.siteid
        AND
            t1.backgroundid = t2.backgroundid
        WHERE
            t1.age BETWEEN 12 AND 100
        AND
            t1.race = {}
        ''').format(
            Literal(race)
        )
        data = pgIO.getAllData(query)
        sud_data = [d[0] for d in data]
        mood_data = [d[1] for d in data]
        anxiety_data = [d[2] for d in data]
        adjustment_data = [d[3] for d in data]
        adhd_data = [d[4] for d in data]
        psyc_data = [d[5] for d in data]
        pers_data = [d[6] for d in data]
        childhood_data = [d[7] for d in data]
        impulse_data = [d[8] for d in data]
        cognitive_data = [d[9] for d in data]
        eating_data = [d[10] for d in data]
        smtf_data = [d[11] for d in data]
        disso_data = [d[12] for d in data]
        sleep_data = [d[13] for d in data]
        fd_data = [d[14] for d in data]

        d = {'sud': sud_data, 'mood': mood_data, 'anxiety': anxiety_data, 'adjustment': adjustment_data, 'adhd': adhd_data, 'psyc': psyc_data, 'pers': pers_data, 'childhood': childhood_data, 'impulse': impulse_data, 'cognitive': cognitive_data, 'eating': eating_data, 'smtf': smtf_data, 'disso': disso_data, 'sleep': sleep_data, 'fd': fd_data}
        df = pd.DataFrame(data=d)

        # Change all columns to binary
        df.replace({False:0, True:1}, inplace=True)

        df['intercept'] = 1.0

    except Exception as e:
        logger.error('createDF_byRace_anySUD failed because of {}'.format(e))

    return df

@lD.log(logBase + '.allTheOtherStuff')
def allTheOtherStuff(logger):
    aa_results, _ = logRegress(createDF_byRace_anySUD("AA"))
    aa_results = aa_results.to_dict()
    nh_results, _ = logRegress(createDF_byRace_anySUD("NHPI"))
    nh_results = nh_results.to_dict()
    mr_results, _ = logRegress(createDF_byRace_anySUD("MR"))
    mr_results = mr_results.to_dict()
    return aa_results, nh_results, mr_results
