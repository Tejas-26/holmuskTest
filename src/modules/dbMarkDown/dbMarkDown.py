from logs import logDecorator as lD
import jsonref, pprint
from tqdm import tqdm
import operator
from psycopg2.sql import SQL, Identifier, Literal
from lib.databaseIO import pgIO
from modules.dbMarkDown import reportMaker as rM

dbMarkDownConf = jsonref.load(open('../config/modules/dbMarkDown.json'))
config = jsonref.load(open('../config/config.json'))
logBase = config['logging']['logBase'] + '.modules.dbMarkDown.dbMarkDown'

@lD.log(logBase + '.getUserBackground')
def getUserBackground(logger, user):
    data = []
    try:
        query = '''
        SELECT
        distinct race,
        count(race)
        FROM
            raw_data.background
        WHERE
            race ~ '[^0-9]+$'

        GROUP BY
        race
        '''
        data = pgIO.getAllData(query, user)
    except Exception as e:
        logger.error(f'Unable to get data for user: {user}: {e}')
    return data

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
def topValuesCol(logger, d):
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
            topDataCol = dataCol[:dbMarkDownConf["params"]["top_number"]]
            for pair in topDataCol:
                category = pair[0]
                value = pair[1]
                topVals[column][category] = value
    except Exception as e:
        logger.error(f'Unable to get data for the column names: {e}')

    return topVals


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
    print('Main function of dbMarkDown module')
    print('='*30)
    tableInfo = {
        'totalNumUser': None,
        'totalNumColumns': None,
        'columnNames': None,
    }

    tableInfo['columnNames'] = colNames()
    tableInfo['totalNumUser'] = numRowsCol()[0]
    tableInfo['totalNumColumns'] = numRowsCol()[1]

    colsInfo = jsonref.load(open('../config/columns.json'))
    #topValuesColumns = {key: None for key in tableInfo['columnNames']}
    # topValuesColumns = topValuesCol(colsInfo, topValuesColumns)
    topValuesColumns = topValuesCol(colsInfo)
    rM.makeIntro(tableInfo)
    rM.makeCols(tableInfo)
    rM.makeTop(topValuesColumns)
    with open('../data/final/finalData.json', 'w') as outfile:
        jsonref.dump(topValuesColumns, outfile)
    print('Getting out of dbMarkDown module')
    print('-'*30)

    return
