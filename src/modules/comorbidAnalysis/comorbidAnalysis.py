from logs import logDecorator as lD
import jsonref, pprint
from tqdm import tqdm
import operator
from psycopg2.sql import SQL, Identifier, Literal
from lib.databaseIO import pgIO
from modules.comorbidAnalysis import reportMaker as rM
from modules.comorbidAnalysis import comFunctions as cF

comorbidAnalysisConf = jsonref.load(open('../config/modules/comorbid/comorbid_table1/comorbidAnalysis.json'))
config = jsonref.load(open('../config/config.json'))
logBase = config['logging']['logBase'] + '.modules.comorbid.comorbidAnalysis.comorbid_table1.comorbidAnalysis'


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
    tableInfo = {
        'totalNumUser': None,
        'totalNumColumns': None,
        'columnNames': None,
    }

    tableInfo['columnNames'] = cF.colNames()
    tableInfo['totalNumUser'] = cF.numRowsCol()[0]
    tableInfo['totalNumColumns'] = cF.numRowsCol()[1]

    colsInfoBackground = jsonref.load(open('../config/columns.json'))
    #topValuesColumns = {key: None for key in tableInfo['columnNames']}
    # topValuesColumns = topValuesCol(colsInfo, topValuesColumns)
    # tvcBackground = cF.topValuesColBackground(colsInfo)
    # rM.makeIntro(tableInfo)
    # rM.makeCols(tableInfo)
    # rM.makeTop(topValuesColumns)
    # with open('../data/final/finalData.json', 'w') as outfile:
    #     jsonref.dump(topValuesColumns, outfile)
    print('Getting out of comorbidAnalysis module')
    print('-'*30)

    return
