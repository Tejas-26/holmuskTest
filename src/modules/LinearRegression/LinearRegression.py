from logs import logDecorator as lD
import jsonref, pprint
from modules.LinearRegression import readData as rD
from sklearn import linear_model as lm
from matplotlib import pyplot as plt

config = jsonref.load(open('../config/config.json'))
logBase = config['logging']['logBase'] + '.modules.LinearRegression.LinearRegression'


@lD.log(logBase + '.performLR')
def performLR(logger):
    '''print a line

    This function simply prints a single line

    Parameters
    ----------
    logger : {logging.Logger}
        The logger used for logging error information
    '''

    print('We are in LinearRegression')

    fileName = '../data/aircondit7.csv'
    X = rD.readCSV_X(fileName)
    Y = rD.readCSV_Y(fileName)
    lr = lm.LinearRegression()
    lr.fit(X,Y)
    ypred = lr.predict(X)
    plt.scatter(X,Y)
    plt.plot(X, ypred, color='red')
    plt.show()
    return

@lD.log(logBase + '.main')
def main(logger, resultsDict):
    '''main function for LinearRegression

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
    print('Main function of LinearRegression')
    print('='*30)
    print('We get a copy of the result dictionary over here ...')
    pprint.pprint(resultsDict)

    performLR()

    print('Getting out of LinearRegression')
    print('-'*30)

    return
