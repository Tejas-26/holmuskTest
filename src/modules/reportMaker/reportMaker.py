from logs import logDecorator as lD
import jsonref
from modules.reportMaker import writeT1
from modules.table1 import comFunctions as cfT1
from modules.table2 import comFunctions as cfT2
from modules.table3 import comFunctions as cfT3
from modules.reportMaker import plotF1
from modules.reportMaker import writeT2
from modules.reportMaker import writeT3
# from modules.reportMaker import writeT4
# from modules.reportMaker import writeAppendix

config = jsonref.load(open('../config/config.json'))
logBase = config['logging']['logBase'] + '.modules.reportMaker.reportMaker'

@lD.log(logBase + '.main')
def main(logger, resultDict):
    '''
    main function for module1

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
    print('Main function of reportMaker module')
    print('='*30)
    # Table 1 Report creation
    # writeT1.genIntro()
    # raceAgeDict = cfT1.countRaceAge()
    # writeT1.genRaceAge(raceAgeDict)
    # #race vs sex (M/F) splits
    # raceSexDict = cfT1.countRaceSex()
    # writeT1.genRaceSex(raceSexDict)
    # #race vs patient_type splits (in/out)
    # raceSettingDict = cfT1.countRaceSetting()
    # writeT1.genRaceSetting(raceSettingDict)
    #
    # # Table 2 Info
    # writeT2.genIntro()
    # table2_dict1 = jsonref.load(open("../data/final/allAgesGeneralSUD.json"))
    # table2_dict2 = jsonref.load(open("../data/final/allAgesCategorisedSUD.json"))
    # table2_dict3 = jsonref.load(open("../data/final/ageBinnedGeneralSUD.json"))
    #
    # writeT2.genAllAgesOverallSUD(table2_dict1)
    # writeT2.genAllAgesCategorySUD(table2_dict2, table2_dict1)
    # writeT2.genAllAgesBinnedSUD(table2_dict3, table2_dict1)
    # table2_dict4AA = jsonref.load(open("../data/final/ageAABinnedCategorisedSUD.json"))
    # writeT2.genBC(table2_dict4AA, "Asian American")
    # table2_dict4NHPI = jsonref.load(open("../data/final/ageNHPIBinnedCategorisedSUD.json"))
    # writeT2.genBC(table2_dict4NHPI, "Native Hawaiian")
    # table2_dict4MR = jsonref.load(open("../data/final/ageMRBinnedCategorisedSUD.json"))
    # writeT2.genBC(table2_dict4MR, "Multi-ethnic")

    # Table 3 Report Creation
    #writeT3.genIntro()
    r = cfT3.createDF_allRaces_anySUD()
    print('Getting out of reportMaker module')
    print('-'*30)
    return
