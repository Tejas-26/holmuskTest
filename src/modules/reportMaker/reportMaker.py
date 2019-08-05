from logs import logDecorator as lD
import jsonref, pprint, json

from modules.reportMaker import writeT1
from modules.table1 import comFunctions as cfT1
from modules.table2 import comFunctions as cfT2
from modules.reportMaker import plotF1
from modules.reportMaker import writeT2
# from modules.reportMaker import writeT3
# from modules.reportMaker import writeT4
# from modules.reportMaker import writeAppendix

config = jsonref.load(open('../config/config.json'))
logBase = config['logging']['logBase'] + '.modules.reportMaker.reportMaker'

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
    print('Main function of reportMaker module')
    print('='*30)
    # mainRaceDict = cfT1.countMainRace()
    '''
    cfT1.cleanUp()
    # Table 1 Report creation
    writeT1.genIntro()
    #main race information

    writeT1.genRace(mainRaceDict)
    #gathering data for the following three subsections of report
    cfT1.createrace_t1()
    cfT1.createrestofusers()
    cfT1.popDiagCols()
    cfT1.delAllFalserestofusers()


    #race vs age groups
    raceAgeDict = cfT1.countRaceAge()
    writeT1.genRaceAge(raceAgeDict)
    #race vs sex (M/F) splits
    raceSexDict = cfT1.countRaceSex()
    writeT1.genRaceSex(raceSexDict)
    #race vs patient_type splits (in/out)
    raceSettingDict = cfT1.countRaceSetting()
    writeT1.genRaceSetting(raceSettingDict)
    '''

    # Table 2 Info
    # table2_dict1 = jsonref.load(open("../data/final/allAgesGeneralSUD.json"))
    # print("All ages general SUD: " + str(table2_dict1))
    # table2_dict2 = jsonref.load(open("../data/final/allAgesCategorisedSUD.json"))
    # print("All ages categorised SUD: " + str(table2_dict2))
    # writeT2.genTotalPrev(table2_dict1,table2_dict2,mainRaceDict)
    writeT2.genIntro()
    #result = cfT2.ageBinnedCategorisedSUD()
    #print(result)
    table2_dict1 = jsonref.load(open("../data/final/allAgesGeneralSUD.json"))
    table2_dict2 = jsonref.load(open("../data/final/allAgesCategorisedSUD.json"))
    table2_dict3 = jsonref.load(open("../data/final/ageBinnedGeneralSUD.json"))
    writeT2.genAllAgesOverallSUD(table2_dict1)
    writeT2.genAllAgesCategorySUD(table2_dict2, table2_dict1)
    writeT2.genAllAgesBinnedSUD(table2_dict3, table2_dict1)
    '''
    with open("../data/final/ageBinnedCategorisedSUD.json") as json_file:
        table2_dict4 = json.load(json_file)

    # Table 3 Info
    with open("../data/final/oddsratios_allRaces.json") as json_file:
        table3_dict1 = json.load(json_file)
    with open("../data/final/oddsratios_anysud_byRace.json") as json_file:
        table3_dict2 = json.load(json_file)
    with open("../data/final/oddsratios_morethan2sud_byRace.json") as json_file:
        table3_dict3 = json.load(json_file)
    writeTable3.genIntro()
    writeTable3.oddsRatiosAllRaces(table3_dict1,table1Dict)
    writeTable3.oddsRatiosByRace(table3_dict2, table3_dict3, table1Dict)

    # Table 4 Info
    with open("../data/final/table4data.json") as json_file:
        table4Dict = json.load(json_file)
    writeTable4.genIntro()
    writeTable4.oddsRatiosByRace(table4Dict, table1Dict)
    '''

    # Appendix (What databases are used)
    # writeAppendix.genAppendix()

    print('Getting out of reportMaker module')
    print('-'*30)

    return
