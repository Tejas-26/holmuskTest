
from logs import logDecorator as lD
import jsonref, pprint

import json

from modules.reportMaker import writeT1
from modules.table1 import comFunctions as cf
from modules.reportMaker import plotF1
# from modules.reportMaker import writeT2
# from modules.reportMaker import writeT3
# from modules.reportMaker import writeT4
# from modules.reportMaker import writeAppendix

config = jsonref.load(open('../config/config.json'))
logBase = config['logging']['logBase'] + '.modules.reportMaker.reportMaker'
raceAgeDict = {"AA":[],"NHPI":[],"MR":[]}
raceSexDict = {"AA":[],"NHPI":[],"MR":[]}
raceSettingDict = {"AA":[],"NHPI":[],"MR":[]}

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
    cf.cleanUp()
    # Table 1 Report creation
    writeT1.genIntro()
    #main race information
    mainRaceDict = cf.countMainRace()
    writeT1.genRace(mainRaceDict)
    #gathering data for the following three subsections of report
    cf.createrace_t1()
    cf.createrestofusers()
    cf.popDiagCols()
    cf.delAllFalserestofusers()
    #race vs age groups
    raceAgeDict = cf.countRaceAge()
    writeT1.genRaceAge(raceAgeDict)
    #race vs sex (M/F) splits
    raceSexDict = cf.countRaceSex()
    writeT1.genRaceSex(raceSexDict)
    #race vs patient_type splits (in/out)
    raceSettingDict = cf.countRaceSetting()
    writeT1.genRaceSetting(raceSettingDict)


    # Figure 1 Info - use later
    '''
    with open("../data/final/diagnosesCount.json") as json_file:
        fig1Dict = json.load(json_file)
    plotFig1.genIntro()
    plotFig1.genFig(fig1Dict)
    '''
    '''
    # Table 2 Info
    with open("../data/final/allAgesGeneralSUD.json") as json_file:
        table2_dict1 = json.load(json_file)
    with open("../data/final/allAgesCategorisedSUD.json") as json_file:
        table2_dict2 = json.load(json_file)
    with open("../data/final/ageBinnedGeneralSUD.json") as json_file:
        table2_dict3 = json.load(json_file)
    with open("../data/final/ageBinnedCategorisedSUD.json") as json_file:
        table2_dict4 = json.load(json_file)
    writeTable2.genIntro()
    writeTable2.genTotalPrev(table2_dict1,table2_dict2,table1Dict)
    writeTable2.genAAAgeBinnedPrev(table2_dict3,table2_dict4,table1Dict)
    writeTable2.genNHPIAgeBinnedPrev(table2_dict3,table2_dict4,table1Dict)
    writeTable2.genMRAgeBinnedPrev(table2_dict3,table2_dict4,table1Dict)

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
