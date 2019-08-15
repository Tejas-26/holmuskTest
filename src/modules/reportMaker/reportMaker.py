from logs import logDecorator as lD
import jsonref
from modules.reportMaker import writeT1
from modules.figure1 import comFunctions as cfF1
from modules.table1 import comFunctions as cfT1
from modules.table2 import comFunctions as cfT2
from modules.table3 import comFunctions as cfT3
from modules.table4 import comFunctions as cfT4
from modules.reportMaker import plotF1
from modules.reportMaker import writeT2
from modules.reportMaker import writeT3
from modules.reportMaker import writeT4
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
    writeT1.genIntro()
    mainRace = jsonref.load(open("../data/final/sampleCount.json"))
    writeT1.genRace(mainRace)
    raceAgeDict = cfT1.countRaceAge()
    writeT1.genRaceAge(raceAgeDict)
    #race vs sex (M/F) splits
    raceSexDict = cfT1.countRaceSex()
    writeT1.genRaceSex(raceSexDict)
    # race vs patient_type splits (in/out)
    raceSettingDict = cfT1.countRaceSetting()
    writeT1.genRaceSetting(raceSettingDict)

    # Figure 1 Info
    plotF1.genIntro()
    rd = cfF1.genDiagCount("../data/final/t3PatientCount.json")
    plotF1.genFig(rd)

    # Table 2 Info
    writeT2.genIntro()
    countRaceSUD = cfT2.countRaceSUDppl()
    allAgesGeneral = cfT2.allAgesGeneralSUD()
    table2_dict2 = cfT2.allAgesCategorisedSUD()
    table2_dict3 = cfT2.ageBinnedGeneralSUD()
    writeT2.genAllAgesOverallSUD(allAgesGeneral)
    writeT2.genAllAgesCategorySUD(table2_dict2, allAgesGeneral)
    writeT2.genAllAgesBinnedSUD(table2_dict3, allAgesGeneral)
    table2_dict4AA = cfT2.ageBinnedCategorisedSUD("AA")
    writeT2.genBC(table2_dict4AA, "Asian American")
    table2_dict4NHPI = cfT2.ageBinnedCategorisedSUD("NHPI")
    writeT2.genBC(table2_dict4NHPI, "Native Hawaiian")
    table2_dict4MR = cfT2.ageBinnedCategorisedSUD("MR")
    writeT2.genBC(table2_dict4MR, "Multi-ethnic")

    # Table 3 Report Creation
    writeT3.genIntro()
    t3Patients = jsonref.load(open("../data/final/t3PatientCount.json"))
    table3_dict1 = jsonref.load(open("../data/final/oddsratios_allRaces_anySUD.json"))
    table3_dict2 = jsonref.load(open("../data/final/oddsratios_allRaces_2SUDormore.json"))
    writeT3.oddsRatiosAllRaces(table3_dict1, table3_dict2, t3Patients)
    table3_dict3 = jsonref.load(open("../data/final/oddsratios_byRace.json"))
    writeT3.oddsRatiosByRace(table3_dict3, t3Patients)

    # Table 4 Report Creation
    aa, nh, mr = cfT4.allTheOtherStuff()
    writeT4.genIntro()
    writeT4.oddsRatiosByRace(aa, nh, mr, t3Patients)
    print('Getting out of reportMaker module')
    print('-'*30)
    return
