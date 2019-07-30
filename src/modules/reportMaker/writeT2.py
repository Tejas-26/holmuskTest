from logs import logDecorator as lD
import jsonref, pprint
import statistics as stats
from psycopg2.sql import SQL, Identifier, Literal
from lib.databaseIO import pgIO
from collections import Counter

from tqdm import tqdm
from multiprocessing import Pool
from time import sleep

config = jsonref.load(open('../config/config.json'))
logBase = config['logging']['logBase'] + '.modules.writeT1.writeT1'

@lD.log(logBase + '.genIntro')
def genIntro(logger):

    report = f'''
## Description of Table 2:
This table measures the prevalance of the below among the 3 races, as a percentage of the total or of the various ages:
Any SUD - any_sud
\>=2 SUD - morethan2_sud
Alcohol - alc
Cannabis - cannabis
Amphetamine - amphe
Hallucinogen - halluc
Nicotine - nicotin
Cocaine - cocaine
Opioids - opioids
Sedatives/tranquilizers - sedate
Other drugs - others
Polysubstance - polysub
Inhalants - inhalant
        '''
    with open('../report/paper1Report.md', 'a+') as f:
        f.write( report )

    return

@lD.log(logBase + '.genTotalPrev')
def genTotalPrev(logger, r1, r2, r3):

    report = f'''
### Total Sample
|Prevalence, %       |AA          |NHPI        |MR          |
|--------------------|------------|------------|------------|
|**DSM-IV diagnosis**|**Total = {r3['AA'][0]}**|**Total = {r3['NHPI'][0]}**|**Total = {r3['MR'][0]}**|'''

    for row in r1:
        report = report + f'''
|{row}               |{r1[row][0]}|{r1[row][1]}|{r1[row][2]}|'''

    for row in r2:
        report = report + f'''
|{row}               |{r2[row][0]}|{r2[row][1]}|{r2[row][2]}|'''

    report = report + '''
***'''

    with open('../report/paper1Report.md', 'a+') as f:
        f.write( report )

    return

@lD.log(logBase + '.genAAAgeBinnedPrev')
def genAAAgeBinnedPrev(logger, r1, r2, r3):

    report = f'''
### Asian Americans, separated into age bins
|Prevalence, %       |1-11 y/o            |12-17 y/o           |18-34 y/o           |35-49 y/o           |50+ y/o             |
|--------------------|--------------------|--------------------|--------------------|--------------------|--------------------|
|**DSM-IV diagnosis**|**Total: {r3['AA'][1][0]}**|**Total: {r3['AA'][1][1]}**|**Total: {r3['AA'][1][2]}**|**Total: {r3['AA'][1][3]}**|**Total: {r3['AA'][1][4]}**|'''

    for row in r1:
        report = report + f'''
|{row}               |{r1[row][0][0]}|{r1[row][0][1]}|{r1[row][0][2]}|{r1[row][0][3]}|{r1[row][0][4]}|'''

    for row in r2:
        report = report + f'''
|{row}               |{r2[row][0][0]}|{r2[row][0][1]}|{r2[row][0][2]}|{r2[row][0][3]}|{r2[row][0][4]}|'''

    report = report + '''
***'''

    with open('../report/paper1Report.md', 'a+') as f:
        f.write( report )

    return

@lD.log(logBase + '.genNHPIAgeBinnedPrev')
def genNHPIAgeBinnedPrev(logger, r1, r2, r3):

    report = f'''
### Native Hawaiians/Pacific Islanders, separated into age bins
|Prevalence, %       |1-11 y/o            |12-17 y/o           |18-34 y/o           |35-49 y/o           |50+ y/o             |
|--------------------|--------------------|--------------------|--------------------|--------------------|--------------------|
|**DSM-IV diagnosis**|**Total: {r3['NHPI'][1][0]}**|**Total: {r3['NHPI'][1][1]}**|**Total: {r3['NHPI'][1][2]}**|**Total: {r3['NHPI'][1][3]}**|**Total: {r3['NHPI'][1][4]}**|'''

    for row in r1:
        report = report + f'''
|{row}               |{r1[row][1][0]}|{r1[row][1][1]}|{r1[row][1][2]}|{r1[row][1][3]}|{r1[row][1][4]}|'''

    for row in r2:
        report = report + f'''
|{row}               |{r2[row][1][0]}|{r2[row][1][1]}|{r2[row][1][2]}|{r2[row][1][3]}|{r2[row][1][4]}|'''

    report = report + '''
***'''

    with open('../report/paper1Report.md', 'a+') as f:
        f.write( report )

    return

@lD.log(logBase + '.genMRAgeBinnedPrev')
def genMRAgeBinnedPrev(logger, r1, r2, r3):

    report = f'''
### Mixed-Race, separated into age bins
|Prevalence, %       |1-11 y/o            |12-17 y/o           |18-34 y/o           |35-49 y/o           |50+ y/o             |
|--------------------|--------------------|--------------------|--------------------|--------------------|--------------------|
|**DSM-IV diagnosis**|**Total: {r3['MR'][1][0]}**|**Total: {r3['MR'][1][1]}**|**Total: {r3['MR'][1][2]}**|**Total: {r3['MR'][1][3]}**|**Total: {r3['MR'][1][4]}**|'''

    for row in r1:
        report = report + f'''
|{row}               |{r1[row][2][0]}|{r1[row][2][1]}|{r1[row][2][2]}|{r1[row][2][3]}|{r1[row][2][4]}|'''

    for row in r2:
        report = report + f'''
|{row}               |{r2[row][2][0]}|{r2[row][2][1]}|{r2[row][2][2]}|{r2[row][2][3]}|{r2[row][2][4]}|'''

    report = report + '''
***'''

    with open('../report/paper1Report.md', 'a+') as f:
        f.write( report )

    return
