from logs import logDecorator as lD
import jsonref
import pprint
import statistics as stats
from psycopg2.sql import SQL, Identifier, Literal
from lib.databaseIO import pgIO
from collections import Counter

from tqdm import tqdm
from multiprocessing import Pool
from time import sleep

config = jsonref.load(open('../config/config.json'))
logBase = config['logging']['logBase'] + '.modules.reportMaker.writeT2'

@lD.log(logBase + '.genIntro')
def genIntro(logger):
    report = open('../data/comorbidMDTable2Intro.txt', 'r')
    report = report.read()
    with open('../report/paper1markdown.md', 'a+') as f:
        f.write( report )

    return

@lD.log(logBase + '.genAAAgeBinnedPrev')
def genAllAgesOverallSUD(logger, r):
    report = f'''
### Quantities of patients with SUDs, by race
|Quantity            |Asian Americans     |NHPI                |Multi-Ethnic        |
|--------------------|--------------------|--------------------|--------------------|
|**any SUD**|**Total:{r['any_sud'][0]}**|**Total:{r['any_sud'][1]}**|**Total:{r['any_sud'][2]}**|
|**at least 2 SUDs**|**Total:{r['morethan2_sud'][0]}**|**Total:{r['morethan2_sud'][1]}**|**Total:{r['morethan2_sud'][2]}**|
'''
    report = report + '''\n***'''
    with open('../report/paper1markdown.md', 'a+') as f:
        f.write( report )
    return

@lD.log(logBase + '.genPC')
def genPC(logger, n, d):
    if d == 0:
        return 0
    else:
        return round((n/d)*100, 1)

@lD.log(logBase + '.genNHPIAgeBinnedPrev')
def genAllAgesCategorySUD(logger, r1, r2):

    report = f'''
### Categorised percentages of SUD patients, via race
|Quantity, %         |Asian Americans     |NHPI                |Multi-Ethnic        |
|--------------------|--------------------|--------------------|--------------------|
'''

    for row in r1:
        report = report + f'''|{row}|{genPC(r1[row][0],r2['any_sud'][0])}|{genPC(r1[row][1],r2['any_sud'][1])}|{genPC(r1[row][2],r2['any_sud'][2])}|\n'''
    report = report + '''\n***'''

    with open('../report/paper1markdown.md', 'a+') as f:
        f.write( report )

    return

@lD.log(logBase + '.genMRAgeBinnedPrev')
def genAllAgesBinnedSUD(logger, r1):
    #a = "any_sud"
    report = f'''
### Categorised percentages of all SUD patients, via race, separated into age bins
|Race, qty           |1-11 y/o            |12-17 y/o           |18-34 y/o           |35-49 y/o           |50+ y/o             |
|--------------------|--------------------|--------------------|--------------------|--------------------|--------------------|
'''
    any = r1['any_sud']
    for row in any:
        report = report + f'''|{row}|{any[row][0]}|{any[row][1]}|{any[row][2]}|{any[row][3]}|{any[row][4]}|\n'''

    more = r1['morethan2_sud']
    for row in any:
        report = report + f'''|{row}|{more[row][0]}|{more[row][1]}|{more[row][2]}|{more[row][3]}|{more[row][4]}|\n'''
    report = report + '''\n***'''

    with open('../report/paper1markdown.md', 'a+') as f:
        f.write( report )

    return
