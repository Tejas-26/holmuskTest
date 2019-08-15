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
table4_config = jsonref.load(open('../config/modules/tejasT4.json'))
logBase = config['logging']['logBase'] + '.modules.reportMaker.writeT4'

@lD.log(logBase + '.rd')
def rd(logger, x):
    return round(x, 2)

@lD.log(logBase + '.genIntro')
def genIntro(logger):

    report = f'''
***
## Description of Table 4:
This table contains the odds ratios and confidence intervals after a logistic regression is performed for each race:
* Asian Americans, aged 12 and older
* Native Hawaiian, aged 12 and older
* Mixed Race, aged 12 and older
Logistic regression is performed for comorbidity of any SUD with other mental health disorders in the list below:
Mood - mood
Anxiety - anxiety
Adjustment - adjustment
ADHD/CD/ODD/DBD - adhd
Substance Use Disorder - sud
Psychotic - psyc
Personality - pers
Childhood-onset - childhood
Impulse-control - impulse
Cognitive - cognitive
Eating - eating
Somatoform - smtf
Dissociation - disso
Sleep - sleep
Factitious Disorders - fd
        '''
    with open('../report/paper1markdown.md', 'a+') as f:
        f.write( report )

    return

@lD.log(logBase + '.oddsRatiosByRace')
def oddsRatiosByRace(logger, aa, nh, mr, sampleabove12):
    suds = list(aa["OR"].keys())
    suds.remove('intercept')
    report = f'''
### Asian Americans, aged 12 or older
|Logistic Regression, Any SUD|N = {sampleabove12["AA"]}   |          |
|----------------------------|----------------------------|----------|
|**DSM-IV Diagnosis**        |**Odds Ratio**              |**95% CI**|'''

    for disorder in suds:
        report = report + f'''
|{disorder}                  |{rd(aa["OR"][disorder])}|{rd(aa["2.5%"][disorder])} - {rd(aa["97.5%"][disorder])}|'''

    report = report + f'''
***'''

    suds = list(nh["OR"].keys())
    suds.remove('intercept')
    report = report + f'''
### Native Hawaiians/Pacific Islanders, aged 12 or older
|Logistic Regression, Any SUD|N = {sampleabove12["NHPI"]} |          |
|----------------------------|----------------------------|----------|
|**DSM-IV Diagnosis**        |**Odds Ratio**              |**95% CI**|'''

    for disorder in suds:
        report = report + f'''
|{disorder}                  |{rd(nh["OR"][disorder])}|{rd(nh["2.5%"][disorder])} - {rd(nh["97.5%"][disorder])}|'''

    report = report + f'''
***'''

    suds = list(mr["OR"].keys())
    suds.remove('intercept')
    report = report + f'''
### Mixed Race, aged 12 or older
|Logistic Regression, Any SUD|N = {sampleabove12["MR"]}   |          |
|----------------------------|----------------------------|----------|
|**DSM-IV Diagnosis**        |**Odds Ratio**              |**95% CI**|'''

    for disorder in suds:
        report = report + f'''
|{disorder}                  |{rd(mr["OR"][disorder])}|{rd(mr["2.5%"][disorder])} - {rd(mr["97.5%"][disorder])}|'''

    report = report + f'''
***'''

    with open('../report/paper1markdown.md', 'a+') as f:
        f.write( report )

    return
