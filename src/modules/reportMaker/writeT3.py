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
    report = open('../data/comorbidMDTable3Intro.txt', 'r')
    report = report.read()
    with open('../report/paper1markdown.md', 'a+') as f:
        f.write( report )
    return

@lD.log(logBase + '.oddsRatiosAllRaces')
def oddsRatiosAllRaces(logger, r1, r2):

    totalsampleabove12 = 0
    for race in r2:
        for i in range(1,5):
            totalsampleabove12 += r2[race][1][i]

    report = f'''
### All races, aged 12 or older
|Logistic Regression, Any SUD|N = {totalsampleabove12}    |          |
|----------------------------|----------------------------|----------|
|**Any SUD**                 |**Odds Ratio**              |**95% CI**|
|*Race*                      |                            |          |
|NHPI vs AA                  |{r1["NHPI"][0][0]}          |{r1["NHPI"][0][1]} - {r1["NHPI"][0][2]}|
|MR vs AA                    |{r1["MR"][0][0]}            |{r1["MR"][0][1]} - {r1["MR"][0][2]}|
|*Age in years*              |                            |          |
|12 - 17 vs 50+              |{r1["12-17"][0][0]}         |{r1["12-17"][0][1]} - {r1["12-17"][0][2]}|
|18 - 34 vs 50+              |{r1["18-34"][0][0]}         |{r1["18-34"][0][1]} - {r1["18-34"][0][2]}|
|35 - 49 vs 50+              |{r1["35-49"][0][0]}         |{r1["35-49"][0][1]} - {r1["35-49"][0][2]}|
|*Sex*                       |                            |          |
|Male vs Female              |{r1["M"][0][0]}             |{r1["M"][0][1]} - {r1["M"][0][2]}|
|*Treatment Setting*         |                            |          |
|Hospital vs Mental Health Center|{r1["Hospital"][0][0]}      |{r1["Hospital"][0][1]} - {r1["Hospital"][0][2]}|
|**2 or more SUDs**                |**Odds Ratio**              |**95% CI**|
|*Race*                            |                            |          |
|NHPI vs AA                        |{r1["NHPI"][1][0]}          |{r1["NHPI"][1][1]} - {r1["NHPI"][1][2]}|
|MR vs AA                          |{r1["MR"][1][0]}            |{r1["MR"][1][1]} - {r1["MR"][1][2]}|
|*Age in years*                    |                            |          |
|12 - 17 vs 50+                    |{r1["12-17"][1][0]}         |{r1["12-17"][1][1]} - {r1["12-17"][1][2]}|
|18 - 34 vs 50+                    |{r1["18-34"][1][0]}         |{r1["18-34"][1][1]} - {r1["18-34"][1][2]}|
|35 - 49 vs 50+                    |{r1["35-49"][1][0]}         |{r1["35-49"][1][1]} - {r1["35-49"][1][2]}|
|*Sex*                             |                            |          |
|Male vs Female                    |{r1["M"][1][0]}             |{r1["M"][1][1]} - {r1["M"][1][2]}|
|*Treatment Setting*               |                            |          |
|Hospital vs Mental Health Center  |{r1["Hospital"][1][0]}      |{r1["Hospital"][1][1]} - {r1["Hospital"][1][2]}|
***
'''

    with open('../report/paper1Report.md', 'a+') as f:
        f.write( report )

    return

@lD.log(logBase + '.oddsRatiosByRace')
def oddsRatiosByRace(logger, r1, r2, r3):

    sampleabove12 = {
        "AA": 0,
        "NHPI": 0,
        "MR": 0
    }

    for race in r3:
        for i in range(1,5):
            sampleabove12[race] += r3[race][1][i]

    report = f'''
### Asian Americans, aged 12 or older
|Logistic Regression, Any SUD|N = {sampleabove12["AA"]}   |          |
|----------------------------|----------------------------|----------|
|**Any SUD**                 |**Odds Ratio**              |**95% CI**|
|*Age in years*              |                            |          |
|12 - 17 vs 50+              |{r1["12-17"][0][0]}         |{r1["12-17"][0][1]} - {r1["12-17"][0][2]}|
|18 - 34 vs 50+              |{r1["18-34"][0][0]}         |{r1["18-34"][0][1]} - {r1["18-34"][0][2]}|
|35 - 49 vs 50+              |{r1["35-49"][0][0]}         |{r1["35-49"][0][1]} - {r1["35-49"][0][2]}|
|*Sex*                       |                            |          |
|Male vs Female              |{r1["M"][0][0]}             |{r1["M"][0][1]} - {r1["M"][0][2]}|
|*Treatment Setting*         |                            |          |
|Hospital vs Mental Health Center|{r1["Hospital"][0][0]}  |{r1["Hospital"][0][1]} - {r1["Hospital"][0][2]}|
|**2 or more SUDs**                |**Odds Ratio**        |**95% CI**|
|*Age in years*                    |                      |          |
|12 - 17 vs 50+                    |{r2["12-17"][0][0]}   |{r2["12-17"][0][1]} - {r2["12-17"][0][2]}|
|18 - 34 vs 50+                    |{r2["18-34"][0][0]}   |{r2["18-34"][0][1]} - {r2["18-34"][0][2]}|
|35 - 49 vs 50+                    |{r2["35-49"][0][0]}   |{r2["35-49"][0][1]} - {r2["35-49"][0][2]}|
|*Sex*                             |                      |          |
|Male vs Female                    |{r2["M"][0][0]}       |{r2["M"][0][1]} - {r2["M"][0][2]}|
|*Treatment Setting*               |                      |          |
|Hospital vs Mental Health Center  |{r2["Hospital"][0][0]}|{r2["Hospital"][0][1]} - {r2["Hospital"][0][2]}|
***
### Native Hawaiians/Pacific Islanders, aged 12 or older
|Logistic Regression, Any SUD|N = {sampleabove12["NHPI"]} |          |
|----------------------------|----------------------------|----------|
|**Any SUD**                 |**Odds Ratio**              |**95% CI**|
|*Age in years*              |                            |          |
|12 - 17 vs 50+              |{r1["12-17"][1][0]}         |{r1["12-17"][1][1]} - {r1["12-17"][1][2]}|
|18 - 34 vs 50+              |{r1["18-34"][1][0]}         |{r1["18-34"][1][1]} - {r1["18-34"][1][2]}|
|35 - 49 vs 50+              |{r1["35-49"][1][0]}         |{r1["35-49"][1][1]} - {r1["35-49"][1][2]}|
|*Sex*                       |                            |          |
|Male vs Female              |{r1["M"][1][0]}             |{r1["M"][1][1]} - {r1["M"][1][2]}|
|*Treatment Setting*         |                            |          |
|Hospital vs Mental Health Center|{r1["Hospital"][1][0]}  |{r1["Hospital"][1][1]} - {r1["Hospital"][1][2]}|
|**2 or more SUDs**                |**Odds Ratio**        |**95% CI**|
|*Age in years*                    |                      |          |
|12 - 17 vs 50+                    |{r2["12-17"][1][0]}   |{r2["12-17"][1][1]} - {r2["12-17"][1][2]}|
|18 - 34 vs 50+                    |{r2["18-34"][1][0]}   |{r2["18-34"][1][1]} - {r2["18-34"][1][2]}|
|35 - 49 vs 50+                    |{r2["35-49"][1][0]}   |{r2["35-49"][1][1]} - {r2["35-49"][1][2]}|
|*Sex*                             |                      |          |
|Male vs Female                    |{r2["M"][1][0]}       |{r2["M"][1][1]} - {r2["M"][1][2]}|
|*Treatment Setting*               |                      |          |
|Hospital vs Mental Health Center  |{r2["Hospital"][1][0]}|{r2["Hospital"][1][1]} - {r2["Hospital"][1][2]}|
***
### Mixed Race, aged 12 or older
|Logistic Regression, Any SUD|N = {sampleabove12["MR"]}   |          |
|----------------------------|----------------------------|----------|
|**Any SUD**                 |**Odds Ratio**              |**95% CI**|
|*Age in years*              |                            |          |
|12 - 17 vs 50+              |{r1["12-17"][2][0]}         |{r1["12-17"][2][1]} - {r1["12-17"][2][2]}|
|18 - 34 vs 50+              |{r1["18-34"][2][0]}         |{r1["18-34"][2][1]} - {r1["18-34"][2][2]}|
|35 - 49 vs 50+              |{r1["35-49"][2][0]}         |{r1["35-49"][2][1]} - {r1["35-49"][2][2]}|
|*Sex*                       |                            |          |
|Male vs Female              |{r1["M"][2][0]}             |{r1["M"][2][1]} - {r1["M"][2][2]}|
|*Treatment Setting*         |                            |          |
|Hospital vs Mental Health Center|{r1["Hospital"][2][0]}  |{r1["Hospital"][2][1]} - {r1["Hospital"][2][2]}|
|**2 or more SUDs**                |**Odds Ratio**        |**95% CI**|
|*Age in years*                    |                      |          |
|12 - 17 vs 50+                    |{r2["12-17"][2][0]}   |{r2["12-17"][2][1]} - {r2["12-17"][2][2]}|
|18 - 34 vs 50+                    |{r2["18-34"][2][0]}   |{r2["18-34"][2][1]} - {r2["18-34"][2][2]}|
|35 - 49 vs 50+                    |{r2["35-49"][2][0]}   |{r2["35-49"][2][1]} - {r2["35-49"][2][2]}|
|*Sex*                             |                      |          |
|Male vs Female                    |{r2["M"][2][0]}       |{r2["M"][2][1]} - {r2["M"][2][2]}|
|*Treatment Setting*               |                      |          |
|Hospital vs Mental Health Center  |{r2["Hospital"][2][0]}|{r2["Hospital"][2][1]} - {r2["Hospital"][2][2]}|
'''

    with open('../report/paper1Report.md', 'a+') as f:
        f.write( report )

    return
