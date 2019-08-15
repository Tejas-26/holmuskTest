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

@lD.log(logBase + '.rd')
def rd(logger, x):
    return round(x, 2)

@lD.log(logBase + '.oddsRatiosAllRaces')
def oddsRatiosAllRaces(logger, r1, r2, r3):

    totalsampleabove12 = 0
    for race in r3:
        totalsampleabove12+=r3[race]

    report = f'''
### All races, aged 12 or older
|Logistic Regression, Any SUD|N = {totalsampleabove12}    |          |
|----------------------------|----------------------------|----------|
|**Any SUD**                 |**Odds Ratio**              |**95% CI**|
|*Race*                      |                            |          |
|NHPI vs AA                  |{rd(r1["OR"]["NHPI"])}          |{rd(r1["2.5%"]["NHPI"])} - {rd(r1["97.5%"]["NHPI"])}|
|MR vs AA                    |{rd(r1["OR"]["MR"])}            |{rd(r1["2.5%"]["MR"])} - {rd(r1["97.5%"]["MR"])}|
|*Age in years*              |                            |          |
|12 - 17 vs 50+              |{rd(r1["OR"]["12-17"])}         |{rd(r1["2.5%"]["12-17"])} - {rd(r1["97.5%"]["12-17"])}|
|18 - 34 vs 50+              |{rd(r1["OR"]["18-34"])}         |{rd(r1["2.5%"]["18-34"])} - {rd(r1["97.5%"]["18-34"])}|
|35 - 49 vs 50+              |{rd(r1["OR"]["35-49"])}         |{rd(r1["2.5%"]["35-49"])} - {rd(r1["97.5%"]["35-49"])}|
|*Sex*                       |                            |          |
|Male vs Female              |{rd(r1["OR"]["M"])}             |{rd(r1["2.5%"]["M"])} - {rd(r1["97.5%"]["M"])}|
|*Treatment Setting*         |                            |          |
|Hospital vs Mental Health Center|{rd(r1["OR"]["Inpatient"])}      |{rd(r1["2.5%"]["Inpatient"])} - {rd(r1["97.5%"]["Inpatient"])}|
|**at least 2 SUDs**               |**Odds Ratio**              |**95% CI**|
|*Race*                            |                            |          |
|NHPI vs AA                        |{rd(r2["OR"]["NHPI"])}          |{rd(r2["2.5%"]["NHPI"])} - {rd(r2["97.5%"]["NHPI"])}|
|MR vs AA                          |{rd(r2["OR"]["MR"])}            |{rd(r2["2.5%"]["MR"])} - {rd(r2["97.5%"]["MR"])}|
|*Age in years*                    |                            |          |
|12 - 17 vs 50+                    |{rd(r2["OR"]["12-17"])}         |{rd(r2["2.5%"]["12-17"])} - {rd(r2["97.5%"]["12-17"])}|
|18 - 34 vs 50+                    |{rd(r2["OR"]["18-34"])}         |{rd(r2["2.5%"]["18-34"])} - {rd(r2["97.5%"]["18-34"])}|
|35 - 49 vs 50+                    |{rd(r2["OR"]["35-49"])}         |{rd(r2["2.5%"]["35-49"])} - {rd(r2["97.5%"]["35-49"])}|
|*Sex*                             |                            |          |
|Male vs Female                    |{rd(r2["OR"]["M"])}             |{rd(r2["2.5%"]["M"])} - {rd(r2["97.5%"]["M"])}|
|*Treatment Setting*               |                            |          |
|Hospital vs Mental Health Center  |{rd(r2["OR"]["Inpatient"])}      |{rd(r2["2.5%"]["Inpatient"])} - {rd(r2["97.5%"]["Inpatient"])}|
***
'''

    with open('../report/paper1markdown.md', 'a+') as f:
        f.write( report )

    return

@lD.log(logBase + '.oddsRatiosByRace')
def oddsRatiosByRace(logger, r1, r3):
    aa = r1["AA"]
    nh = r1["NHPI"]
    mr = r1["MR"]
    aa_any = aa["anySUD"]
    nh_any = nh["anySUD"]
    mr_any = mr["anySUD"]
    aa_al2 = aa["atleast2"]
    nh_al2 = nh["atleast2"]
    mr_al2 = mr["atleast2"]
    report = f'''
### Asian Americans, aged 12 or older
|Logistic Regression, Any SUD|N = {r3["AA"]}   |          |
|----------------------------|----------------------------|----------|
|**Any SUD**                 |**Odds Ratio**              |**95% CI**|
|*Age in years*              |                            |          |
|12 - 17 vs 50+              |{rd(aa_any["OR"]["12-17"])}|{rd(aa_any["2.5%"]["12-17"])} - {rd(aa_any["97.5%"]["12-17"])}|
|18 - 34 vs 50+              |{rd(aa_any["OR"]["18-34"])}|{rd(aa_any["2.5%"]["18-34"])} - {rd(aa_any["97.5%"]["18-34"])}|
|35 - 49 vs 50+              |{rd(aa_any["OR"]["35-49"])}|{rd(aa_any["2.5%"]["35-49"])} - {rd(aa_any["97.5%"]["35-49"])}|
|*Sex*                       |                            |          |
|Male vs Female              |{rd(aa_any["OR"]["M"])}|{rd(aa_any["2.5%"]["M"])} - {rd(aa_any["97.5%"]["M"])}|
|*Treatment Setting*         |                            |          |
|Hospital vs Mental Health Center|{rd(aa_any["OR"]["Inpatient"])}  |{rd(aa_any["2.5%"]["Inpatient"])} - {rd(aa_any["97.5%"]["Inpatient"])}|
|**2 or more SUDs**                |**Odds Ratio**        |**95% CI**|
|*Age in years*                    |                      |          |
|12 - 17 vs 50+              |{rd(aa_al2["OR"]["12-17"])}|{rd(aa_al2["2.5%"]["12-17"])} - {rd(aa_al2["97.5%"]["12-17"])}|
|18 - 34 vs 50+              |{rd(aa_al2["OR"]["18-34"])}|{rd(aa_al2["2.5%"]["18-34"])} - {rd(aa_al2["97.5%"]["18-34"])}|
|35 - 49 vs 50+              |{rd(aa_al2["OR"]["35-49"])}|{rd(aa_al2["2.5%"]["35-49"])} - {rd(aa_al2["97.5%"]["35-49"])}|
|*Sex*                       |                            |          |
|Male vs Female              |{rd(aa_al2["OR"]["M"])}|{rd(aa_al2["2.5%"]["M"])} - {rd(aa_al2["97.5%"]["M"])}|
|*Treatment Setting*         |                            |          |
|Hospital vs Mental Health Center|{rd(aa_al2["OR"]["Inpatient"])}  |{rd(aa_al2["2.5%"]["Inpatient"])} - {rd(aa_al2["97.5%"]["Inpatient"])}|
***
### Native Hawaiians/Pacific Islanders, aged 12 or older
|Logistic Regression, Any SUD|N = {r3["NHPI"]} |          |
|----------------------------|----------------------------|----------|
|**Any SUD**                 |**Odds Ratio**              |**95% CI**|
|*Age in years*              |                            |          |
|12 - 17 vs 50+              |{rd(nh_any["OR"]["12-17"])}|{rd(nh_any["2.5%"]["12-17"])} - {rd(nh_any["97.5%"]["12-17"])}|
|18 - 34 vs 50+              |{rd(nh_any["OR"]["18-34"])}|{rd(nh_any["2.5%"]["18-34"])} - {rd(nh_any["97.5%"]["18-34"])}|
|35 - 49 vs 50+              |{rd(nh_any["OR"]["35-49"])}|{rd(nh_any["2.5%"]["35-49"])} - {rd(nh_any["97.5%"]["35-49"])}|
|*Sex*                       |                            |          |
|Male vs Female              |{rd(nh_any["OR"]["M"])}|{rd(nh_any["2.5%"]["M"])} - {rd(nh_any["97.5%"]["M"])}|
|*Treatment Setting*         |                            |          |
|Hospital vs Mental Health Center|{rd(nh_any["OR"]["Inpatient"])}  |{rd(nh_any["2.5%"]["Inpatient"])} - {rd(nh_any["97.5%"]["Inpatient"])}|
|**2 or more SUDs**                |**Odds Ratio**        |**95% CI**|
|*Age in years*                    |                      |          |
|12 - 17 vs 50+              |{rd(nh_al2["OR"]["12-17"])}|{rd(nh_al2["2.5%"]["12-17"])} - {rd(nh_al2["97.5%"]["12-17"])}|
|18 - 34 vs 50+              |{rd(nh_al2["OR"]["18-34"])}|{rd(nh_al2["2.5%"]["18-34"])} - {rd(nh_al2["97.5%"]["18-34"])}|
|35 - 49 vs 50+              |{rd(nh_al2["OR"]["35-49"])}|{rd(nh_al2["2.5%"]["35-49"])} - {rd(nh_al2["97.5%"]["35-49"])}|
|*Sex*                       |                            |          |
|Male vs Female              |{rd(nh_al2["OR"]["M"])}|{rd(nh_al2["2.5%"]["M"])} - {rd(nh_al2["97.5%"]["M"])}|
|*Treatment Setting*         |                            |          |
|Hospital vs Mental Health Center|{rd(nh_al2["OR"]["Inpatient"])}  |{rd(nh_al2["2.5%"]["Inpatient"])} - {rd(nh_al2["97.5%"]["Inpatient"])}|
***
### Mixed Race, aged 12 or older
|Logistic Regression, Any SUD|N = {r3["MR"]}   |          |
|----------------------------|----------------------------|----------|
|**Any SUD**                 |**Odds Ratio**              |**95% CI**|
|*Age in years*              |                            |          |
|12 - 17 vs 50+              |{rd(mr_any["OR"]["12-17"])}|{rd(mr_any["2.5%"]["12-17"])} - {rd(mr_any["97.5%"]["12-17"])}|
|18 - 34 vs 50+              |{rd(mr_any["OR"]["18-34"])}|{rd(mr_any["2.5%"]["18-34"])} - {rd(mr_any["97.5%"]["18-34"])}|
|35 - 49 vs 50+              |{rd(mr_any["OR"]["35-49"])}|{rd(mr_any["2.5%"]["35-49"])} - {rd(mr_any["97.5%"]["35-49"])}|
|*Sex*                       |                            |          |
|Male vs Female              |{rd(mr_any["OR"]["M"])}|{rd(mr_any["2.5%"]["M"])} - {rd(mr_any["97.5%"]["M"])}|
|*Treatment Setting*         |                            |          |
|Hospital vs Mental Health Center|{rd(mr_any["OR"]["Inpatient"])}  |{rd(mr_any["2.5%"]["Inpatient"])} - {rd(mr_any["97.5%"]["Inpatient"])}|
|**2 or more SUDs**                |**Odds Ratio**        |**95% CI**|
|*Age in years*                    |                      |          |
|12 - 17 vs 50+              |{rd(mr_al2["OR"]["12-17"])}|{rd(mr_al2["2.5%"]["12-17"])} - {rd(mr_al2["97.5%"]["12-17"])}|
|18 - 34 vs 50+              |{rd(mr_al2["OR"]["18-34"])}|{rd(mr_al2["2.5%"]["18-34"])} - {rd(mr_al2["97.5%"]["18-34"])}|
|35 - 49 vs 50+              |{rd(mr_al2["OR"]["35-49"])}|{rd(mr_al2["2.5%"]["35-49"])} - {rd(mr_al2["97.5%"]["35-49"])}|
|*Sex*                       |                            |          |
|Male vs Female              |{rd(mr_al2["OR"]["M"])}|{rd(mr_al2["2.5%"]["M"])} - {rd(mr_al2["97.5%"]["M"])}|
|*Treatment Setting*         |                            |          |
|Hospital vs Mental Health Center|{rd(mr_al2["OR"]["Inpatient"])}  |{rd(mr_al2["2.5%"]["Inpatient"])} - {rd(mr_al2["97.5%"]["Inpatient"])}|
***
'''

    with open('../report/paper1markdown.md', 'a+') as f:
        f.write( report )

    return
