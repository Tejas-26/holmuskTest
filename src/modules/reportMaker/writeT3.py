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
|NHPI vs AA                  |{r1["OR"]["NHPI"]}          |{r1["2.5%"]["NHPI"]} - {r1["97.5%"]["NHPI"]}|
|MR vs AA                    |{r1["OR"]["MR"]}            |{r1["2.5%"]["MR"]} - {r1["97.5%"]["MR"]}|
|*Age in years*              |                            |          |
|12 - 17 vs 50+              |{r1["OR"]["12-17"]}         |{r1["2.5%"]["12-17"]} - {r1["97.5%"]["12-17"]}|
|18 - 34 vs 50+              |{r1["OR"]["18-34"]}         |{r1["2.5%"]["18-34"]} - {r1["97.5%"]["18-34"]}|
|35 - 49 vs 50+              |{r1["OR"]["35-49"]}         |{r1["2.5%"]["35-49"]} - {r1["97.5%"]["35-49"]}|
|*Sex*                       |                            |          |
|Male vs Female              |{r1["OR"]["M"]}             |{r1["2.5%"]["M"]} - {r1["97.5%"]["M"]}|
|*Treatment Setting*         |                            |          |
|Hospital vs Mental Health Center|{r1["OR"]["Inpatient"]}      |{r1["2.5%"]["Inpatient"]} - {r1["97.5%"]["Inpatient"]}|
|**at least 2 SUDs**               |**Odds Ratio**              |**95% CI**|
|*Race*                            |                            |          |
|NHPI vs AA                        |{r2["OR"]["NHPI"]}          |{r2["2.5%"]["NHPI"]} - {r2["97.5%"]["NHPI"]}|
|MR vs AA                          |{r2["OR"]["MR"]}            |{r2["2.5%"]["MR"]} - {r2["97.5%"]["MR"]}|
|*Age in years*                    |                            |          |
|12 - 17 vs 50+                    |{r2["OR"]["12-17"]}         |{r2["2.5%"]["12-17"]} - {r2["97.5%"]["12-17"]}|
|18 - 34 vs 50+                    |{r2["OR"]["18-34"]}         |{r2["2.5%"]["18-34"]} - {r2["97.5%"]["18-34"]}|
|35 - 49 vs 50+                    |{r2["OR"]["35-49"]}         |{r2["2.5%"]["35-49"]} - {r2["97.5%"]["35-49"]}|
|*Sex*                             |                            |          |
|Male vs Female                    |{r2["OR"]["M"]}             |{r2["2.5%"]["M"]} - {r2["97.5%"]["M"]}|
|*Treatment Setting*               |                            |          |
|Hospital vs Mental Health Center  |{r2["OR"]["Inpatient"]}      |{r2["2.5%"]["Inpatient"]} - {r2["97.5%"]["Inpatient"]}|
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
|12 - 17 vs 50+              |{aa_any["OR"]["12-17"]}|{aa_any["2.5%"]["12-17"]} - {aa_any["97.5%"]["12-17"]}|
|18 - 34 vs 50+              |{aa_any["OR"]["18-34"]}|{aa_any["2.5%"]["18-34"]} - {aa_any["97.5%"]["18-34"]}|
|35 - 49 vs 50+              |{aa_any["OR"]["35-49"]}|{aa_any["2.5%"]["35-49"]} - {aa_any["97.5%"]["35-49"]}|
|*Sex*                       |                            |          |
|Male vs Female              |{aa_any["OR"]["M"]}|{aa_any["2.5%"]["M"]} - {aa_any["97.5%"]["M"]}|
|*Treatment Setting*         |                            |          |
|Hospital vs Mental Health Center|{aa_any["OR"]["Inpatient"]}  |{aa_any["2.5%"]["Inpatient"]} - {aa_any["97.5%"]["Inpatient"]}|
|**2 or more SUDs**                |**Odds Ratio**        |**95% CI**|
|*Age in years*                    |                      |          |
|12 - 17 vs 50+              |{aa_al2["OR"]["12-17"]}|{aa_al2["2.5%"]["12-17"]} - {aa_al2["97.5%"]["12-17"]}|
|18 - 34 vs 50+              |{aa_al2["OR"]["18-34"]}|{aa_al2["2.5%"]["18-34"]} - {aa_al2["97.5%"]["18-34"]}|
|35 - 49 vs 50+              |{aa_al2["OR"]["35-49"]}|{aa_al2["2.5%"]["35-49"]} - {aa_al2["97.5%"]["35-49"]}|
|*Sex*                       |                            |          |
|Male vs Female              |{aa_al2["OR"]["M"]}|{aa_al2["2.5%"]["M"]} - {aa_al2["97.5%"]["M"]}|
|*Treatment Setting*         |                            |          |
|Hospital vs Mental Health Center|{aa_al2["OR"]["Inpatient"]}  |{aa_al2["2.5%"]["Inpatient"]} - {aa_al2["97.5%"]["Inpatient"]}|
***
### Native Hawaiians/Pacific Islanders, aged 12 or older
|Logistic Regression, Any SUD|N = {r3["NHPI"]} |          |
|----------------------------|----------------------------|----------|
|**Any SUD**                 |**Odds Ratio**              |**95% CI**|
|*Age in years*              |                            |          |
|12 - 17 vs 50+              |{nh_any["OR"]["12-17"]}|{nh_any["2.5%"]["12-17"]} - {nh_any["97.5%"]["12-17"]}|
|18 - 34 vs 50+              |{nh_any["OR"]["18-34"]}|{nh_any["2.5%"]["18-34"]} - {nh_any["97.5%"]["18-34"]}|
|35 - 49 vs 50+              |{nh_any["OR"]["35-49"]}|{nh_any["2.5%"]["35-49"]} - {nh_any["97.5%"]["35-49"]}|
|*Sex*                       |                            |          |
|Male vs Female              |{nh_any["OR"]["M"]}|{nh_any["2.5%"]["M"]} - {nh_any["97.5%"]["M"]}|
|*Treatment Setting*         |                            |          |
|Hospital vs Mental Health Center|{nh_any["OR"]["Inpatient"]}  |{nh_any["2.5%"]["Inpatient"]} - {nh_any["97.5%"]["Inpatient"]}|
|**2 or more SUDs**                |**Odds Ratio**        |**95% CI**|
|*Age in years*                    |                      |          |
|12 - 17 vs 50+              |{nh_al2["OR"]["12-17"]}|{nh_al2["2.5%"]["12-17"]} - {nh_al2["97.5%"]["12-17"]}|
|18 - 34 vs 50+              |{nh_al2["OR"]["18-34"]}|{nh_al2["2.5%"]["18-34"]} - {nh_al2["97.5%"]["18-34"]}|
|35 - 49 vs 50+              |{nh_al2["OR"]["35-49"]}|{nh_al2["2.5%"]["35-49"]} - {nh_al2["97.5%"]["35-49"]}|
|*Sex*                       |                            |          |
|Male vs Female              |{nh_al2["OR"]["M"]}|{nh_al2["2.5%"]["M"]} - {nh_al2["97.5%"]["M"]}|
|*Treatment Setting*         |                            |          |
|Hospital vs Mental Health Center|{nh_al2["OR"]["Inpatient"]}  |{nh_al2["2.5%"]["Inpatient"]} - {nh_al2["97.5%"]["Inpatient"]}|
***
### Mixed Race, aged 12 or older
|Logistic Regression, Any SUD|N = {r3["MR"]}   |          |
|----------------------------|----------------------------|----------|
|**Any SUD**                 |**Odds Ratio**              |**95% CI**|
|*Age in years*              |                            |          |
|12 - 17 vs 50+              |{mr_any["OR"]["12-17"]}|{mr_any["2.5%"]["12-17"]} - {mr_any["97.5%"]["12-17"]}|
|18 - 34 vs 50+              |{mr_any["OR"]["18-34"]}|{mr_any["2.5%"]["18-34"]} - {mr_any["97.5%"]["18-34"]}|
|35 - 49 vs 50+              |{mr_any["OR"]["35-49"]}|{mr_any["2.5%"]["35-49"]} - {mr_any["97.5%"]["35-49"]}|
|*Sex*                       |                            |          |
|Male vs Female              |{mr_any["OR"]["M"]}|{mr_any["2.5%"]["M"]} - {mr_any["97.5%"]["M"]}|
|*Treatment Setting*         |                            |          |
|Hospital vs Mental Health Center|{mr_any["OR"]["Inpatient"]}  |{mr_any["2.5%"]["Inpatient"]} - {mr_any["97.5%"]["Inpatient"]}|
|**2 or more SUDs**                |**Odds Ratio**        |**95% CI**|
|*Age in years*                    |                      |          |
|12 - 17 vs 50+              |{mr_al2["OR"]["12-17"]}|{mr_al2["2.5%"]["12-17"]} - {mr_al2["97.5%"]["12-17"]}|
|18 - 34 vs 50+              |{mr_al2["OR"]["18-34"]}|{mr_al2["2.5%"]["18-34"]} - {mr_al2["97.5%"]["18-34"]}|
|35 - 49 vs 50+              |{mr_al2["OR"]["35-49"]}|{mr_al2["2.5%"]["35-49"]} - {mr_al2["97.5%"]["35-49"]}|
|*Sex*                       |                            |          |
|Male vs Female              |{mr_al2["OR"]["M"]}|{mr_al2["2.5%"]["M"]} - {mr_al2["97.5%"]["M"]}|
|*Treatment Setting*         |                            |          |
|Hospital vs Mental Health Center|{mr_al2["OR"]["Inpatient"]}  |{mr_al2["2.5%"]["Inpatient"]} - {mr_al2["97.5%"]["Inpatient"]}|
'''

    with open('../report/paper1markdown.md', 'a+') as f:
        f.write( report )

    return
