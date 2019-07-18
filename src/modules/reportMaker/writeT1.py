from logs import logDecorator as lD
import jsonref, pprint

from psycopg2.sql import SQL, Identifier, Literal
from lib.databaseIO import pgIO
from collections import Counter

from tqdm import tqdm
from multiprocessing import Pool
from time import sleep

config = jsonref.load(open('../config/config.json'))
logBase = config['logging']['logBase'] + '.modules.writeT1.writeT1'

# function makes intro to paper 1
@lD.log(logBase + '.genIntro')
def genIntro(logger):
    report = open('../data/comorbidMDIntro.txt', 'r')
    report = report.read()
    with open('../report/paper1markdown.md', 'w+') as f:
        f.write( report )
    return

@lD.log(logBase + '.genRace')
def genRace(logger, r):

    report = f'''
|Race     |Count          |
|---------|---------------|
|AA       |{r["AA"]}   |
|NHPI     |{r["NHPI"]} |
|MR       |{r["MR"]}   |
|**Total**|{r["AA"]+r["NHPI"]+r["MR"]}|
'''

    with open('../report/paper1markdown.md', 'a+') as f:
        f.write( report )

    return

@lD.log(logBase + '.genPC')
def genPC(logger, n, d):
    result = round((n/d)*100, 1)
    return result


@lD.log(logBase + '.genRaceAge')
def genRaceAge(logger, r):

    report = f'''
### Number of patients grouped by race and age
|Age  |AA|%|NHPI|%|MR|%|
|-----|--|-|-----|-|--|-|
|1-11 |{r["AA"][1][0]}|{genPC(r["AA"][1][0],r["AA"][0])}|{r["NHPI"][1][0]}|{genPC(r["NHPI"][1][0],r["NHPI"][0])}|{r["MR"][1][0]}|{genPC(r["MR"][1][0],r["MR"][0])}|
|12-17|{r["AA"][1][1]}|{genPC(r["AA"][1][1],r["AA"][0])}|{r["NHPI"][1][1]}|{genPC(r["NHPI"][1][1],r["NHPI"][0])}|{r["MR"][1][1]}|{genPC(r["MR"][1][1],r["MR"][0])}|
|18-34|{r["AA"][1][2]}|{genPC(r["AA"][1][2],r["AA"][0])}|{r["NHPI"][1][2]}|{genPC(r["NHPI"][1][2],r["NHPI"][0])}|{r["MR"][1][2]}|{genPC(r["MR"][1][2],r["MR"][0])}|
|35-49|{r["AA"][1][3]}|{genPC(r["AA"][1][3],r["AA"][0])}|{r["NHPI"][1][3]}|{genPC(r["NHPI"][1][3],r["NHPI"][0])}|{r["MR"][1][3]}|{genPC(r["MR"][1][3],r["MR"][0])}|
|50+  |{r["AA"][1][4]}|{genPC(r["AA"][1][4],r["AA"][0])}|{r["NHPI"][1][4]}|{genPC(r["NHPI"][1][4],r["NHPI"][0])}|{r["MR"][1][4]}|{genPC(r["MR"][1][4],r["MR"][0])}|
'''

    with open('../report/paper1markdown.md', 'a+') as f:
        f.write( report )

    return

@lD.log(logBase + '.genRaceSex')
def genRaceSex(logger, r):

    report = f'''
### Number of patients grouped by race and sex
|Sex|AA|%|NHPI|%|MR|%|
|---|--|-|-----|-|--|-|
|Male  |{r["AA"][2][0]}|{genPC(r["AA"][2][0],r["AA"][0])}|{r["NHPI"][2][0]}|{genPC(r["NHPI"][2][0],r["NHPI"][0])}|{r["MR"][2][0]}|{genPC(r["MR"][2][0],r["MR"][0])}|
|Female|{r["AA"][2][1]}|{genPC(r["AA"][2][1],r["AA"][0])}|{r["NHPI"][2][1]}|{genPC(r["NHPI"][2][1],r["NHPI"][0])}|{r["MR"][2][1]}|{genPC(r["MR"][2][1],r["MR"][0])}|
'''

    with open('../report/paper1markdown.md', 'a+') as f:
        f.write( report )

    return

@lD.log(logBase + '.genRaceSetting')
def genRaceSetting(logger, r):

    report = f'''
### Number of patients grouped by race and setting
|Setting|AA|%|NHPI|%|MR|%|
|-------|--|-|-----|-|--|-|
|Hospital            |{r["AA"][3][0]}|{genPC(r["AA"][3][0],r["AA"][0])}|{r["NHPI"][3][0]}|{genPC(r["NHPI"][3][0],r["NHPI"][0])}|{r["MR"][3][0]}|{genPC(r["MR"][3][0],r["MR"][0])}|
|Mental Health Center|{r["AA"][3][1]}|{genPC(r["AA"][3][1],r["AA"][0])}|{r["NHPI"][3][1]}|{genPC(r["NHPI"][3][1],r["NHPI"][0])}|{r["MR"][3][1]}|{genPC(r["MR"][3][1],r["MR"][0])}|
***
'''

    with open('../report/paper1markdown.md', 'a+') as f:
        f.write( report )

    return
