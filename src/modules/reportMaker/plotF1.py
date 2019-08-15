from logs import logDecorator as lD
import jsonref, pprint
import numpy as np
import matplotlib.pyplot as plt

from psycopg2.sql import SQL, Identifier, Literal
from lib.databaseIO import pgIO
from collections import Counter

from tqdm import tqdm
from multiprocessing import Pool
from time import sleep

config = jsonref.load(open('../config/config.json'))
logBase = config['logging']['logBase'] + '.modules.plotF1.plotF1'
f1_config = jsonref.load(open('../config/modules/tejasF1.json'))

@lD.log(logBase + '.genIntro')
def genIntro(logger):

    report = f'''

## Description of Figure 1:
The Axis I/II disorders that are considered and their abbreviations are as follows:
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
Disorders with less than 3% prevalence are not shown in the figure
        '''
    with open('../report/paper1markdown.md', 'a+') as f:
        f.write( report )

    return

@lD.log(logBase + '.genFig')
def genFig(logger, r):
    try:
        print("Plotting Figure 1...")

        barWidth = 0.25

        #create bars
        AAbars = []
        NHPIbars = []
        MRbars = []
        for category in r:
            AAbars.append(r[category][0])
            NHPIbars.append(r[category][1])
            MRbars.append(r[category][2])

        #the X position of the bars
        r1 = np.arange(len(AAbars))
        r2 = [x + barWidth for x in r1]
        r3 = [x + barWidth for x in r2]
        r4 = r1+r2+r3

        #create barplots
        plt.bar(r1, AAbars, color='#000000', width=barWidth, edgecolor='white', label='AA')
        plt.bar(r2, NHPIbars, color='#3360CC', width=barWidth, edgecolor='white', label='NHPI')
        plt.bar(r3, MRbars, color='#A2D729', width=barWidth, edgecolor='white', label='MR')

        plt.ylabel('%', fontweight='bold')
        plt.xlabel('DSM-IV Diagnoses', fontweight='bold')
        plt.xticks([r + barWidth for r in range(len(r4))], list(r.keys()), fontsize=8, rotation=30)

        # create labels (the percentages) to be shown at the top of each bar
        for i in range(len(AAbars)):
            plt.text(x=r1[i]-0.15, y=AAbars[i]+0.1, s=AAbars[i], size=5)
        for i in range(len(NHPIbars)):
            plt.text(x=r2[i]-0.15, y=NHPIbars[i]+0.1, s=NHPIbars[i], size=5)
        for i in range(len(MRbars)):
            plt.text(x=r3[i]-0.15, y=MRbars[i]+0.1, s=MRbars[i], size=5)


        plt.legend()
        plt.tight_layout()

        plt.savefig('../results/diagnosesPercentageGraph.png', dpi=300)
        plt.close()

        report = '''
![](../results/diagnosesPercentageGraph.png)
***
'''

        with open('../report/paper1markdown.md', 'a+') as f:
            f.write( report )

    except Exception as e:
        logger.error('Failed to generate figure because of: {}'.format(e))
    return
