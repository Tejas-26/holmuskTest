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
logBase = config['logging']['logBase'] + '.modules.plotF1.plotF1'
f1_config = jsonref.load(open('../config/modules/tejasF1.json'))
