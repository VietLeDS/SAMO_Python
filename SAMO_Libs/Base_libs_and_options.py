# Precondition
import pandas as pd
import numpy as np
import logging as lg
import time
from multiprocessing import Process
import datetime
import random
from sqlalchemy import create_engine
from sqlalchemy import MetaData
from sqlalchemy import inspect
import os
import copy
import unidecode

from warnings import simplefilter
simplefilter(action="ignore", category=pd.errors.PerformanceWarning)
simplefilter(action='ignore', category=FutureWarning)
pd.options.mode.chained_assignment = None

FORMAT = '%(asctime)s %(message)s'
lg.basicConfig(format=FORMAT, level=lg.INFO)
# pd.set_option('display.max_columns', None)
# pd.set_option('display.max_rows', None)