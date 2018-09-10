#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
==============================================
function
----------------------------------------------

         FILE: m01_sku_selection.py

  DESCRIPTION: sku selection
       TARGET: select n sku satisfy the alpha * M order
       OPTIONS:
            inputs:
                # ss_data: sale and stock data
                # alpha: the sale scale to be satisfied
        NOTES:
            # ss_data is a DataFrame must contain the following columns:
                    - sku_id
                    - sale_30_avg: mean of latest 30 days sales
                    - canuse_qtty: canuse_stock
                    - sale_3_avg: mean of latest 3 day sales
            # alpha must in [0,1]
 REQUIREMENTS: ---
       AUTHOR: ---
      VERSION: 1.0
      CREATED: ---
       MODIFY: ---
=================================================
"""

from auxiliary import fileManagement as fm
import numpy as np
from sklearn import preprocessing
import pandas as pd


from auxiliary import getRunningEnvironment

import datetime as dt
from textwrap import wrap
import copy
import os
import shutil

from matplotlib.font_manager import FontProperties
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

import pandas as pd

import scipy.stats

from scipy.optimize import curve_fit
from numpy import inf

import warnings
warnings.simplefilter('ignore', np.RankWarning)

def genOrder():
    numpy.random.choice(numpy.arange(1, 7), p=[0.1, 0.05, 0.05, 0.2, 0.4, 0.2])

def main(scenario):




if __name__ == '__main__':
    scenarioFileName = 'settings_scenario_hot_sku_store.yaml'
    settingsScenario = fm.loadSettingsFromYamlFile(scenarioFileName)
    main(settingsScenario)
