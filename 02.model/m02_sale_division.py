#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
==============================================
function
----------------------------------------------

         FILE: m02_sale_division.py

  DESCRIPTION: sku selection
       TARGET: select n sku satisfy the alpha * M order
       OPTIONS:
            inputs:
                # os_data: order and sale relationship data
                # sku_list: list of sku_id to be selected in the hot_sale_store
                # sku_label: the original partition of stores
        NOTES:
            # os_data is a DataFrame must contain the following columns:
                - sku_id
                - parent_ord_id
                - sales
                - store_id
            # sku_list: a list of sku_id contained by os_data.sku_id
            # sku_label: this version only support 2 original partitions of the sku
 REQUIREMENTS: ---
       AUTHOR: ---
      VERSION: 1.0
      CREATED: ---
       MODIFY: ---
=================================================
"""

from auxiliary import fileManagement as fm
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
import numpy as np
import scipy.stats

from scipy.optimize import curve_fit
from numpy import inf

import warnings
warnings.simplefilter('ignore', np.RankWarning)

CHINESE_FONT = FontProperties(fname = 'C:\Windows\\Fonts\\simsun.ttc')


def main(scenario):



if __name__ == '__main__':
    scenarioFileName = 'settings_scenario_hot_sku_store.yaml'
    settingsScenario = fm.loadSettingsFromYamlFile(scenarioFileName)
    main(settingsScenario)
