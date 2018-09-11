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
import yaml
import platform


def loadSettingsFromYamlFile(fileName):
    """
    Load settings from a yaml file(json file) specified by the fileName and returns a dictionary with all settings
    """

    if platform.system() == 'Windows':
        scenarioSettingJSON_Win = '02.model/settings' + '\\' + fileName
        with open(scenarioSettingJSON_Win, 'r') as f:
            scenario = yaml.load(f)
    else:
        scenarioSettingsJSON_Linux = '02.model/settings' + '/' + fileName
        with open(scenarioSettingsJSON_Linux, 'r') as f:
            scenario = yaml.load(f)
    return scenario



def genFakesale(scenario):
    '''
    :param scenario
    :return: np.array: fake order based on 2-8 law
    '''
    sku_id = [i for i in range(scenario['skuNum'])]    ## from 0
    selected_sku_id = sku_id[:int(scenario['skuNum']*0.2)]
    unselected_sku_id = sku_id[int(scenario['skuNum']*0.2):]
    fake_sku_id = selected_sku_id*16+unselected_sku_id
    return {'fake_sku_id':np.array(fake_sku_id), 'selected_sku_id':selected_sku_id, 'unselected_sku_id':unselected_sku_id}


def genFakeOrder(scenario):
    '''
    need genFakesale
    :param scenario
    :return: fake order
    '''
    N = scenario['ordNum']
    M = scenario['skuNum']
    boldM = np.zeros((M, N), dtype=int)
    fake_sku_id = genFakesale(scenario)['fake_sku_id']
    sku_num_in_ord = np.random.choice(scenario['skuNumInOrd'], size=N, p=scenario['skuNumInOrdDist'])

    for i in range(N):
        unique, counts = np.unique(np.random.choice(fake_sku_id,sku_num_in_ord[i],replace=True),return_counts=True)
        boldM[unique,i] = counts
    return boldM


'''
a = np.array([0, 3, 0, 1, 0, 1, 2, 1, 0, 0, 0, 0, 1, 3, 4])
unique, counts = np.unique(a, return_counts=True)
dict(zip(unique, counts))
'''

def main(scenario):
    boldM = genFakeOrder(scenario)
    M = scenario['skuNum']
    store_label = genFakesale(scenario)['selected_sku_id']




if __name__ == '__main__':
    scenarioFileName = 'settings_scenario_hot_sku_store.yaml'
    settingsScenario = loadSettingsFromYamlFile(scenarioFileName)
    '''
    scenario = settingsScenario
    '''
    main(settingsScenario)
