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

'''
from auxiliary import fileManagement as fm
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
'''
import numpy as np
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


def genFakeSale(scenario):
    '''
    :param scenario
    :return: np.array: fake order based on 2-8 law
    '''
    sku_id = [i for i in range(scenario['skuNum'])]    ## from 0
    selected_sku_id = sku_id[:int(scenario['skuNum']*0.2)]
    unselected_sku_id = sku_id[int(scenario['skuNum']*0.2):]
    fake_sku_id = selected_sku_id*16+unselected_sku_id
    return {'sku_id': np.array(sku_id), 'fake_sku_id': np.array(fake_sku_id), 'selected_sku_id': np.array(selected_sku_id), 'unselected_sku_id': np.array(unselected_sku_id)}


def genFakePartition(scenario):
    '''
    need genFakesale
    :param scenario:
    :return:
    '''
    selected_sku_id = genFakeSale(scenario)['selected_sku_id']
    unselected_sku_id = genFakeSale(scenario)['unselected_sku_id']
    sku_in_store_1 = np.append(selected_sku_id[:int(len(selected_sku_id) / 2)],
                               unselected_sku_id[:int(len(unselected_sku_id) / 2)])  ## small half to store 1
    sku_in_store_2 = np.setdiff1d(np.append(selected_sku_id, unselected_sku_id), sku_in_store_1, assume_unique=True)
    return { 'sku_in_store_1': sku_in_store_1, 'sku_in_store_2': sku_in_store_2 }


def genFakeOrder(scenario):
    '''
    need genFakesale
    :param scenario
    :return: fake order
    '''
    N = scenario['ordNum']
    M = scenario['skuNum']
    boldM = np.zeros((M, N), dtype=int)
    fake_sku_id = genFakeSale(scenario)['fake_sku_id']
    sku_num_in_ord = np.random.choice(scenario['skuNumInOrd'], size=N, p=scenario['skuNumInOrdDist'])

    for i in range(N):
        unique, counts = np.unique(np.random.choice(fake_sku_id,sku_num_in_ord[i],replace=True),return_counts=True)
        boldM[unique,i] = counts
    return boldM


def matrixDegenerate(boldM,selected_sku_id,sku_in_store_1):
    '''
    :param boldM:
    :param sku_selection: the left are unselected skus
    :param sku_in_store_1: the left skus are in store 2
    :return:
    '''
    sku_id = [i for i in range(boldM.shape[0])]  ## the row index is the sku_id
    unselected_sku_id = np.setdiff1d(np.array(sku_id), selected_sku_id, assume_unique=True)
    sku_in_store_2 = np.setdiff1d(np.array(sku_id), sku_in_store_1, assume_unique=True)
    unselected_store_1_sku_id = np.intersect1d(unselected_sku_id, sku_in_store_1, assume_unique=True)
    unselected_store_2_sku_id = np.intersect1d(unselected_sku_id, sku_in_store_2, assume_unique=True)
    selected_store_1_sku_id = np.intersect1d(selected_sku_id, sku_in_store_1, assume_unique=True)
    selected_store_2_sku_id = np.intersect1d(selected_sku_id, sku_in_store_2, assume_unique=True)

    ## calculate the Fake rows for order division
    unselected_store_1_result = boldM[unselected_store_1_sku_id,:].sum(axis=0, keepdims=True)
    unselected_store_2_result = boldM[unselected_store_2_sku_id,:].sum(axis=0, keepdims=True)
    selected_store_1_result = boldM[selected_store_1_sku_id,:].sum(axis=0, keepdims=True)
    selected_store_2_result = boldM[selected_store_2_sku_id,:].sum(axis=0, keepdims=True)

    deBoldM = np.concatenate((selected_store_1_result, selected_store_2_result, unselected_store_1_result, unselected_store_2_result), axis=0)
    deBoldM_rowname = ['selected_store_1_result','selected_store_2_result','unselected_store_1_result','unselected_store_2_result']

    return {'deBoldM_rowname':deBoldM_rowname, 'deBoldM':deBoldM}


def ordMark(deBoldM):
    '''
    :param deBoldM:
    :return: logical np.array
    '''
    cond1 = (deBoldM[2,:] > 0) & (deBoldM[3,:] > 0)
    cond2 = (deBoldM[0,:] == 0) & (deBoldM[1,:] > 0) & (deBoldM[2,:] == 0) & (deBoldM[3,:] > 0)
    cond3 = (deBoldM[0,:] > 0) & (deBoldM[1, :] == 0) & (deBoldM[2, :] > 0) & (deBoldM[3, :] == 0)
    false_y = np.logical_or(cond1,cond2,cond3)
    # false_y = cond1 | cond2 |cond3
    choice_y = np.logical_not(false_y)

    return choice_y


def countSkuNum(boldM, choice_y, selected_sku_id):
    '''
    :param BoldM:
    :param choice_y:
    :return: matrix of selected_sku_id*2, one row is selected sales another is total sales
    '''
    sale_in_hot = boldM[selected_sku_id,:][:,choice_y].sum(axis=1, keepdims=True)
    sale_in_ori = boldM[selected_sku_id,:].sum(axis=1, keepdims=True)

    return np.concatenate((sale_in_hot,sale_in_ori), axis=1)


def main(scenario):
    boldM = genFakeOrder(scenario)
    selected_sku_id = genFakeSale(scenario)['selected_sku_id']
    sku_in_store_1 = genFakePartition(scenario)['sku_in_store_1']

    deBoldMAll = matrixDegenerate(boldM,selected_sku_id, sku_in_store_1)
    deBoldM = deBoldMAll['deBoldM']
    # deBoldM_rowname = deBoldMAll['deBoldM_rowname']
    # print(deBoldM_rowname)
    choice_y = ordMark(deBoldM)
    result = countSkuNum(boldM,choice_y,selected_sku_id)


    np.savetxt("foo.csv", result, delimiter=",")

    pass


if __name__ == '__main__':
    scenarioFileName = 'settings_scenario_hot_sku_store.yaml'
    settingsScenario = loadSettingsFromYamlFile(scenarioFileName)
    '''
    scenario = settingsScenario
    '''
    main(settingsScenario)
