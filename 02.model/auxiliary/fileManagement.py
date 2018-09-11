"""
Some useful functions to manage file on the server
"""

import errno
import subprocess
import pandas as pd
import time
import os
import shutil
import yaml
import platform


def archiveFile(path, fileName):
    """
    TimeStamp and archive the specified file
    """
    timestamp = time.strftime('%b-%d-%Y_%H%M', time.localtime())
    if(os.path.isfile(path + fileName)):
        newFileName = fileName + timestamp + '.h5'
        if(os.path.isfile(path + newFileName)):
            newFileName = fileName + '_2_' + timestamp + '.h5'
        shutil.copyfile(path + fileName, path + newFileName)
        print("File archived at: " + path + newFileName)
    else:
        print("No archive needed")
    return


def loadSettingsFromYamlFile(fileName):
    """
    Load settings from a yaml file(json file) specified by the fileName and returns a dictionary with all settings
    """

    if platform.system() == 'Windows':
        scenarioSettingJSON_Win = 'settings' + '\\' + fileName
        with open(scenarioSettingJSON_Win, 'r') as f:
            scenario = yaml.load(f)
    else:
        scenarioSettingsJSON_Linux = 'settings' + '/' + fileName
        with open(scenarioSettingsJSON_Linux, 'r') as f:
            scenario = yaml.load(f)
    return scenario



def getSQLConnection(fileName):
    """
    Load settings from a yaml file(json file) specified by the fileName and returns a dictionary with all settings
    """
    if platform.system() == 'Windows':
        sqlConnectionSettingsJSON_Win = 'settings' + '\\' + fileName
        with open(sqlConnectionSettingsJSON_Win, 'r') as f:
            sqlConnection = yaml.load(f)
    else:  # mac系统是Darwin
        sqlConnectionSettingsJSON_Linux = 'settings' + '/' + fileName
        with open(sqlConnectionSettingsJSON_Linux, 'r') as f:
            sqlConnection = yaml.load(f)
    return sqlConnection






def store_df_in_named_file(df, name, downloadLocally = False):
    """
    Stores Pandas DataFrame df in h5 file with name "name"
    """


    silentremove(ensure_suffix(name, '.h5'))
    with pd.HDFStore(ensure_suffix(name, '.h5'), complevel=9,
                     complib='blosc') as store:
        store['df'] = df


    """
    df.to_csv(name + ".txt", sep = '\t', encoding = 'utf-16')
    """

    if downloadLocally:
        print("Downloading " + str(ensure_suffix(name, '.h5')))
        subprocess.call(["sz",ensure_suffix(name, '.h5')])


def get_df_from_named_file(name, df_name='df'):
    """
    Gets Pandas DataFrame name from h5 file with name "df_name"
    """


    with pd.HDFStore(ensure_suffix(name, '.h5')) as store:
        return store[df_name]


    """
    df = pd.read_csv(name + ".txt", sep = '\t', encoding = 'utf-16')
    """


def silentremove(filename):
    """
    Silently removes file. Usually h5 file.
    """
    try:
        os.remove(filename)
    except OSError as e:
        if e.errno != errno.ENOENT: # errno.ENOENT = no such file or directory
            raise # re-raise exception if a different error occured


def ensure_suffix(string, suffix):
    """
    Ensures that a file name "string" has a suffix "suffix".
    If it doesn't have it, the function adds suffix to the file name "string"
    """
    if string.endswith(suffix):
        return string
    else:
        return string + suffix


def saveToCsv(df, outputPath, outputFileName, downloadLocally=False, sep=','):
    fullOutputPath = os.path.join(outputPath, outputFileName)
    df.to_csv(fullOutputPath, sep=sep, encoding='utf-8', index=False)
    print('| Save file to: %s' % fullOutputPath)
    print('|  - Number of records: %s' % len(df))
    if downloadLocally:
        print('|  - Download locally...')
        subprocess.call(["sz", fullOutputPath])
    print('')


def saveToH5(df, outputPath, outputFileName, downloadLocally=False, sep=','):
    fullOutputPath = os.path.join(outputPath, outputFileName)
    store_df_in_named_file(df, fullOutputPath)
    print('| Save file to: %s' % fullOutputPath)
    print('|  - Number of records: %s' % len(df))
    if downloadLocally:
        print('|  - Download locally...')
        subprocess.call(["sz", fullOutputPath])
    print('')


def saveToYaml(dict, outputPath, outputFileName, downloadLocally=False):
    with open(os.path.join(outputPath, outputFileName), 'w') as f:
        yaml.dump(dict, f, default_flow_style=False)


def readH5(filePath, dfName='df', nrows=None):
    print('| Read file from: %s' % filePath)
    if nrows:
        with pd.HDFStore(filePath) as store:
            df = store[dfName][:nrows]
    else:
        with pd.HDFStore(filePath) as store:
            df = store[dfName]
    print('|  - Number of records: %s' % len(df))
    return df


def download(filePath):
    print('| Download file...')
    print('| - from: %s' % filePath)
    subprocess.call(["sz", filePath])


def zipPath(path):
    dirName  = os.path.dirname(path)
    baseName = os.path.basename(path)
    print('| Zip path...')
    print('| - from: %s' % path)
    print('| - to:   %s' % path + '.zip')
    return shutil.make_archive(path, 'zip', dirName, baseName)

