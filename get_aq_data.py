import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from joblib import Memory

import datetime

from azure_table_interface import query_aq_data

# Set up caching for the Azure table access
memory = Memory('./_cache_')

ID_to_name = {'nesta-1': 'Priory Rd (South)',
              'nesta-2': 'Priory Rd (North)',
              'nesta-2-1': 'Priory Rd (North)',
              'nesta-4': 'Horseshoe Bridge',
              'nesta-5': 'Kent Rd',
              'nesta-6': 'Portswood Rd',
              'nesta-7': 'St Denys Rd',
              'nesta-8': 'Priory Rd-Kent Rd junction'}

TICKS_TWO_HOURLY = [datetime.time(hour, 0) for hour in range(0, 24, 2)]

@memory.cache
def get_sensor_data(sensor_id, **kwargs):
    res = query_aq_data(sensor_id,
                        **kwargs)
    
    res = res.resample('15min').mean()
    
    if len(res.columns) == 1:
        res.columns = [sensor_id.split("_")[1]]
    
    return res

def get_all_sensor_data(col='pm25'):
    sensor_ids = ['aq-deployment_nesta-' + s for s in ['1', '2', '2-1', '4', '5', '6', '7', '8']]
    
    dfs = [get_sensor_data(sensor_id, from_date='2019-01-01', cols=[col]) for sensor_id in sensor_ids]
    
    data = pd.concat(dfs, axis=1)
    
    # Deal with nesta-2 and nesta-2-1 sensors which are in the same location, but -2-1 was a replacement
    # after -2 failed. Just combine the two columns as one
    data['nesta-2'] = data['nesta-2'].combine_first(data['nesta-2-1'])
    del data['nesta-2-1']

    data.loc['2019-06-07','nesta-1'] = np.nan
    
    hourly_mean = data.resample('1H').mean()
    daily_mean = data.resample('1D').mean()
    
    return data, hourly_mean, daily_mean

def get_flo_data(col='pm25'):
    col_to_select = col + "_mean"

    flo_data = pd.read_csv('../Data/BS Sensors/20190307 to 20190823_15min averages_StDenys_6sensors.csv',
                           parse_dates=[2], dayfirst=True)
    flo_data = flo_data.drop('Unnamed: 0', axis=1)
    flo_data = flo_data[['site', 'date', col_to_select]]

    flo_data = flo_data.pivot(columns='site', index='date', values='pm25_mean')

    flo_data.columns = [col.lower() for col in flo_data.columns]
    flo_data.index.name = 'timestamp'

    flo_data['nesta-2'] = flo_data['nesta-2'].combine_first(flo_data['nesta-2-1'])
    del flo_data['nesta-2-1']

    flo_data.loc['2019-06-07','nesta-1'] = np.nan

    hourly_mean = flo_data.resample('1H').mean()
    daily_mean = flo_data.resample('1D').mean()
    
    return flo_data, hourly_mean, daily_mean