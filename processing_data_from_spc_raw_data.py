"""
Copyright (C) NEXTracker, Inc - All Rights Reserved
Unauthorized copying of s file, via any medium is strictly prohibited
Proprietary and confidential
"""
__author__ = "hvaranasi"

import numpy as np
import datetime
import loading_data_from_config_file as config
from IPython import core


def process_data(data, plant, ncu, spc):

    try:
        if data.empty or len(data) < 1:
            return
    except AttributeError:
        return

    data['Site_time'] = data.index + datetime.timedelta(hours=int(data.index.strftime("%z")[0][:-2]))
    data['Site_time'] = data['Site_time'].dt.strftime("%Y-%m-%d %H:%M:%S")
    data['position'] = np.rad2deg(data['position'])
    data['target'] = np.rad2deg(data['target'])
    data['deviation'] = abs(data['target'] - data['position'])
    data.loc[data['deviation'].ge(2), 'angle_diff_greater_than2deg'] = 1
    data['angle_diff_greater_than2deg'] = data['angle_diff_greater_than2deg'].fillna(0)
    data.loc[data['faultHigh'] == '100000', 'tracker_stall_status'] = '1'
    data.loc[data['faultHigh'] != '100000', 'tracker_stall_status'] = '0'
    data['tracker_stall_status'] = data['tracker_stall_status'].fillna('0')
    data['tracker_stall_status'] = data['tracker_stall_status'].astype(int)
    data['night_stow'] = data['warning'].apply(lambda i: "{:16d}".format(int(i))[5])
    data['low_battery'] = data['warning'].apply(lambda i: "{:16d}".format(int(i))[11])
    data['wind_stow'] = data['warning'].apply(lambda i: "{:16d}".format(int(i))[4])
    data['flood_stow'] = data['warning'].apply(lambda i: "{:16d}".format(int(i))[2])
    data['snow_stow'] = data['warning'].apply(lambda i: "{:16d}".format(int(i))[1])

    features = data.columns
    if ('maxMotorCurrent' in features) & ('motorCurrent' in features):
        mean1 = data['maxMotorCurrent'].mean()
        mean2 = data['motorCurrent'].mean()
        if np.isnan(mean1):
            data['MC'] = data['motorCurrent']
        elif np.isnan(mean2):
            data['MC'] = data['maxMotorCurrent']
        else:
            if mean1 > mean2:
                data['MC'] = data['maxMotorCurrent']
            else:
                data['MC'] = data['motorCurrent']
        data.drop(['maxMotorCurrent', 'motorCurrent'], axis=1, inplace=True)
    elif ('maxMotorCurrent' in features) & ('motorCurrent' not in features):
        data['MC'] = data['maxMotorCurrent']
        data.drop(['maxMotorCurrent'], axis=1, inplace=True)
    elif ('maxMotorCurrent' not in features) & ('motorCurrent' in features):
        data['MC'] = data['motorCurrent']
        data.drop(['motorCurrent'], axis=1, inplace=True)
    else:
        print('No motor current data for analysis')

    return data
