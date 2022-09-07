"""
Copyright (C) NEXTracker, Inc - All Rights Reserved
Unauthorized copying of s file, via any medium is strictly prohibited
Proprietary and confidential
"""
__author__ = "hvaranasi"


# Function filters the "NCUs" based on the values mentioned below.
def filter_out_ncu(ncu):
    return not ncu.get("in_use", False) or ncu.get("type", "P4Q") == "P4Q"


def finding_sensors(spc):
    spc_feature = spc['features']
    sensor_flag = sensors_list(spc_feature, sensors_set)

    try:
        return (spc.get('in_use') == False) or (spc.get('in_use') == True and len(sensor_flag) > 0) or\
               (spc.get('in_use') == False and len(sensor_flag) > 0) or (spc.get("type", "P4Q") == "P4Q")
    except TypeError:
        return


def sensors_list(value, flag_sensor_set):
    try:
        dec_value = int(value)
    except ValueError:
        return
    return set([sensor for sensor in flag_sensor_set if dec_value & sensor[1]])


# # Returns true if this SPC is associated with a tracker
# func IsTracker(spc *model.SPC) bool {​​​​​​​​
#    return IsTrackerFeatures(spc.Features)
# }​​​​​​​​

# spc features attribute
SPC_WIND_SENSOR_FEATURE_MASK = ("SPC_WIND_SENSOR_FEATURE_MASK", 0x0001)
SPC_SNOW_SENSOR_FEATURE_MASK = ("SPC_SNOW_SENSOR_FEATURE_MASK", 0x0002)
# SPC_CLEANING_FEATURE_MASK = ("SPC_CLEANING_FEATURE_MASK", 0x0004)  # Tracker is in cleaning mode
SPC_FLOOD_SENSOR_FEATURE_MASK = ("SPC_FLOOD_SENSOR_FEATURE_MASK", 0x0010)
# SPC_UPGRADE_FLAG_FEATURE_MASK = ("SPC_UPGRADE_FLAG_FEATURE_MASK", 0x0020)
SPC_GHI_SENSOR_FEATURE_MASK = ("SPC_GHI_SENSOR_FEATURE_MASK", 0x0040)
# SPC_HVPS_FEATURE_MASK = ("SPC_HVPS_FEATURE_MASK", 0x0080)  # Tracker is powered by HVPS

'''
if any of these four Flags appear it is an non tracker else it is a tracker.
'''
# SPC_NOT_TRACKER_FEATURE_MASK = SPC_WIND_SENSOR_FEATURE_MASK | SPC_SNOW_SENSOR_FEATURE_MASK |\
#                                SPC_FLOOD_SENSOR_FEATURE_MASK | SPC_GHI_SENSOR_FEATURE_MASK

sensors_set = {SPC_WIND_SENSOR_FEATURE_MASK,
               SPC_SNOW_SENSOR_FEATURE_MASK,
               # SPC_CLEANING_FEATURE_MASK,
               SPC_FLOOD_SENSOR_FEATURE_MASK,
               # SPC_UPGRADE_FLAG_FEATURE_MASK,
               SPC_GHI_SENSOR_FEATURE_MASK,
               # SPC_HVPS_FEATURE_MASK
               }
