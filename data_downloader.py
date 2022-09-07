"""
Copyright (C) NEXTracker, Inc - All Rights Reserved
Unauthorized copying of this file, via any medium is strictly prohibited
Proprietary and confidential
"""
__author__ = "hvaranasi"

import pytz
import datetime
import requests
import pandas
import warnings
import tqdm
import os
import loading_data_from_config_file as config
import processing_data_from_spc_raw_data as processing_data
import Connector as con
warnings.filterwarnings('ignore')


# processing all plants
def process_all_plants(dt_start, dt_end):
    # Iterate through all of the available plants
    plants_api_response = requests.get(api_server + "/v1/sites", headers=config.auth_header)
    if plants_api_response.status_code != 200:
        print("Error reading plants")
    for plant in plants_api_response.json():
        process_plant(plant, dt_start, dt_end)


# process multiple plants Example: 256,257,258
def process_plant_by_ids(plant_pids, dt_start, dt_end):

    for ID in plant_pids.split(","):
        plant_api_response = requests.get(api_server + "/v1/sites/" + str(ID), headers=config.auth_header)
        if plant_api_response.status_code != 200:
            print("Error reading plant id", ID)
            continue

        process_plant(plant_api_response.json(), dt_start, dt_end)


# Process a range of plants Example: 256:300
def process_plant_by_range(plant_pids, dt_start, dt_end):
    first_num = int(plant_pids.split(":")[0])
    second_num = int(plant_pids.split(":")[-1])
    for ID in range(first_num, second_num):
        plant_api_response = requests.get(api_server + "/v1/sites/" + str(ID), headers=config.auth_header)
        if plant_api_response.status_code != 200:
            print("Error reading plant id", ID)
            continue
        process_plant(plant_api_response.json(), dt_start, dt_end, )


# Capturing the Plant and related list of NCUs
def process_plant(plant, dt_start, dt_end):

    plant_pid = plant["site_id"]
    report_progress("Plant: " + str(plant_pid))
    if "placeholder" in plant.keys() and plant["placeholder"]:
        return

    # Reads all NCUs under plant_id and continues to a new plant if there is an error
    ncu_api_response = requests.get(api_server + "/v1/sites/" + str(plant_pid) + "/ncus?rt=t,_pagesize=10000",
                                    headers=config.auth_header)

    if ncu_api_response.status_code != 200:
        print("Error reading ncus for plant " + str(plant_pid))
        return

    # Iterates all the NCUs read, filtering out anyone not in use
    print('processing plant ... ' + plant["site_name"]+'... ' + str(plant_pid))
    print('*'*50)
    nid = []
    for ncu_df in ncu_api_response.json():
        if len(ncu_id) < 1:
            pass
        else:
            if ncu_df['nid'] not in [int(x) for x in ncu_id.split(',')]:
                continue
        nid.append(pandas.DataFrame([ncu_df]))

    if len(nid) > 0:
        nid_list = pandas.concat(nid)

        director(plant, nid_list, dt_start, dt_end)


# Capture the functions based on the (SPC< GHI and NCU) selection from Checkbox.
def director(plant, nid_list, dt_start, dt_end):

    chk_bool = con.main()[0]

    if chk_bool['ncu'] == 1:
        ncu_information(plant, nid_list, dt_start, dt_end)
    elif chk_bool['spc'] == 1:
        spc_information(plant, nid_list, dt_start, dt_end)
    elif chk_bool['ghi'] == 1:
        ghi_information(plant, nid_list, dt_start, dt_end)
    elif chk_bool['ncu'] == 1 and chk_bool['spc'] == 1:
        ncu_information(plant, nid_list, dt_start, dt_end)
        spc_information(plant, nid_list, dt_start, dt_end)


# Based on the checkbox selection resampling is done
def resample_result():
    '''
    Creating boolean for resampling the data based on 10 mins and 5 mins
    '''
    chk_res_bool = con.main()[1]
    if chk_res_bool['chk_15min'] == 1:
        res = '900s'
    elif chk_res_bool['chk_10min'] == 1:
        res = '600s'
    elif chk_res_bool['chk_5min'] == 1:
        res = '300s'
    else:
        res = '60s'

    return res


# Capturing GHI and NCU information
def ghi_information(plant, nid_list, dt_start, dt_end):
    print("Processing and Printing GHI Data.... ")

    for ncu_x in tqdm.tqdm(nid_list['nid']):
        ncu = nid_list.loc[nid_list['nid'] == ncu_x]

        if "serial_number" not in ncu:
            ncu['serial_number'] = 'n/a'

        if "name" in ncu and "vendor_asset_id" not in ncu:
            ncu['alt_name'] = ncu["name"]
        elif "name" not in ncu and "vendor_asset_id" in ncu:
            ncu['alt_name'] = ncu["vendor_asset_id"]
        elif "name" in ncu and "vendor_asset_id" in ncu:
            ncu['alt_name'] = ncu["name"]
        elif "name" not in ncu or 'vendor_asset_id' not in ncu:
            ncu['alt_name'] = 'n/a'

        ncu_serial_number = str(ncu['serial_number'][0])
        if ncu_serial_number == "0000000000000000":
            continue
        res = resample_result()

        # GHI data *************************************************
        ghi_field_to_analyze = ["time", "dfi", "ghi"]
        path = ["\\".join(os.getcwd().split("\\")[0:3])]
        try:
            ghi_data = get_ghi_data(plant, ncu_serial_number, ghi_field_to_analyze, dt_start, dt_end)
            if len(ghi_data) < 1:
                continue
        except TypeError:
            continue

        ghi_data = ghi_data.resample(res).mean()
        ghi_data['Site_time'] = ghi_data.index + datetime.timedelta(hours=int(ghi_data.index.strftime("%z")[0][:-2]))
        ghi_data['Site_time'] = ghi_data['Site_time'].dt.strftime("%Y-%m-%d %H:%M:%S")
        ghi_data['ncu_serial_number'] = ncu_serial_number
        ghi_df = ghi_data[['Site_time', 'dfi', 'ghi', 'ncu_serial_number']]
        ghi_title = plant["site_name"]+" - "+ncu_serial_number+"_ghi.csv"
        ghi_df.to_csv(os.path.join(path[0], "Desktop", ghi_title))

        # NCU Data Pulling ***********************************************
        ncu_field_to_analyze = ["time", "absws", "bc", "bs", "bv", "dir", "fl", "pv", "sh", "ss", "tz", "ws"]
        ncu_data = get_ncu_data(plant, ncu_serial_number, ncu_field_to_analyze, dt_start, dt_end)
        if ncu_data.empty:
            print("NCU data is empty, try with different dates...")
            continue
        ncu_data = ncu_data.resample(res).mean()
        ncu_data['Site_time'] = ncu_data.index + datetime.timedelta(hours=int(ncu_data.index.strftime("%z")[0][:-2]))
        ncu_data['Site_time'] = ncu_data['Site_time'].dt.strftime("%Y-%m-%d %H:%M:%S")
        ncu_title = plant["site_name"]+" - "+ncu_serial_number + ".csv"
        ncu_data.to_csv(os.path.join(path[0], "Desktop", ncu_title))


# Capturing detailed NCU information
def ncu_information(plant, nid_list, dt_start, dt_end):
    print("Processing and Printing NCUs Data.... ")

    for ncu_x in tqdm.tqdm(nid_list['nid']):
        ncu = nid_list.loc[nid_list['nid'] == ncu_x]

        if "serial_number" not in ncu:
            ncu['serial_number'] = 'n/a'
        if "name" in ncu and "vendor_asset_id" not in ncu:
            ncu['alt_name'] = ncu["name"]
        elif "name" not in ncu and "vendor_asset_id" in ncu:
            ncu['alt_name'] = ncu["vendor_asset_id"]
        elif "name" in ncu and "vendor_asset_id" in ncu:
            ncu['alt_name'] = ncu["name"]
        elif "name" not in ncu or 'vendor_asset_id' not in ncu:
            ncu['alt_name'] = 'n/a'

        ncu_serial_number = str(ncu['serial_number'][0])
        if ncu_serial_number == "0000000000000000":
            continue

        # NCU Data Pulling ***********************************************
        ncu_field_to_analyze = ["time", "absws", "bc", "bs", "bv", "dir", "fl", "pv", "sh", "ss", "tz", "ws"]
        ncu_data = get_ncu_data(plant, ncu_serial_number, ncu_field_to_analyze, dt_start, dt_end)

        if ncu_data.empty:
            print("NCU data is empty, try with different dates...")
            continue

        res = resample_result()

        ncu_data = ncu_data.resample(res).mean()
        ncu_data['Site_time'] = ncu_data.index + datetime.timedelta(hours=int(ncu_data.index.strftime("%z")[0][:-2]))
        ncu_data['Site_time'] = ncu_data['Site_time'].dt.strftime("%Y-%m-%d %H:%M:%S")
        path = ["\\".join(os.getcwd().split("\\")[0:3])]
        ncu_title = plant["site_name"]+" - "+ncu_serial_number + ".csv"
        ncu_data.to_csv(os.path.join(path[0], "Desktop", ncu_title))


# Capturing detailed SPC information
def spc_information(plant, ncu_details, dt_start, dt_end):

    for ncu_pid in ncu_details['nid']:
        ncu = ncu_details.loc[ncu_details['nid'] == ncu_pid]

        if ncu['serial_number'][0] == "0000000000000000":
            continue

        print('    processing SPC ... ' + "NCU_Name: " + str(ncu['serial_number'][0]) + " -- " + "NCU_ID =  "
              + str(ncu_pid) + "--" + "design_wind_speed = " +
              str(plant['design_wind_speed']) + "--" + " capacity_w = " + str(plant['capacity_w']))

        # Reads all NPCs under ncu_id and continues to a new NCU if there is an error
        spc_api_response = requests.get(api_server + "/v1/ncus/" + str(ncu_pid) + "/spcs?rt=t",
                                        headers=config.auth_header)
        if spc_api_response.status_code != 200:
            print("Error reading spcs for ncu " + str(ncu_pid))
            continue

        # Iterates all the SPCs read, filtering out anyone not in use
        spc_cnt = 0
        spc_cnt_tot = len(spc_api_response.json())
        for spc in tqdm.tqdm(spc_api_response.json()[1:]):
            # spc_sid = spc.get("sid", "NO SID *** VERY WEIRD")
            if "name" not in spc:
                spc['name'] = 1
            if "serial_number" not in spc:
                spc['serial_number'] = 'n/a'
            if "features" not in spc:
                spc['features'] = 'na'
            if "role" not in spc:
                spc['role'] = 'na'

            '''
            if (feature != "0000") & (role is not SPC)
            '''
            # if (filters.finding_sensors(spc)) or (spc['role'].find("SPC") == -1):
            #     continue
            spc_cnt += 1
            spc_serial_number = str(spc["serial_number"])

            ##########################################################################################################
            field_to_analyze = ['target', 'position', 'warning', 'faultHigh', 'faultLow', 'motorCurrent',
                                'maxMotorCurrent', 'batteryTemp', 'batteryVoltage', 'panelCurrent', 'panelVoltage',
                                'pcbTemp']


            ncu_serial_number = str(ncu['serial_number'])

            if len(spc_serial) < 1:
                pass
            else:
                if spc_serial_number not in spc_serial.split(','):
                    continue
            # if config.debugging == "true":
            #     print("      SPC: " + spc_serial_number + " -- SPC Name: " + str(spc['name']) + ", " + str(
            #         spc_cnt) + "/" + str(spc_cnt_tot) +" -- featuer#: "+ str(spc['features']))

            ''' Using the commented code below I can print the GHI Data, SPC Data and NCU Data for the selected
                Date range of a given plant.
            '''
            # SPC Data Pulling *************************************************
            data = get_data(plant, spc_serial_number, field_to_analyze, dt_start, dt_end)

            if data.empty:
                return
            spc_data = processing_data.process_data(data, plant, ncu, spc)
            spc_data['Site_time'] = spc_data['Site_time'].astype(str)

            # def var(row):
            #     x = row['pcbTemp']
            #     if x > 128:
            #         return x - 512
            #     return x
            # spc_data['pcb_Temp'] = spc_data.apply(var, axis=1)
            path = ["\\".join(os.getcwd().split("\\")[0:3])]
            spc_title = plant["site_name"]+" - "+spc_serial_number + "_SPC" + ".csv"
            spc_data.to_csv(os.path.join(path[0], "Desktop", spc_title))


# Function that pull last one year data
def get_data(plant, spc_serial_number, list_data, date_start, date_end):

    str_time_start = date_start.strftime("%Y-%m-%d %H:%M:%S")
    str_time_end = date_end.strftime("%Y-%m-%d %H:%M:%S")

    time_zone = plant['time_zone']

    url = URL.format(api_server, spc_serial_number, str_time_start, str_time_end)
    response = requests.get(url, headers=config.auth_header)
    if response.status_code != 200:
        print("Error reading time series data for spc " + spc_serial_number + ". " + response.reason)
        print("URL " + url)
        return {}

    data = {}
    value_series = {}
    time_series = []
    for name_data in list_data:
        value_series[name_data] = []

    json_response = response.json()
    if len(json_response['Results'][0]) > 0:
        results = json_response['Results'][0]
        if not 'Series' in results.keys() or results['Series'] is None or len(results['Series']) == 0:
            return pandas.DataFrame({})

        for el in results['Series'][0]['values']:
            try:
                time_series.append(pandas.to_datetime(el[0]).replace(tzinfo=pytz.timezone(time_zone)))
            except:
                time_series.append(pandas.to_datetime(el[0]).replace(tzinfo=pytz.timezone('UTC')))

            for name_data in list_data:
                value_series[name_data].append(el[list_data.index(name_data) + 1])

        for name_data in list_data:
            data[name_data] = pandas.Series(value_series[name_data], index=time_series)

    return pandas.DataFrame(data)


# Function to pull NCU data
def get_ncu_data(plant, ncu_serial_number, ncu_list_data, date_start, date_end):

    str_time_start = date_start.strftime("%Y-%m-%d %H:%M:%S")
    str_time_end = date_end.strftime("%Y-%m-%d %H:%M:%S")

    time_zone = plant['time_zone']

    # # Get all detailed information about the NCU using NCU Number
    # ncu_detail_api_response = requests.get(
    #     api_server + "/v1/ncus/" + str(ncu_serial_number) + "/reports?_from=" + str_time_start + "&_to="+ str_time_end + "&_columns=time,absws,ws" + "&_nullfiller=0",
    #     headers=config.auth_header)

    ncu_detail_api_response = requests.get(
        api_server + "/v1/ncus/" + str(ncu_serial_number) + "/reports?_from=" + str_time_start + "&_to="+ str_time_end+ "&_nullfiller=0",
        headers=config.auth_header)

    if ncu_detail_api_response.status_code != 200:
        print("Error reading ncus " + str(ncu_serial_number))
        return

    ncu_data = {}
    ncu_value_series = {}
    ncu_time_series = []
    for ncu_name_data in ncu_list_data:
        ncu_value_series[ncu_name_data] = []

    json_response = ncu_detail_api_response.json()
    if len(json_response['Results'][0]) > 0:
        results = json_response['Results'][0]

        if not 'Series' in results.keys() or results['Series'] is None or len(results['Series']) == 0:
            return pandas.DataFrame({})

        for ncu_el in results['Series'][0]['values']:
            ncu_time_series.append(pandas.to_datetime(ncu_el[0]).replace(tzinfo=pytz.timezone(time_zone)))

            for ncu_name_data in ncu_list_data:
                ncu_value_series[ncu_name_data].append(ncu_el[ncu_list_data.index(ncu_name_data)])

        for ncu_name_data in ncu_list_data:
            ncu_data[ncu_name_data] = pandas.Series(ncu_value_series[ncu_name_data], index=ncu_time_series)

    return pandas.DataFrame(ncu_data)


# Function to pull ghi data
def get_ghi_data(plant, ncu_serial_number, ncu_list_data, date_start, date_end):

    str_time_start = date_start.strftime("%Y-%m-%d %H:%M:%S")
    str_time_end = date_end.strftime("%Y-%m-%d %H:%M:%S")

    time_zone = plant['time_zone']

    # Get all detailed information about the NCU using NCU Number
    # ncu_detail_api_response = requests.get(
    #     api_server + "/v1/ncus/" + str(ncu_serial_number) + "/reports?_from=" + str_time_start + "&_to="+ str_time_end + "&_columns=time,absws,ws" + "&_nullfiller=0",
    #     headers=config.auth_header)


    ncu_detail_api_response = requests.get(
        api_server + "/v1/ncus/" + str(ncu_serial_number) + "/irradiance?_from=" + str_time_start + "&_to=" + str_time_end+ "&_nullfiller=0",
        headers=config.auth_header)

    if ncu_detail_api_response.status_code != 200:
        print("Error reading ncus " + str(ncu_serial_number))
        return

    ghi_data = {}
    ncu_value_series = {}
    ncu_time_series = []
    for ncu_name_data in ncu_list_data:
        ncu_value_series[ncu_name_data] = []

    json_response = ncu_detail_api_response.json()
    if len(json_response['Results'][0]) > 0:
        results = json_response['Results'][0]

        if not 'Series' in results.keys() or results['Series'] is None or len(results['Series']) == 0:
            return pandas.DataFrame({})

        for ncu_el in results['Series'][0]['values']:
            ncu_time_series.append(pandas.to_datetime(ncu_el[0]).replace(tzinfo=pytz.timezone(time_zone)))

            for ncu_name_data in ncu_list_data:
                ncu_value_series[ncu_name_data].append(ncu_el[ncu_list_data.index(ncu_name_data)])

        for ncu_name_data in ncu_list_data:
            ghi_data[ncu_name_data] = pandas.Series(ncu_value_series[ncu_name_data], index=ncu_time_series)

    return pandas.DataFrame(ghi_data)


# Function that pull last 15 days data
def get_15days_data(plant, spc_serial_number, list_data, date_start, date_end):
    """
    main method to get data from database and transform into dict of separated pandas.Series.
    :param spc_serial_number: str
    :param list_data: list(str), names of the keys/columns of data requested
    :param date_end: datetime in UTC
    :return: list(pandas.Series)
    """

    if date_end.strftime("%Y-%m-%d") == datetime.datetime.now().strftime("%Y-%m-%d"):
        str_time_end = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    else:
        str_time_end = date_end.strftime("%Y-%m-%d %H:%M:%S")

    str_time_start = (date_end - datetime.timedelta(days=15)).strftime("%Y-%m-%d %H:%M:%S")

    time_zone = plant['time_zone']

    url = URL.format(api_server, spc_serial_number, str_time_start, str_time_end)
    response = requests.get(url, headers=config.auth_header)
    if response.status_code != 200:
        print("Error reading time series data for spc " + spc_serial_number + ". " + response.reason)
        print("URL " + url)
        return {}

    data = {}
    value_series = {}
    time_series = []
    for name_data in list_data:
        value_series[name_data] = []

    json_response = response.json()
    if len(json_response['Results'][0]) > 0:
        results = json_response['Results'][0]

        if not 'Series' in results.keys() or results['Series'] is None or len(results['Series']) == 0:
            return pandas.DataFrame({})

        for el in results['Series'][0]['values']:
            time_series.append(
                pandas.to_datetime(el[0]).replace(tzinfo=pytz.timezone(time_zone)))

            for name_data in list_data:
                value_series[name_data].append(
                    el[list_data.index(name_data) + 1])

        for name_data in list_data:
            data[name_data] = pandas.Series(value_series[name_data], index=time_series)
    return pandas.DataFrame(data)


def report_progress(status):
    payload = {
        "running_status": status
    }
    url = "{}/v1/tasks/history".format(api_server)
    result = requests.put(
        url,
        headers={
            "Content-Type": "application/json",
            "Authorization": "Bearer " + api_bearer_token
        },
        json=payload
    )
    return result

# if __name__ == "__main__":
date_start, date_end, plant_id, ncu_id, spc_serial, api_server, api_bearer_token, URL = config.process_parameters()


# Processing the end results and saving them to the Excel output.
print("Start date: ", date_start, " / ", "End date: ", date_end)

if plant_id == "":
    process_all_plants(date_start, date_end)
elif plant_id.find(",") != -1 or plant_id is not None:
    process_plant_by_ids(plant_id, date_start, date_end)
elif plant_id.find(":") != -1:
    process_plant_by_range(plant_id, date_start, date_end)
