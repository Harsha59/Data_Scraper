"""
Copyright (C) NEXTracker, Inc - All Rights Reserved
Unauthorized copying of this file, via any medium is strictly prohibited
Proprietary and confidential
"""
__author__ = "hvaranasi"

import datetime
import os
import configparser

Config = configparser.ConfigParser()
file = os.path.join(os.getcwd(), "config.ini")
Config.read(file)


# Capturing the "Sections" and its "Values" mentioned in the config.ini file.
def config_selection_mapping(section):
    sections_data = {}
    options = Config.options(section)
    for value in options:
        try:
            sections_data[value] = Config.get(section, value)
            if sections_data[value] == -1:
                print("skip: %s" % value)
        except:
            print(value)
            sections_data[value] = None
    return sections_data


# Final processing of variables before they were called from the Root Cause Analysis.
def process_parameters():

    # SECTION 1:
    from_date = config_selection_mapping("Date")['from_date']
    to_date = config_selection_mapping("Date")['to_date']

    # SECTION 2:auth_header
    site_id = config_selection_mapping("PLANT_DETAILS")['site_id']

    # SECTION 3:
    nid = config_selection_mapping("NCU_SPC_Details")['nid']
    spc = config_selection_mapping("NCU_SPC_Details")['spc']

    # SECTION 4:
    api_server = config_selection_mapping("SERVER_DETAILS")['api server']
    bearer_token = config_selection_mapping("SERVER_DETAILS")['bearer token']

    # # SECTION 5:
    # url = config_selection_mapping("URL")["url"]

    if to_date == "":
        dt_end = datetime.datetime.now()
    else:
        dt_end = datetime.datetime.strptime(to_date, '%Y-%m-%dT%H:%M:%SZ')

    if from_date == "":
        dt_start = dt_end - datetime.timedelta(days=10)
    else:
        dt_start = datetime.datetime.strptime(from_date, '%Y-%m-%dT%H:%M:%SZ')

    # # Makes sure dates are UTC
    # site_timezone = pytz.timezone('UTC')
    # dt_start = site_timezone.localize(dt_start)
    # dt_end = site_timezone.localize(dt_end)

    # Simple validation
    if dt_start > dt_end:
        print("Error, initial date is after final day")
        exit(1)

    url = "{}/v1/spcs/{}/reports?_from={}&_to={}&_columns=target,position,warning,faultHigh,faultLow,motorCurrent,maxMotorCurrent,batteryTemp,batteryVoltage,panelCurrent,panelVoltage,pcbTemp&_nullfiller=0"
    return dt_start, dt_end, site_id, nid, spc, api_server, bearer_token, url


date_start, date_end, site_id, nid, spc, api_server, api_bearer_token, URL = process_parameters()

if api_bearer_token == "":
    print("Error, an authentication token must be provided by setting the BEARER_TOKEN environment variable."
          " If you don't have one, get a valid token from the software team.")
    exit(1)
auth_header = {"Authorization": "Bearer " + api_bearer_token}
debugging = os.getenv('DEBUGGING', "true")


