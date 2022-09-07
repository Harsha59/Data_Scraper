"""
Copyright (C) NEXTracker, Inc - All Rights Reserved
Unauthorized copying of this file, via any medium is strictly prohibited
Proprietary and confidential
"""
__author__ = "hvaranasi"

'''
This module act as an Action Listener for the Checkboxes.
'''

def chk_bool(ncu_chk, spc_chk, ghi_chk):
    ghi.append(ghi_chk)
    ncu.append(ncu_chk)
    spc.append(spc_chk)


def chk_resample_bool(Chk15min, Chk10min, Chk5min):
    chk_15min.append(Chk15min)
    chk_10min.append(Chk10min)
    chk_5min.append(Chk5min)


def main():
    dict1 = {"ncu": ncu[0], "spc": spc[0], "ghi": ghi[0]}
    dict2 = {"chk_15min": chk_15min[0], "chk_10min": chk_10min[0], "chk_5min": chk_5min[0]}
    return dict1, dict2


ncu = []
spc = []
ghi = []
chk_15min = []
chk_10min = []
chk_5min = []