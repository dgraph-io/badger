#!/bin/python3

'''
This script does parsing of the data from YCSB benchmark, and pushes the same to mixpanel.
'''

from mixpanel import Mixpanel
from collections import OrderedDict

mp = Mixpanel('8dd8a84584162683b416699a2d6649ce')

with open("./dashboard/js/data.js") as fh:
    for line in fh.readlines():
        if "ycsb" in line:
            currentDateData = line.strip().split(":")[1].split('\\n')[-2]
            mixPanelDataDict = OrderedDict()
            mixPanelDataDict["dataType"] = line.strip().split(":")[0]
            mixPanelDataDict["dateOfExecution"] = currentDateData.split(',')[0]
            mixPanelDataDict["ops/sec"] = currentDateData.split(',')[1]
            mixPanelData = line.strip().split(":")[0] + ' YCSB benchmarking data ' + mixPanelDataDict["dateOfExecution"] + " " +  mixPanelDataDict["ops/sec"]
            print(mixPanelData)

            mp.track(line.strip().split(":")[0], 'YCSB benchmarking data', mixPanelDataDict)
        
print("-- Done --")
