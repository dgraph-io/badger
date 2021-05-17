#!/bin/python3

'''
This script does parsing of the data from YCSB benchmark, and pushes the same to mixpanel.
'''

from mixpanel import Mixpanel
from collections import OrderedDict
import time

mp = Mixpanel('8dd8a84584162683b416699a2d6649ce')
timestampVal = int(time.time())

with open("./dashboard/js/data.js") as fh:
    for line in fh.readlines():
        if "ycsb" in line:
            currentDateData = line.strip().split(":")[1].split('\\n')[-2]
            mixPanelDataDict = OrderedDict()
            mixPanelDataDict["dataType"] = line.strip().split(":")[0]
            mixPanelDataDict["dateOfExecution"] = currentDateData.split(',')[0]
            mixPanelDataDict["ops/sec"] = currentDateData.split(',')[1]
            mixPanelData = line.strip().split(":")[0] + ' YCSB benchmarking ' + mixPanelDataDict["dateOfExecution"] + " " +  mixPanelDataDict["ops/sec"]
            print(mixPanelData, timestampVal)

            mp.import_data(api_secret="bdd412c66ea5ad66fbb15fd191c642bf",api_key="8dd8a84584162683b416699a2d6649ce",distinct_id=line.strip().split(":")[0], event_name='YCSB benchmarking', timestamp=timestampVal,  properties=mixPanelDataDict)
print("-- Done --")
