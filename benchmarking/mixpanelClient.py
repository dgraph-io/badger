#!/bin/python3

'''
This script does parsing of the data from YCSB benchmark, and pushes the same to mixpanel.
'''

from mixpanel import Mixpanel
from collections import OrderedDict
import time
import sys

# Use the below argv only to import previous data. default should be "0"
if len(sys.argv) > 1:
    day = int(sys.argv[1])
else:
    day = 0

mp = Mixpanel('8dd8a84584162683b416699a2d6649ce')
timestampVal = int((time.time() - (86400 * day)))

with open("./dashboard/js/data.js") as fh:
    for line in fh.readlines():
        if "ycsb" in line:
            currentDateData = line.strip().split(":")[1].split('\\n')[-(day+2)]
            mixPanelDataDict = OrderedDict()
            mixPanelDataDict["dataType"] = line.strip().split(":")[0]
            mixPanelDataDict["dateOfExecution"] = currentDateData.split(',')[0]
            mixPanelDataDict["ops/sec"] = currentDateData.split(',')[1]
            mixPanelData = line.strip().split(":")[0] + ' YCSB benchmarking ' + mixPanelDataDict["dateOfExecution"] + " " +  mixPanelDataDict["ops/sec"]
            print(mixPanelData,"---",  timestampVal)

            mp.import_data(api_secret="bdd412c66ea5ad66fbb15fd191c642bf",api_key="8dd8a84584162683b416699a2d6649ce",distinct_id=line.strip().split(":")[0], event_name='YCSB benchmarking', timestamp=int(timestampVal),  properties=mixPanelDataDict,meta = {"mp_processing_time_ms" : timestampVal})
print("-- Done --")
