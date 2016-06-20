# -*- coding:utf-8 -*-
import sys
reload(sys)
sys.setdefaultencoding('utf8')

import csv
from elasticsearch import Elasticsearch
from elasticsearch import helpers

#es = Elasticsearch("114.212.84.249")
es = Elasticsearch("localhost")
csvfile = file('businfo.csv','rb')
reader = csv.reader(csvfile)

actions = []
count = 0
for line in reader:
    action ={
    "_index":"bus_info",
    "_type":"bus",
    "_id":line[0],
    "_source":{
        "XLMC":line[1],
        "ZDBH":line[2]}
    }
    count +=1
    actions.append(action)
    
    if count > 100:
        count = 0
        helpers.bulk(es,actions)
        del actions[0:len(actions)]

if (len(actions) > 0):
    helpers.bulk(es, actions)
    del actions[0:len(actions)]

print(u"成功导入公交车信息数据!")
csvfile.close()

csvfile = file('busline.csv','rb')
reader = csv.reader(csvfile)
actions = []
count = 0

for line in reader:
    action ={
    "_index":"bus_line",
    "_type":"bus",
    "_id":line[0],
    "_source":{
        "XLMC":line[1],
        "SXXL":line[2],
        "XXXL":line[3],
        }
    }
    count +=1
    actions.append(action)
    
    if count > 100:
        count = 0
        helpers.bulk(es,actions)
        del actions[0:len(actions)]

if (len(actions) > 0):
    helpers.bulk(es, actions)
    del actions[0:len(actions)]

print(u"成功导入公交线路信息数据!")
csvfile.close()



