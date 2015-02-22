# coding: utf-8

import urllib2
import json
from Infra.CrossCutting.functions import unix_time_millis
import re


def esReturnTableUpdate(tableName, tableUpdateTime):
    listReturn = []
    data = {
        "query": {
            "filtered": {
                "filter": {
                    "range": {"row_time": {"gte": unix_time_millis(tableUpdateTime)}}
                }
            }
        }
    }

    url = "http://localhost:9200/syncdb/{0}/_search".format(tableName)
    data = json.dumps(data)
    req = urllib2.Request(url, data,)
    out = urllib2.urlopen(req)
    data = out.read()
    data = json.loads(data)
    for dados in data['hits']['hits']:
        dataReturn = {}
        dataReturn['id'] = dados['_id']
        for key, value in dados['_source'].iteritems():
            dataReturn[key] = value
        listReturn.append(dataReturn)
    return listReturn


def insertEs(tableName, dataTable):
    data = {}
    for key, value in dataTable.iteritems():
        if key == "id":
            url = "http://localhost:9200/syncdb/{0}/{1}".format(
                tableName, value)
        else:
            if key == "row_time":
                if type(value) <> int:
                    value = unix_time_millis(value)
            if type(value) == unicode or type(value) == str:
                if not re.search(r'[\w\d]{8}-[\w\d]{4}-[\w\d]{4}-[\w\d]{4}-[\w\d]{12}', str(value)):
                    value = "'{0}'".format(value)
            data[str(key)] = value
    data = json.dumps(data)
    req = urllib2.Request(url, data,)
    urllib2.urlopen(req)
