# coding: utf-8

import datetime


def printHelpMessage():
    return """
Opcoes:
  -h, --help                              Help info
  -t, --time                              synchronization time(seconds)"""


def printBasicInfo():
    return """
            Sync Cassandra and Elastic Search  |  rfunix

              Usage:  sync.py    [-t/--time   synchronization time(seconds) ]
      \n              Get basic options and Help, use: -h\--help
              """


def unix_time(dt):
    epoch = datetime.datetime.utcfromtimestamp(0)
    delta = dt - epoch
    return delta.total_seconds()


def unix_time_millis(dt):
    """TIMESTAMP UNIX"""
    return long(unix_time(dt) * 1000.0)


def returnUnixTimeNow():
    return long(unix_time(datetime.datetime.now()) * 1000.0)


def returnDataTableInsert(cas, es):
    dataTableInsert = {}
    dataTableInsert["cas"] = []
    dataTableInsert["es"] = []
    for dic in cas:
        filterData = filter(lambda x: x["id"] == str(dic["id"]), es)
        if len(filterData) > 0:
            if filterData[0]["row_time"] > unix_time_millis(dic["row_time"]):
                dataTableInsert["cas"].append(filterData[0])
            else:
                if filterData[0]["row_time"] < unix_time_millis(dic["row_time"]):
                    dataTableInsert["es"].append(filterData[0])
        else:
            dataTableInsert["es"].append(dic)
    for dic in es:
        filterData = filter(lambda x: str(x["id"]) == dic["id"], cas)
        if len(filterData) > 0:
            if unix_time_millis(filterData[0]["row_time"]) > dic["row_time"]:
                dataTableInsert["es"].append(filterData[0])
            else:
                if unix_time_millis(filterData[0]["row_time"]) < dic["row_time"]:
                    dataTableInsert["cas"].append(filterData[0])
        else:
            dataTableInsert["cas"].append(dic)
    return dataTableInsert
