# coding:utf-8
from cassandra.cluster import Cluster
from Infra.CrossCutting.functions import unix_time_millis
from cassandra.query import dict_factory
import re
from datetime import datetime


def getCluster():
    return Cluster(["localhost"])


def returnSyncTables():
    cluster = getCluster()
    session = cluster.connect('syncdb')
    row = session.execute(
        "select * from synctables WHERE active_sync = True ALLOW FILTERING;")
    return row


def cassReturnTableUpdate(tableName, tableUpdateTime):
    """
    Utilizar ALLOW FILTERING para permitir filtragem (Cassandra bloqueia por default, para maior desempenho)
    """
    cluster = getCluster()
    query = "select * from {0} where row_time >= {1} ALLOW FILTERING;".format(
        tableName, unix_time_millis(tableUpdateTime))
    session = cluster.connect('syncdb')
    session.row_factory = dict_factory
    row = session.execute(query)
    return row


def cassDelete(tableName, id):
    cluster = getCluster()
    query = "DELETE from {0} where id = {1};".format(tableName, id)
    session = cluster.connect('syncdb')
    session.execute(query)


def insertCassandra(tableName, data):
    cluster = getCluster()
    session = cluster.connect('syncdb')
    query = "INSERT INTO {0} (".format(tableName)
    for key, value in data.iteritems():
        query = "{0} {1},".format(query, key)
    query = re.sub(r",(?=$)", ")", query)
    query = "{0} VALUES (".format(query)
    for key, value in data.iteritems():
        if key == 'row_time':
            if type(value) <> int:
                value = unix_time_millis(value)
        if type(value) == unicode or type(value) == str:
            if not re.search(r'[\w\d]{8}-[\w\d]{4}-[\w\d]{4}-[\w\d]{4}-[\w\d]{12}', str(value)):
                value = "'{0}'".format(value)
        query = "{0} {1},".format(query, value)
    query = re.sub(r",(?=$)", ")", query)
    print query
    session.execute(query)


def updateLastSync(id):
    cluster = getCluster()
    query = "UPDATE synctables SET last_sync = dateof(now()) where id = {0};".format(
        id)
    session = cluster.connect('syncdb')
    session.execute(query)
