# coding: utf-8
import traceback
import sched
import time
from Infra.Data.cassandaRep import returnSyncTables, cassReturnTableUpdate, insertCassandra, cassDelete, updateLastSync
from Infra.Data.esRep import esReturnTableUpdate, insertEs
from Infra.CrossCutting.functions import returnDataTableInsert

s = sched.scheduler(time.time, time.sleep)
run = False


def execute(timeSync):
    """
    Inicia sync, agendamento realizado atraves do parametro timeSync
    """
    while True:
        runSched(timeSync)
    # Syncdb()


def runSched(timeSync):
    global run
    if not run:
        run = True
    else:
        print "+++++++++++++++ Daemon running +++++++++++++++!"
        return

    s.enter(timeSync, 1, Syncdb, ())
    s.run()


def Syncdb():
    print "+++++++++++++++ Start Sync +++++++++++++++"
    global run
    for table in returnSyncTables():
        try:
            updateLastSync(table.id)
            cassDataTable = cassReturnTableUpdate(
                table.name_table, table.last_sync)
            esDataTable = esReturnTableUpdate(
                table.name_table, table.last_sync)
            dataTableInsert = returnDataTableInsert(cassDataTable, esDataTable)
            updateData(dataTableInsert, table.name_table)
            run = False
        except Exception, e:
            traceback.print_exc()
    print "+++++++++++++++ End Sync +++++++++++++++"


def updateData(data, tableName):
    for dic in data["cas"]:
        try:
            cassDelete(tableName, dic["id"])
            insertCassandra(tableName, dic)
            print "+ Insert success cassandra table -> {0} id -> {1}".format(tableName, dic["id"])
        except Exception, e:
            traceback.print_exc()

    for dic in data["es"]:
        try:
            insertEs(tableName, dic)
            print "+ Insert success ElasticSearch table -> {0} id -> {1}".format(tableName, dic["id"])
        except Exception, e:
            traceback.print_exc()
