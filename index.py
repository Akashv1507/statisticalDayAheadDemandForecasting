import argparse
import datetime as dt
from src.appConfig import getAppConfigDict
from src.rawDataCreator.minWiseDemandDataCreator import createMinWiseDemand_purityPercent
from src.rawDataCreator.entityMappingTableCreator import createEntityMappingTable

startDate = dt.datetime.strptime("2020-08-29", '%Y-%m-%d')
endDate = dt.datetime.strptime("2020-08-29", '%Y-%m-%d')

configDict=getAppConfigDict()
createMinWiseDemand_purityPercent(startDate,endDate,configDict)

# createEntityMappingTable(configDict)

# import psycopg2

# conn = psycopg2.connect(database='testdb' , user='postgres', password='oracle123', host='127.0.0.1', port='5432')
# print ("Opened database successfully")

