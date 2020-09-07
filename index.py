import argparse
import datetime as dt
from src.appConfig import getAppConfigDict
from src.rawDataCreator.minWiseDemand_purtiyPercentCreator import createMinWiseDemand_purityPercent
from src.rawDataCreator.entityMappingTableCreator import createEntityMappingTable
from src.derivedDataCreator.blockwiseDemandCreator import createBlockWiseDemand

startDate = dt.datetime.strptime("2020-08-30", '%Y-%m-%d')
endDate = dt.datetime.strptime("2020-08-31", '%Y-%m-%d')

configDict=getAppConfigDict()
# print(createMinWiseDemand_purityPercent(startDate,endDate,configDict))

# createEntityMappingTable(configDict)
# print(createBlockWiseDemand(startDate,endDate,configDict))

# import psycopg2

# conn = psycopg2.connect(database='testdb' , user='postgres', password='oracle123', host='127.0.0.1', port='5432')
# print ("Opened database successfully")

