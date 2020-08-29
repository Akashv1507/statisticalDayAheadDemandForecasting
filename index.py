import argparse
import datetime as dt
from src.appConfig import getAppConfigDict
from src.rawDataCreator.minWiseDemandDataCreator import minWiseDemandDataCreator

startDate = dt.datetime.strptime("2020-08-28", '%Y-%m-%d')
endDate = dt.datetime.strptime("2020-08-28", '%Y-%m-%d')

configDict=getAppConfigDict()
minWiseDemandDataCreator(startDate,endDate,configDict)

# import psycopg2

# conn = psycopg2.connect(database='testdb' , user='postgres', password='oracle123', host='127.0.0.1', port='5432')
# print ("Opened database successfully")

