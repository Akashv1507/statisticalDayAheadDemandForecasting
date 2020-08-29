import datetime as dt
from typing import List, Tuple
from src.fetchers.demandDataFetcher import fetchDemandDataFromApi

def minWiseDemandDataCreator(startDate:dt.datetime ,endDate: dt.datetime,configDict:dict):


    # tokenUrl: str = 'http://portal.wrldc.in/idserver/connect/token'
    # apiBaseUrl: str = 'http://portal.wrldc.in/dashboard'
    # clientId = 'wrldc_electron'
    # clientSecret = 'wrldc@123'

    # obj_scadaApiFetcher = ScadaApiFetcher(tokenUrl, apiBaseUrl, clientId, clientSecret)

    # measId = 'WRLDCMP.SCADA1.A0003297'
    # startDt = dt.datetime.now() - dt.timedelta(days=1)
    # endDt = startDt

    currDate = startDate
    while currDate <= endDate:
        demand_purity_dict = fetchDemandDataFromApi(currDate,configDict)
        # groupedDates = demand_purity_dict['data'].groupby("entityName")
        # for nameOfGroup, groupDf in groupedDates:
        #     print(nameOfGroup)
        #     print(len(groupDf.index))
        #     print(groupDf.head())

        
        currDate += dt.timedelta(days=1)
    