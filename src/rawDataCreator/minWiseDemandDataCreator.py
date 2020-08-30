import datetime as dt
from typing import List, Tuple
from src.fetchers.demandDataFetcher import fetchDemandDataFromApi
from src.repos.purityPercentageInsertionRepo import PurityPercentageInsertionRepo
from src.repos.minwiseDemandInsertionRepo import MinWiseDemandInsertionRepo

def createMinWiseDemand_purityPercent(startDate:dt.datetime ,endDate: dt.datetime,configDict:dict):
    
    conString:str = configDict['con_string_mis_warehouse']
    obj_purityPercentageInsRepo = PurityPercentageInsertionRepo(conString)
    obj_minWiseInsertionRepo = MinWiseDemandInsertionRepo(conString)

    currDate = startDate
    while currDate <= endDate:
        demand_purity_dict = fetchDemandDataFromApi(currDate,configDict)
        isInsertionSuccessPurity = obj_purityPercentageInsRepo.insertPurityPercentage(demand_purity_dict['purityPercentage'])
        print(isInsertionSuccessPurity)
        isInsertionSuccessMinWiseDemand = obj_minWiseInsertionRepo.insertMinWiseDemand(demand_purity_dict['data'])
        print(isInsertionSuccessMinWiseDemand)

        
        
        currDate += dt.timedelta(days=1)
    