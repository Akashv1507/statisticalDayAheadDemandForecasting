import datetime as dt
from typing import List, Tuple
from src.fetchers.demandDataFetcher import fetchDemandDataFromApi
from src.repos.purityPercentageInsertionRepo import PurityPercentageInsertionRepo
from src.repos.minwiseDemandInsertionRepo import MinWiseDemandInsertionRepo

def createMinWiseDemand_purityPercent(startDate:dt.datetime ,endDate: dt.datetime,configDict:dict)->bool:
    """ create minwise demand data and daywise purity percentage of each entity

    Args:
        startDate (dt.datetime): start date
        endDate (dt.datetime): end date
        configDict (dict):   apllication configuration dictionary

    Returns:
        bool: return true if insertion is success.
    """    

    
    conString:str = configDict['con_string_mis_warehouse']

    #creating instance of classes
    obj_purityPercentageInsRepo = PurityPercentageInsertionRepo(conString)
    obj_minWiseInsertionRepo = MinWiseDemandInsertionRepo(conString)

    puritySuccessCount = 0
    demandSuccessCount = 0
    currDate = startDate
    
    # Iterating through each day and inserting demand and purity percentage 
    while currDate <= endDate:
        demand_purity_dict = fetchDemandDataFromApi(currDate,configDict)

        isInsertionSuccessPurity = obj_purityPercentageInsRepo.insertPurityPercentage(demand_purity_dict['purityPercentage'])
        if isInsertionSuccessPurity:
            puritySuccessCount = puritySuccessCount + 1

        isInsertionSuccessMinWiseDemand = obj_minWiseInsertionRepo.insertMinWiseDemand(demand_purity_dict['data'])
        if isInsertionSuccessMinWiseDemand:
            demandSuccessCount = demandSuccessCount + 1
    
        currDate += dt.timedelta(days=1)
    
    numOfDays = (endDate-startDate).days

    #checking whether data is inserted for each day or not
    if puritySuccessCount == numOfDays +1 and demandSuccessCount == numOfDays +1:
        return True
    else:
        return False
    
    