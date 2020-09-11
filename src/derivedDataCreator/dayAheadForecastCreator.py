import datetime as dt
from typing import List, Tuple
from src.repos.dayAheadForecast.fetchDemandForAlgoRepo import DemandFetchForAlgoRepo
from src.repos.dayAheadForecast.dayAheadForecastInsertionRepo import DayAheadDemandForecastInsertion


def createDayAheadForecast(startDate:dt.datetime ,endDate: dt.datetime, configDict:dict)->bool:
    """ create  blockwise day ahead forecasted demand data 

    Args:
        startDate (dt.datetime): start date
        endDate (dt.datetime): end date
        configDict (dict):   application configuration dictionary

    Returns:
        bool: return true if insertion is success.
    """    

    
    conString:str = configDict['con_string_mis_warehouse']
    insertSuccessCount = 0

    #creating instance of classes
    obj_demandFetchForAlgo = DemandFetchForAlgoRepo(conString)
    obj_dayAheadDemandForecastInsertion = DayAheadDemandForecastInsertion(conString)
    
    currDate = startDate
    
    # Iterating through each day and inserting day ahead forecasted demand of all entities  
    while currDate <= endDate:

        data:List[Tuple] = obj_demandFetchForAlgo.fetchBlockwiseDemandForAlgo(currDate)
        # print(data)
        isInsertionSuccess = obj_dayAheadDemandForecastInsertion.insertDayAheadDemandForecast(data)
        if isInsertionSuccess:
            insertSuccessCount = insertSuccessCount + 1
        currDate += dt.timedelta(days=1)
    
    numOfDays = (endDate-startDate).days

    #checking whether data is inserted for each day or not
    if insertSuccessCount == numOfDays +1:
        return True
    else:
        return False
    