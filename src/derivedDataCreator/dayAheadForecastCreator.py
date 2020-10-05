import datetime as dt
from typing import List, Tuple, TypedDict
from src.repos.dayAheadForecast.fetchDemandForAlgoRepo import DemandFetchForAlgoRepo
from src.repos.dayAheadForecast.dayAheadForecastInsertionRepo import DayAheadDemandForecastInsertion
from src.repos.dayAheadForecast.r0RevisionInsertionRepo import R0RevisionInsertion
from src.typeDefs.dayaheadForecast import IResultDict

def createDayAheadForecast(startDate:dt.datetime ,endDate: dt.datetime, configDict:dict)->bool:
    """ create  blockwise day ahead forecasted demand data and r0 forecast 

    Args:
        startDate (dt.datetime): start date
        endDate (dt.datetime): end date
        configDict (dict):   application configuration dictionary

    Returns:
        bool: return true if insertion is success.
    """    

    
    conString:str = configDict['con_string_mis_warehouse']
    demandInsertionSuccessCount = 0
    r0InsertionSuccessCount = 0

    #creating instance of classes
    obj_demandFetchForAlgo = DemandFetchForAlgoRepo(conString)
    obj_dayAheadDemandForecastInsertion = DayAheadDemandForecastInsertion(conString)
    obj_r0ForecastInsertion = R0RevisionInsertion(conString)
    
    currDate = startDate
    
    # Iterating through each day and inserting day ahead forecasted demand of all entities  
    while currDate <= endDate:

        resultDict : IResultDict = obj_demandFetchForAlgo.fetchBlockwiseDemandForAlgo(currDate)
        # print(data)
        isForecastedDemandInsSuccess = obj_dayAheadDemandForecastInsertion.insertDayAheadDemandForecast(resultDict['demandData'])
        if isForecastedDemandInsSuccess:
            demandInsertionSuccessCount = demandInsertionSuccessCount + 1
        
        isR0InsertionSuccess = obj_r0ForecastInsertion.insertR0DemandForecast(resultDict['r0Data'])
        if isR0InsertionSuccess:
            r0InsertionSuccessCount += 1

        currDate += dt.timedelta(days=1)
    
    numOfDays = (endDate-startDate).days

    #checking whether data is inserted for each day or not
    if demandInsertionSuccessCount == numOfDays +1 and r0InsertionSuccessCount == numOfDays +1:
        return True
    else:
        return False
    