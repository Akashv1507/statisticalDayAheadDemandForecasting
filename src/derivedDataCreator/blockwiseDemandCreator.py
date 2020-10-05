import datetime as dt
from typing import List, Tuple
from src.repos.blockwiseDemandInsertion.fetchMinwiseDemandRepo import MinwiseDemandFetchRepo
from src.repos.blockwiseDemandInsertion.insertBlockwiseDemandRepo import BlockWiseDemandInsertionRepo

def createBlockWiseDemand(startDate:dt.datetime, endDate: dt.datetime, configDict:dict)->bool:
    """ create blockwise demand data 

    Args:
        startDate (dt.datetime): start date
        endDate (dt.datetime): end date
        configDict (dict):   apllication configuration dictionary

    Returns:
        bool: return true if insertion is success.
    """    

    conString:str = configDict['con_string_mis_warehouse']

    #creating instance of classes
    obj_minwiseDemandFetchRepo = MinwiseDemandFetchRepo(conString)
    obj_blockwiseDemandInsertionRepo = BlockWiseDemandInsertionRepo(conString)

    insertSuccessCount = 0
    
    currDate = startDate
    
    # Iterating through each day and inserting blockwise demand of all entities  
    while currDate <= endDate:
        data:List[Tuple] = obj_minwiseDemandFetchRepo.fetchMinwiseDemand(currDate,currDate)
        isInsertionSuccess = obj_blockwiseDemandInsertionRepo.insertBlockWiseDemand(data)

        if isInsertionSuccess:
            insertSuccessCount = insertSuccessCount + 1
    
        currDate += dt.timedelta(days=1)
    
    numOfDays = (endDate-startDate).days

    #checking whether data is inserted for each day or not
    if insertSuccessCount == numOfDays +1:
        return True
    else:
        return False
    