import datetime as dt
from typing import List, Tuple
from src.repos.blockwiseDemandInsertion.fetchMinwiseDemandRepo import MinwiseDemandFetchRepo
from src.repos.blockwiseDemandInsertion.insertStagingBlockwiseDemandRepo import StagingBlockWiseDemandInsertionRepo

def createStagingBlockWiseDemand(startDate:dt.datetime ,endDate: dt.datetime, configDict:dict, wantUpdation:bool)->bool:
    """ create staging blockwise demand data 

    Args:
        startDate (dt.datetime): start date
        endDate (dt.datetime): end date
        configDict (dict):   apllication configuration dictionary
        wantUpdation(bool): True if you want upsert False if you want insert

    Returns:
        bool: return true if insertion is success.
    """    

    conString:str = configDict['con_string_mis_warehouse']

    #creating instance of classes
    obj_minwiseDemandFetchRepo = MinwiseDemandFetchRepo(conString)
    obj_stagingBlockwiseDemandInsertionRepo = StagingBlockWiseDemandInsertionRepo(conString, wantUpdation)

    insertSuccessCount = 0
    
    currDate = startDate
    
    # Iterating through each day and inserting blockwise demand of all entities  
    while currDate <= endDate:
        data:List[Tuple] = obj_minwiseDemandFetchRepo.fetchMinwiseDemand(currDate,currDate)
        isInsertionSuccess = obj_stagingBlockwiseDemandInsertionRepo.insertStagingBlockWiseDemand(data)

        if isInsertionSuccess:
            insertSuccessCount = insertSuccessCount + 1
    
        currDate += dt.timedelta(days=1)
    
    numOfDays = (endDate-startDate).days

    #checking whether data is inserted for each day or not
    if insertSuccessCount == numOfDays +1:
        return True
    else:
        return False
    