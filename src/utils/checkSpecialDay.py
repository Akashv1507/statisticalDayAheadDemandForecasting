import datetime as dt
from src.utils.addSpecialDay import specialDayList

def checkSpecialDay(dateKey:dt.datetime)->bool:

    boolResult= dateKey in specialDayList
    return boolResult
    