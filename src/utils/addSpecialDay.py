import datetime as dt

specialDayList=[]
independenceDay = dt.datetime.strptime("2020-08-15", '%Y-%m-%d')
republicDay = dt.datetime.strptime("2020-01-26", '%Y-%m-%d')
dummySpecial1 = dt.datetime.strptime("2020-08-26",'%Y-%m-%d')
dummySpecial2 = dt.datetime.strptime("2020-08-24",'%Y-%m-%d')

specialDayList =[independenceDay,republicDay , dummySpecial1,dummySpecial2]

def addSpecialDay( SpecialDate: dt.datetime)->None:
    """this function will add special day to database

    Args:
        SpecialDate (dt.datetime): special day
    """    
    
    specialDayList.append(SpecialDate)
    