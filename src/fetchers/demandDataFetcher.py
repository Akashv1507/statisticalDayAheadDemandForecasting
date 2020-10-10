import pandas as pd
import datetime as dt
from typing import List, Tuple, TypedDict
from src.fetchers.scadaApiFetcher import ScadaApiFetcher
from src.typeDefs.rawMinwiseDemand import Idemand_purity_dict, IresultFormat


def toMinuteWiseData(demandDf:pd.core.frame.DataFrame, entity:str)->pd.core.frame.DataFrame:
    """convert random secondwise demand dataframe to minwise demand dataframe and add entity column to dataframe.

    Args:
        demandDf (pd.core.frame.DataFrame): random secondwise demand dataframe
        entity (str): entity name

    Returns:
        pd.core.frame.DataFrame: minwise demand dataframe
    """    
    try:
        demandDf = demandDf.resample('1min', on='timestamp').agg({'demandValue': 'first'})  # this will set timestamp as index of dataframe
    except Exception as err:
        print('error while resampling', err)
    demandDf.insert(0, "entityTag", entity)                      # inserting column entityName with all values of 96 block = entity
    demandDf.reset_index(inplace=True)
    return demandDf

def filterAction(demandDf :pd.core.frame.DataFrame, currDate:dt.datetime, entity:str, minRamp:int)-> IresultFormat:
    """ apply filtering action and generate purity percentage tuple

    Args:
        demandDf (pd.core.frame.DataFrame): unfiltered demand df
        currDate (dt.datetime): date for which demand data is fetched from api for particular entity
        entity (str): entity name
        minRamp (int): threshold deviation value for min-min ramping

    Returns:
        dict: resultDict['demandDf'] = filtered demandDf
              resultDict['purityPercent'] = purityPercentTuple
    """    
    # resultDict : resultFormat ={}
    countError = 0
    for ind in demandDf.index.tolist()[1:]:
        if abs(demandDf['demandValue'][ind]-demandDf['demandValue'][ind-1]) > minRamp :
            demandDf['demandValue'][ind] = demandDf['demandValue'][ind-1]
            countError = countError + 1
    try:
        purityPercent = 100 - (countError/(len(demandDf.index)))*100 
    except Exception as err:
        print('error while calculating purity percentage', err)
    purityPercentTuple = (currDate,entity,purityPercent )
   
    resultDict:IresultFormat = {
        'demandDf': demandDf,
        'purityPercent': purityPercentTuple
    }
    return resultDict
    

def applyFilteringToDf(demandDf : pd.core.frame.DataFrame, entity:str, currDate: dt.datetime) -> IresultFormat:
    """ apply filtering logic to each entity demand data and returns dictionary

    Args:
        demandDf (pd.core.frame.DataFrame): demand dataframe
        entity (str): entity name
        currDate (dt.datetime): date for which demand data is fetched from api for particular entity

    Returns:
        dict: resultDict['demandDf'] = filtered demandDf
              resultDict['purityPercent'] = purityPercentTuple
    """    
    if entity == 'WRLDCMP.SCADA1.A0046945':
        resultDict = filterAction(demandDf, currDate, entity, 500)

    if entity == 'WRLDCMP.SCADA1.A0046948' or entity == 'WRLDCMP.SCADA1.A0046962' or entity == 'WRLDCMP.SCADA1.A0046953':
        resultDict = filterAction(demandDf, currDate, entity, 200)
    
    if entity == 'WRLDCMP.SCADA1.A0046957' or entity == 'WRLDCMP.SCADA1.A0046978' or entity == 'WRLDCMP.SCADA1.A0046980':
        resultDict = filterAction(demandDf, currDate, entity, 1000)
   
    if entity == 'WRLDCMP.SCADA1.A0047000':
        resultDict:IresultFormat = filterAction(demandDf, currDate, entity, 2000)
    return resultDict

def toListOfTuple(df:pd.core.frame.DataFrame) -> List[Tuple]:
    """convert demand data to list of tuples

    Args:
        df (pd.core.frame.DataFrame): demand data dataframe

    Returns:
        List[Tuple]: list of tuple of demand data
    """    
    data:List[Tuple] = []
    for ind in df.index:
        tempTuple = (str(df['timestamp'][ind]), df['entityTag'][ind], float(df['demandValue'][ind]) )
        data.append(tempTuple)
    return data


def fetchDemandDataFromApi(currDate: dt.datetime, configDict: dict)-> Idemand_purity_dict:
    """fetches demand data from api-> convert to 1 min data, generate purity percent list -> returns dictionary

    Args:
        currDate (dt.datetime): currant date
        configDict (dict): application dictionary

    Returns:
        dict: demand_purity_dict['data'] = per min demand data for each entity in form of list of tuple
              demand_purity_dict['purityPercentage'] = purity percentage of each entity in form of list of tuple

    """    
    tokenUrl: str = configDict['tokenUrl']
    apiBaseUrl: str = configDict['apiBaseUrl']
    clientId = configDict['clientId']
    clientSecret = configDict['clientSecret']

    purityPercentageList:List[Tuple] = []
    #initializing temporary empty dataframe that append demand values of all entities
    storageDf = pd.DataFrame(columns = [ 'timestamp','entityTag','demandValue']) 
    #list of all entities
    listOfEntity =['WRLDCMP.SCADA1.A0046945','WRLDCMP.SCADA1.A0046948','WRLDCMP.SCADA1.A0046953','WRLDCMP.SCADA1.A0046957','WRLDCMP.SCADA1.A0046962','WRLDCMP.SCADA1.A0046978','WRLDCMP.SCADA1.A0046980','WRLDCMP.SCADA1.A0047000']
    #creating object of ScadaApiFetcher class 
    obj_scadaApiFetcher = ScadaApiFetcher(tokenUrl, apiBaseUrl, clientId, clientSecret)

    for entity in listOfEntity:
        # fetching secondwise data from api for each entity(timestamp,value) and converting to dataframe
        resData = obj_scadaApiFetcher.fetchData(entity, currDate, currDate)
        demandDf = pd.DataFrame(resData, columns =['timestamp','demandValue']) 

        #converting to minutewise data and adding entityName column to dataframe
        demandDf = toMinuteWiseData(demandDf,entity)
        
        #applying filtering logic
        date_key = currDate.date()
        resultDict : IresultFormat = applyFilteringToDf(demandDf,entity, str(date_key))

        #appending purity percentage of each entity to list 
        purityPercentageList.append(resultDict['purityPercent'])

        #appending per min demand data for each entity to tempDf
        storageDf = pd.concat([storageDf, resultDict['demandDf']],ignore_index=True)

    storageDf.to_excel(r'D:\wrldc_projects\demand_forecasting\filtering demo\dpi-1043.xlsx')
    # converting storageDf(contain per min demand values of all entities) to list of tuple 
    data:List[Tuple] = toListOfTuple(storageDf)
    
    demand_purity_dict : Idemand_purity_dict = { 'data': data, 'purityPercentage': purityPercentageList}
    
    return demand_purity_dict
    