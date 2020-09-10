import cx_Oracle
import pandas as pd
import datetime as dt
from typing import List, Tuple


class DemandFetchForAlgoRepo():
    """fetch D-2,D-7,D-9,D-14 demand and apply day ahead demand forecasting algorithm.
    """

    def __init__(self, con_string):
        """initialize connection string
        Args:
            con_string ([type]): connection string 
        """
        self.connString = con_string
        self.storageForecastedDf = pd.DataFrame(columns = [ 'timestamp','entityTag','forecastedDemand']) 
        

    
    def applyDayAheadForecast(self, demandDf:pd.core.frame.DataFrame, entity:str,currDate:dt.datetime)->pd.core.frame.DataFrame:
        """applying DA forecasting algorithm

        Args:
            demandDf (pd.core.frame.DataFrame): demanDf containing D-2,D-7,D-9,D-14 demand of a particular entity
            entity (str): entity name
            currDate (dt.datetime): current date

        Returns:
            pd.core.frame.DataFrame: forecastedDf of a particular entity
        """        
        
        dateOfForecast = currDate + dt.timedelta(days=1)
        #calculating parameters A,B,C,avg
        demandDf['A']= (demandDf['dMinus7DemandValue']-demandDf['dMinus2DemandValue'])/demandDf['dMinus2DemandValue']
        demandDf['B']= (demandDf['dMinus9DemandValue']-demandDf['dMinus7DemandValue'])/demandDf['dMinus9DemandValue']
        demandDf['C']= (demandDf['dMinus9DemandValue']-demandDf['dMinus14DemandValue'])/demandDf['dMinus9DemandValue']
        demandDf['avg'] = demandDf[['A', 'B', 'C']].mean(axis=1)
        #forecasting logic
        demandDf['forecastedDemand'] = (1+demandDf['avg'])*demandDf['dMinus2DemandValue']
        demandDf['timestamp'] = pd.date_range(start=dateOfForecast,freq='15min',periods=96)
        demandDf['entityTag'] = entity
        #selecting only timestamp, entityTag, and forecasted demand columns from demandDf
        forecastedDf = demandDf[['timestamp', 'entityTag', 'forecastedDemand']]
        return forecastedDf



    def toListOfTuple(self,df:pd.core.frame.DataFrame) -> List[Tuple]:
        """convert forecasted BLOCKWISE demand data to list of tuples[(timestamp,entityTag,demandValue),]

        Args:
            df (pd.core.frame.DataFrame): forecasted block wise demand dataframe

        Returns:
            List[Tuple]: list of tuple of forecasted blockwise demand data [(timestamp,entityTag,demandValue),]
        """    
        data:List[Tuple] = []
        for ind in df.index:
            tempTuple = (str(df['timestamp'][ind]), df['entityTag'][ind], float(df['forecastedDemand'][ind]) )
            data.append(tempTuple)
        return data
 

    def fetchBlockwiseDemandForAlgo(self, currDateKey: dt.datetime) -> List[Tuple]:
        """"fetch D-2,D-7,D-9,D-14 demand and apply day ahead demand forecasting algorithm, return list of tuple[(timestamp,entityTag,demandValue),]
        Args:
            self: object of class 
            startDateKey (dt.datetime): start-date
            endDateKey (dt.datetime): end-date
        Returns:
            List[Tuple]: [(timestamp,entityTag,demandValue),]
        """
        dMinus2 = currDateKey-dt.timedelta(days=1)
        dMinus7 = currDateKey-dt.timedelta(days=6)
        dMinus9 = currDateKey-dt.timedelta(days=8)
        dMinus14 = currDateKey-dt.timedelta(days=13)
        # print(dMinus2,dMinus7,dMinus9,dMinus14)

        dMinus2_startTime = dMinus2
        dMinus2_endTime = dMinus2 + dt.timedelta(hours= 23,minutes=45)
        dMinus7_startTime = dMinus7
        dMinus7_endTime = dMinus7 + dt.timedelta(hours= 23,minutes=45)
        dMinus9_startTime = dMinus9
        dMinus9_endTime = dMinus9 + dt.timedelta(hours= 23,minutes=45)
        dMinus14_startTime = dMinus14
        dMinus14_endTime = dMinus14 + dt.timedelta(hours= 23,minutes=45)
        
        listOfEntity =['WRLDCMP.SCADA1.A0046945','WRLDCMP.SCADA1.A0046948','WRLDCMP.SCADA1.A0046953','WRLDCMP.SCADA1.A0046957','WRLDCMP.SCADA1.A0046962','WRLDCMP.SCADA1.A0046978','WRLDCMP.SCADA1.A0046980','WRLDCMP.SCADA1.A0047000']

        try:
            # connString=configDict['con_string_local']
            connection = cx_Oracle.connect(self.connString)

        except Exception as err:
            print('error while creating a connection', err)
        else:
            print(connection.version)
            try:
                #iterating through each entity and generating DA forecast
                for entity in listOfEntity:
                    cur = connection.cursor()
                    fetch_sql = "SELECT time_stamp, demand_value FROM staging_blockwise_demand WHERE time_stamp BETWEEN TO_DATE(:start_time) and TO_DATE(:end_time) and entity_tag = :tag ORDER BY time_stamp"
                    cur.execute("ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS' ")
                    dMinus2Df = pd.read_sql(fetch_sql, params={
                                        'start_time': dMinus2_startTime, 'end_time': dMinus2_endTime, 'tag': entity}, con=connection)
                    dMinus7Df = pd.read_sql(fetch_sql, params={
                                        'start_time': dMinus7_startTime, 'end_time': dMinus7_endTime, 'tag': entity}, con=connection)
                    dMinus9Df = pd.read_sql(fetch_sql, params={
                                        'start_time': dMinus9_startTime, 'end_time': dMinus9_endTime, 'tag': entity}, con=connection)
                    dMinus14Df = pd.read_sql(fetch_sql, params={
                                        'start_time': dMinus14_startTime, 'end_time': dMinus14_endTime, 'tag': entity}, con=connection)
                   # deleting timestamp column and renaming demand_value column of each df
                    del dMinus2Df['TIME_STAMP']
                    del dMinus7Df['TIME_STAMP']
                    del dMinus9Df['TIME_STAMP']
                    del dMinus14Df['TIME_STAMP']
                    dMinus2Df.rename(columns = {'DEMAND_VALUE':'dMinus2DemandValue'}, inplace = True)
                    dMinus7Df.rename(columns = {'DEMAND_VALUE':'dMinus7DemandValue'}, inplace = True)
                    dMinus9Df.rename(columns = {'DEMAND_VALUE':'dMinus9DemandValue'}, inplace = True)
                    dMinus14Df.rename(columns = {'DEMAND_VALUE':'dMinus14DemandValue'}, inplace = True) 
                    #concatenating d-2,d-7,d-9,d-14 demand value of particular entity horizontaly
                    demandConcatDf = pd.concat([dMinus2Df,dMinus7Df,dMinus9Df,dMinus14Df], axis=1)
                    #apply forecasting algorithm
                    forecastedDf = self.applyDayAheadForecast(demandConcatDf,entity,currDateKey )
                    #storageForecastedDf store forecasted value of each entity
                    self.storageForecastedDf = pd.concat([self.storageForecastedDf, forecastedDf],ignore_index=True)
                    
            except Exception as err:
                print('error while creating a cursor', err)
            else:
                connection.commit()
        finally:
            cur.close()
            connection.close()
            print("connection closed")

        self.storageForecastedDf.to_excel(r'D:\wrldc_projects\demand_forecasting\filtering demo\10-sept forecast.xlsx')
        data : List[Tuple] = self.toListOfTuple(self.storageForecastedDf)
        return data

        