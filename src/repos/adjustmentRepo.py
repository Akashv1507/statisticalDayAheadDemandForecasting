import cx_Oracle
import datetime as dt
from typing import List, Tuple
import pandas as pd 
from src.utils.checkSpecialDay import checkSpecialDay



class Adjustment():
    """adjustment repo to check D-2 is special or not based on which modification takes place
    """    
    def __init__(self, con_string: str) -> None:
        """initialize connection string
        Args:
            con_string ([type]): connection string 
        """
        self.connString = con_string

    def doReplacement(self,dateToReplaced:dt.datetime, dateByReplaced: dt.datetime)-> bool:
        """this will replace demand value of dateToReplaced by demand value of dateByReplaced, and push updated value to database

        Args:
            dateToReplaced (dt.datetime): date for which demand value is to be replaced
            dateByReplaced (dt.datetime): date by which demand value of dateToReplaced is to repalced

        Returns:
            bool: true if replacement success else false
        """        
        
        startTime_DateToReplaced = dateToReplaced 
        endTime_DateToReplaced = dateToReplaced + dt.timedelta(hours= 23,minutes=45)
        startTime_DateByReplaced = dateByReplaced
        endTime_DateByReplaced = dateByReplaced + dt.timedelta(hours= 23,minutes=45)
        # print(startTime_DateToReplaced,startTime_DateByReplaced,endTime_DateToReplaced,endTime_DateByReplaced)
        try:
            connection=cx_Oracle.connect(self.connString)

        except Exception as err:
            print('error while creating a connection',err)
        else:
            print(connection.version)
            try:
                cur=connection.cursor()
                fetch_sql_dateToReplaced ='''select time_stamp,entity_tag,demand_value
                            from staging_blockwise_demand
                            where time_stamp between to_date(:start_time) and to_date(:end_time)  order by entity_tag, time_stamp'''
                
                fetch_sql_dateByReplaced ='''select time_stamp,entity_tag,demand_value
                            from staging_blockwise_demand
                            where time_stamp between to_date(:start_time) and to_date(:end_time)  order by entity_tag, time_stamp'''


                cur.execute("ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS' ")
                dfToReplaced = pd.read_sql(fetch_sql_dateToReplaced,params={'start_time' : startTime_DateToReplaced,'end_time': endTime_DateToReplaced}, con=connection)
                dfByReplaced = pd.read_sql(fetch_sql_dateByReplaced,params={'start_time' : startTime_DateByReplaced,'end_time': endTime_DateByReplaced}, con=connection)
                         
            except Exception as err:
                print('error while creating a cursor',err)
            else:
                print('retrieval derived freq data  complete')
                connection.commit()
        finally:
            cur.close()
            connection.close()
            print("connection closed")
        
        # replace demand value of D-2
        dfToReplaced['DEMAND_VALUE'] = dfByReplaced['DEMAND_VALUE']

        # print(dfToReplaced)
        # print(dfByReplaced)
        # creating list of tuple for insertion in db
        data:List[Tuple] = []
        for ind in dfToReplaced.index:
            tempTuple = (str(dfToReplaced['TIME_STAMP'][ind]), dfToReplaced['ENTITY_TAG'][ind], float(dfToReplaced['DEMAND_VALUE'][ind]) )
            data.append(tempTuple)
        
        # making list of tuple of timestamp(unique),entity_tag based on which deletion takes place before insertion of duplicate
        existingRows = [(x[0],x[1]) for x in data]

        #pushing D-2 modified/replaced data to staging table
        try:
            
            connection = cx_Oracle.connect(self.connString)
            isInsertionSuccess = True

        except Exception as err:
            print('error while creating a connection', err)
        else:
            print(connection.version)
            try:
                cur = connection.cursor()
                try:
                    cur.execute("ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS' ")
                    del_sql = "DELETE FROM staging_blockwise_demand WHERE time_stamp = :1 and entity_tag=:2"
                    cur.executemany(del_sql, existingRows)
                    insert_sql = "INSERT INTO staging_blockwise_demand(time_stamp,ENTITY_TAG,demand_value) VALUES(:1, :2, :3)"
                    cur.executemany(insert_sql, data)
                except Exception as e:
                    print("error while insertion/deletion->", e)
                    isInsertionSuccess = False
            except Exception as err:
                print('error while creating a cursor', err)
                isInsertionSuccess = False
            else:
                connection.commit()
        finally:
            cur.close()
            connection.close()
            print("D-2 data is replaced")
        return isInsertionSuccess


    def doAdjustment(self, currDate : dt.datetime) -> bool:
        """this will do adjustment by checking whether D-2 is special day

        Args:
            currDate (dt.datetime): D-1 in case of our forecasting algorithm

        Returns:
            bool: returns true if adjustment success else false
        """        
        
        currDate = currDate.replace(hour=0, minute=0, second=0, microsecond=0) # D-1 in case of forecasting
        prevDate = currDate-dt.timedelta(days=1) # D-2 in case of forecasting
        dMinus3 = prevDate-dt.timedelta(days=1) # D-3 in case of forecasting
        dMinus4 = prevDate-dt.timedelta(days=2) # D-4 in case of forecasting
        prevSunday = prevDate-dt.timedelta(days=7) 

        if checkSpecialDay(prevDate):
            if prevDate.strftime("%A") == 'Sunday' :
                isAdjustmentSuccess:bool = self.doReplacement(prevDate,prevSunday)
                print("a")
            elif dMinus3.strftime("%A") == 'Sunday':
                isAdjustmentSuccess:bool = self.doReplacement(prevDate , dMinus4)
                print("b")
            else:
                isAdjustmentSuccess:bool=self.doReplacement(prevDate,dMinus3)
                print("c")
            
        return isAdjustmentSuccess
        



        