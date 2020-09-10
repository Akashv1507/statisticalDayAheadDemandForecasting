import cx_Oracle
import pandas as pd
import datetime as dt
from typing import List, Tuple


class MinwiseDemandFetchRepo():
    """minute wise demand fetch repository
    """

    def __init__(self, con_string):
        """initialize connection string
        Args:
            con_string ([type]): connection string 
        """
        self.connString = con_string

    def toBlockwiseDemand(self, minwiseDemandDf: pd.core.frame.DataFrame) ->pd.core.frame.DataFrame :
        """convert minute wise demand data to block wise deamnd data

        Args:
            minwiseDemandDf (pd.core.frame.DataFrame): minute wise demand dataframe

        Returns:
            pd.core.frame.DataFrame: block wise demand dataframe
        """        

        storageDf = pd.DataFrame(columns = ['TIME_STAMP','ENTITY_TAG','DEMAND_VALUE'])
        group = minwiseDemandDf.groupby("ENTITY_TAG")
        for entity, groupDf in group:

            try:
                groupDf = groupDf.resample('15min', on='TIME_STAMP').mean()   # this will set timestamp as index of dataframe
            except Exception as err:
                print('error while resampling', err)
            groupDf.insert(0, "ENTITY_TAG", entity)                      # inserting column entityName with all values of 96 block = entity
            groupDf.reset_index(inplace=True)
            storageDf = pd.concat([storageDf, groupDf],ignore_index=True)

        return storageDf
    
    def toListOfTuple(self,df:pd.core.frame.DataFrame) -> List[Tuple]:
        """convert BLOCKWISE demand data to list of tuples[(timestamp,entityTag,demandValue),]

        Args:
            df (pd.core.frame.DataFrame): block wise demand dataframe

        Returns:
            List[Tuple]: list of tuple of blockwise demand data [(timestamp,entityTag,demandValue),]
        """    
        data:List[Tuple] = []
        for ind in df.index:
            tempTuple = (str(df['TIME_STAMP'][ind]), df['ENTITY_TAG'][ind], float(df['DEMAND_VALUE'][ind]) )
            data.append(tempTuple)
        return data
 

    def fetchMinwiseDemand(self, startDateKey: dt.datetime, endDateKey: dt.datetime) -> List[Tuple]:
        """fetch min wise demand from db and return list of tuple[(timestamp,entityTag,demandValue),]
        Args:
            self: object of class 
            startDateKey (dt.datetime): start-date
            endDateKey (dt.datetime): end-date
        Returns:
            List[Tuple]: [(timestamp,entityTag,demandValue),]
        """

        startDate = str(startDateKey.date())
        endDate = str(endDateKey.date())
        start_time_value = startDate + " 00:00:00"
        end_time_value = endDate + " 23:59:00"
        try:
            # connString=configDict['con_string_local']
            connection = cx_Oracle.connect(self.connString)

        except Exception as err:
            print('error while creating a connection', err)
        else:
            print(connection.version)
            try:
                cur = connection.cursor()
                fetch_sql = "SELECT time_stamp, entity_tag, demand_value FROM raw_minwise_demand WHERE time_stamp BETWEEN TO_DATE(:start_time,'YYYY-MM-DD HH24:MI:SS') and TO_DATE(:end_time,'YYYY-MM-DD HH24:MI:SS') ORDER BY time_stamp"
                # cur.execute("ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS' ")
                minwiseDemandDf = pd.read_sql(fetch_sql, params={
                                 'start_time': start_time_value, 'end_time': end_time_value}, con=connection)

            except Exception as err:
                print('error while creating a cursor', err)
            else:
                connection.commit()
        finally:
            cur.close()
            connection.close()
            print("connection closed")

        print("retrieval of minwsie demand completed")

        blockwiseDf = self.toBlockwiseDemand(minwiseDemandDf)
        data : List[Tuple] = self.toListOfTuple(blockwiseDf)
        return data

        