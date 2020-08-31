import cx_Oracle
import datetime as dt
from typing import List, Tuple


class PurityPercentageInsertionRepo():
    """repository to push purity percentage of entities to db.
    """

    def __init__(self, con_string: str) -> None:
        """initialize connection string
        Args:
            con_string ([type]): connection string 
        """
        self.connString = con_string

    def insertPurityPercentage(self, data: List[Tuple]) -> bool:
        """Insert insert purity percent to db
        Args:
            self : object of class 
            data (List[Tuple]): (date_key, entity_tag, purity_percentage)
        Returns:
            bool: return true if insertion is successful else false
        """
        
        # making list of tuple of date_keys(unique),entity_tag based on which deletion takes place before insertion of duplicate

        existingEntityRows = [(x[0],x[1]) for x in data]

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
                    cur.execute(
                        "ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD' ")
                    del_sql = "DELETE FROM purity_percentage WHERE DATE_KEY = :1 and entity_tag=:2"
                    cur.executemany(del_sql, existingEntityRows)
                    insert_sql = "INSERT INTO purity_percentage(DATE_KEY,ENTITY_TAG,PURITY_PERCENTAGE) VALUES(:1, :2, :3)"
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
            print("purity perecentage insertion complete")
        return isInsertionSuccess