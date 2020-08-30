import cx_Oracle
import pandas as pd
from typing import List, Tuple
def createEntityMappingTable(configDict:dict)->bool:
    """create mapping table for entities 

    Args:
        configDict (dict): application dictionary

    Returns:
        bool: return true if insertion is success
    """    

    data : List[Tuple]=[]
    mappingFilePath = configDict['file_path'] + '\\entityMappingTable.xlsx'
    df = pd.read_excel(mappingFilePath)
    for ind in df.index:
       tempTuple= (df['entityTag'][ind], df['entityName'][ind], df['entityFullName'][ind])
       data.append(tempTuple)

    try:
        con_string= configDict['con_string_mis_warehouse']
        connection = cx_Oracle.connect(con_string)
        isInsertSuccess = True
    except Exception as err:
        print('error while creating a connection', err)
    else:
        print(connection.version)
        try:
            cur = connection.cursor()

                # delete the rows which are already present
            existingEntityRows = [(x[0],)
                                    for x in data]
            cur.executemany(
                    "delete from mis_warehouse.entity_mapping_table where entity_tag =: 1", existingEntityRows)

            insert_sql = "INSERT INTO mis_warehouse.entity_mapping_table(entity_tag, entity_name, entity_full_name) VALUES(:1, :2, :3)"
                
            cur.executemany(insert_sql, data)

        except Exception as err:
            print('error while creating a cursor', err)
            isInsertSuccess = False

        else:
            print('Insertion of mapping data complete')
            connection.commit()
    finally:
        cur.close()
        connection.close()
    return isInsertSuccess
    
