import conn_sf_string_onsource as conn_sf
import pandas  as pd


def executed_in_target_sf(target_account,query_loaded):
    try:
        connection_target= conn_sf.connected_sf(target_account)
        for i in query_loaded:
            connection_target.execute(i)
        return 'all database is create successfully'
    except  Exception as error:
        print('error on query executed part in target')
        print(error)
    finally:
        connection_target.close()

def executed_in_target_sf_schema_create(target_account,query_STM,database_name):
    try:
        connection_target_schm= conn_sf.connected_database_sf(target_account,database_name)
        connection_target_schm.execute(query_STM)
    except  Exception as error:
        print('error on query executed part on  schema create in target')
        print(error)
    finally:
        connection_target_schm.close()

def executed_in_target_sf_table_create(target_account,query_STM,database_name,schema_name):
    try:
        connection_target_schm= conn_sf.connected_table_sf(target_account,database_name,schema_name)
        connection_target_schm.execute(query_STM)
    except  Exception as error:
        print('error on query executed part on  table create in target')
        print(error)
    finally:
        connection_target_schm.close()


        
    