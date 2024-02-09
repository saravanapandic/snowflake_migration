import conn_sf_string_onsource as conn_sf
import Db_sche_tb_creation as Db_Cr
import query_in_target as Qit
import schema_creation as scc
import table_crt_part as tcp
import pandas  as pd


try:
# source account details 
# source_account_details=('DEPLOYMENT','Elait@1234','iy53956.central-india.azure','COMPUTE_WH')
    source_account_details=('Govarthanan','Snowflake@777@','mt55505.ap-south-1.aws','COMPUTE_WH')
    target_account_details=('Sashanth','Sash@123','dm63557.ap-south-1.aws','COMPUTE_WH')

    connection_source= conn_sf.connected_sf(source_account_details)
    connection_source.execute("select database_name from SNOWFLAKE.ACCOUNT_USAGE.DATABASES where deleted is null and database_name not in ('SNOWFLAKE_SAMPLE_DATA','SNOWFLAKE')")
    list_values_source_Db = connection_source.fetchall()
    connection_source.close()
    

    #column information for database name
    column_info = connection_source.description
    column_names = [info[0] for info in column_info]

    #list of database available in source snowflake account 
    database_in_source=pd.DataFrame(list_values_source_Db,columns=column_names)
    print ("list database available in sourece snowflake account")
    print(database_in_source)


    #database creation part in target snowflake account 
    target_Stm_database=Db_Cr.db_create_stm(database_in_source)
    print(Qit.executed_in_target_sf(target_account_details,target_Stm_database))

    # create schema in target snowflake account 
    connection_source= conn_sf.connected_sf(source_account_details)
    connection_source.execute("select distinct table_schema,table_catalog from SNOWFLAKE.account_usage.tables where deleted is null and table_schema not in('INFORMATION_SCHEMA','PUBLIC') and table_catalog not in('SNOWFLAKE_SAMPLE_DATA');")
    list_values_source_Db = connection_source.fetchall()
    connection_source.close()

    #column information for schema name
    column_info = connection_source.description
    column_names = [info[0] for info in column_info]

    #list schema,table  available in source snowflake account 
    schema_in_source=pd.DataFrame(list_values_source_Db,columns=column_names)
    print ("list schema available in sourece snowflake account")
    print(schema_in_source)

    scc.schema_check(database_in_source,schema_in_source,target_account_details)
    #table create list 
    connection_source= conn_sf.connected_sf(source_account_details)
    connection_source.execute("select distinct table_schema,table_catalog,table_name from SNOWFLAKE.account_usage.tables where deleted is null and table_schema not in('INFORMATION_SCHEMA','PUBLIC') and table_catalog not in('SNOWFLAKE_SAMPLE_DATA');")
    list_table_on_source = connection_source.fetchall()
    connection_source.close()

    #column information for table name
    column_info = connection_source.description
    column_names = [info[0] for info in column_info]


    table_in_source=pd.DataFrame(list_table_on_source,columns=column_names)
    print ("list table available in sourece snowflake account")
    print(table_in_source)

    tcp.table_crt_list(database_in_source,table_in_source,target_account_details,source_account_details)

    # tcp.table_crt_list(database_in_source,target_account_details)
    print("process is completed")
except  Exception as error:
        print ("error on main file of process")
        print(error)














   

