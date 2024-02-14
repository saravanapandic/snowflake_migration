import pandas as pd
import query_in_target as Qit
import conn_sf_string_onsource as conn_sf
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
from snowflake.connector.pandas_tools import write_pandas,pd_writer
import snowflake.connector

def schema_table_join_Db(db_sc_tb_list,Target_SF_account,source_Sf_account):
    connection_source= conn_sf.connected_sf(source_Sf_account)
    lists=[]
    for db_sc_list_count in range(len(db_sc_tb_list)):
        connection_source.execute("select get_ddl('TABLE',"+"'" + db_sc_tb_list.iloc[db_sc_list_count][3]+"');")
        list_table_ddl = connection_source.fetchall()
        table_Ddl=pd.DataFrame(list_table_ddl)
        Qit.executed_in_target_sf_table_create(Target_SF_account,table_Ddl.iloc[0][0],db_sc_tb_list.iloc[db_sc_list_count][0],db_sc_tb_list.iloc[db_sc_list_count][1])
        print('table:'+ db_sc_tb_list.iloc[db_sc_list_count][2]+' on database: '+ db_sc_tb_list.iloc[db_sc_list_count][0] +' on schema: '+ db_sc_tb_list.iloc[db_sc_list_count][1])
    print("---------------------------------------all table is create successfully-------------------------------------------------------------------------")



def table_crt_list(database_details,schema_table_list,Target_SF_account,source_Sf_account):
    try:
        schema_list=[]
        database_list=[]
        table_list=[]
        for database_count in range (len(database_details)):
            for schema_Db_count in range (len(schema_table_list)):
                if database_details.iloc[database_count][0]==schema_table_list.iloc[schema_Db_count][1]:
                    schema_list.append(schema_table_list.iloc[schema_Db_count][0])
                    database_list.append(schema_table_list.iloc[schema_Db_count][1])
                    table_list.append(schema_table_list.iloc[schema_Db_count][2])
        data = {'database': database_list,'schema': schema_list,'table':table_list}
        data_sc_tb = pd.DataFrame(data)
        data_sc_tb['joined'] = data_sc_tb['database'].astype(str) +'.' + data_sc_tb['schema']+'.'+data_sc_tb['table']
        schema_table_join_Db(data_sc_tb,Target_SF_account,source_Sf_account)
        table_insert_fun(data_sc_tb,Target_SF_account,source_Sf_account)
    except  Exception as error:
        print ("error on table create  process")
        print(error)

def table_insert_fun(table_schema_database_details,Target_SF_Account,Source_SF_Account):
    try:
        connection_source= conn_sf.connected_sf(Source_SF_Account)
        lists=[]
        print("------------------- table value inserting process is going ----------------------------------------------------------------------")
        for db_sc_list_count in range(len(table_schema_database_details)):
            connection_source.execute("select * from " + table_schema_database_details.iloc[db_sc_list_count][3]+" limit 3000;")
            database=table_schema_database_details.iloc[db_sc_list_count][0]
            schema=table_schema_database_details.iloc[db_sc_list_count][1]
            table_name=table_schema_database_details.iloc[db_sc_list_count][2]
            list_table_ddl = connection_source.fetchall()
            #column information for schema name
            column_info = connection_source.description
            column_names = [info[0] for info in column_info]
            table_value_details=pd.DataFrame(list_table_ddl,columns=column_names)
            table_insert_target_account(database,schema,Target_SF_Account,   table_name,table_value_details)

    except  Exception as error:
        print ("error on insert into table  process")
        print(error)




def table_insert_target_account(database_value,schema_value,target_account,table_name,table_dataframe_values):
    try:
        connection_target_schm=snowflake.connector.connect(user=target_account[0],password=target_account[1],account=target_account[2],warehouse=target_account[3],database=database_value,schema =schema_value)
        write_pandas(connection_target_schm,table_dataframe_values,table_name,chunk_size=2000,compression="snappy")
        print("value insert into "+ table_name +" on schema "+ schema_value+"  on database "+database_value)
    except  Exception as error:
        print ("error on insert into table"+ table_name)
        print(error)
