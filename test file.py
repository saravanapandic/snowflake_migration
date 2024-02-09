import snowflake.connector
import pandas  as pd

def connected_table_sf(user_details,database_name,schema_name):
    try:
        conn = snowflake.connector.connect(
            user=user_details[0],
            password=user_details[1],
            account=user_details[2],
            warehouse=user_details[3],
            database=database_name,
            schema =schema_name
            )
        curs_schema=conn.cursor()
        return curs_schema
    except :
        print('snowflake connected is failed on table')



target_account_details=('Sashanth','Sash@123','dm63557.ap-south-1.aws','COMPUTE_WH')
database='META_CONTROL_DB'
schema='META'
a=[[]]
connection_source= connected_table_sf(target_account_details,database,schema)
connection_source.execute("INSERT INTO META_CONTROL_DB.META.COLUMNINFORMATION VALUES(16,null,'REGION','STAGE_DB','SALES_DETAILS','ONLINE_SALES',1,'varchar',0,'NO',20,0,current_user(),current_timestamp(),current_timestamp(),'I','ACTURIS')")
