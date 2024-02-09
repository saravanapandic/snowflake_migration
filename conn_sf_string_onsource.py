import snowflake.connector
import pandas  as pd

def connected_sf(user_details):
    try:
        conn = snowflake.connector.connect(
            user=user_details[0],
            password=user_details[1],
            account=user_details[2],
            warehouse=user_details[3]
            )
        curs=conn.cursor()
        return curs
    except :
        print('snowflake connected is failed on database')

# connection with database 
def connected_database_sf(user_details,database_name):
    try:
        conn = snowflake.connector.connect(
            user=user_details[0],
            password=user_details[1],
            account=user_details[2],
            warehouse=user_details[3],
            database=database_name
            )
        curs_schema=conn.cursor()
        return curs_schema
    except :
        print('snowflake connected is failed on schema')

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
