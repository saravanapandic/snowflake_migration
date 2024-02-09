import pandas

def db_create_stm(db_list):
    try:
        var_conn=[]
        # print(db_list.collect())
        for i in range (len(db_list)):
            var_str='create or replace database '
            value_db=(db_list['DATABASE_NAME'].iloc[i])
            var_conn.append(var_str + value_db )
        return(var_conn)
    except Exception as error:
        print('error on data base creation part')
        print(error)
# db_create_stm(['saeawea','adsadsad'])






         

