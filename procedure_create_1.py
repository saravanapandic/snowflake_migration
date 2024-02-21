import conn_sf_string_onsource as conn_sf
import query_in_target as Qit
import pandas  as pd

def procedure_list_source_from_main(procedure_list_all,source_account,target_account):
    try:
        argument_return_join_string=""
        list_ddl_procedure=[]
        for iterator_procedure_count in  range(len(procedure_list_all)):
            argument_return_join_string=procedure_argument_join(procedure_list_all["ARGUMENT_SIGNATURE"].iloc[iterator_procedure_count])
            # procedure_DDL_source(procedure_list_all["PROCEDURE_CATALOG"].iloc[iterator_procedure_count],procedure_list_all["PROCEDURE_NAME"].iloc[iterator_procedure_count],procedure_list_all["PROCEDURE_SCHEMA"].iloc[iterator_procedure_count],argument_return_join_string,source_account)
            list_ddl_procedure.append(procedure_DDL_source(procedure_list_all["PROCEDURE_CATALOG"].iloc[iterator_procedure_count],procedure_list_all["PROCEDURE_NAME"].iloc[iterator_procedure_count],procedure_list_all["PROCEDURE_SCHEMA"].iloc[iterator_procedure_count],argument_return_join_string,source_account))
        Qit.executed_in_target_sf(target_account,list_ddl_procedure)
        print("number procedure available in source account {source_account} and number store procedure load into target account {target_account}".format(source_account=len(procedure_list_all),target_account=len(list_ddl_procedure))) 
        if len(procedure_list_all)==len(list_ddl_procedure):
            print("----------------------------------------all store procedure is load----------------------------------------------")
        else:
            print("----------------------------------------missing on load all store procedure---------------------------------------")          
    except Exception as error:
        print('error on procedure create on main schmea _1')
        print(error)

def procedure_argument_join(argument_name):
    x = argument_name.split(',')
    joins=""
    len_splite=0

    for i in x:

        if i !="()":
            if len_splite == 0:
                y=i.split(" ")
                string_replace=y[len(y)-1]
                joins=joins + string_replace.replace(")"," ")
                len_splite =1
            else:
                y=i.split(")")
                string_replace=y[len(y)-1]
                joins=joins + ','+ string_replace.replace(")"," ")
                len_splite =1
        else:
            return(" ")
    return (joins)
def procedure_DDL_source(database_name,procedure_name,schema_name,argument_pro,source_account):
    connection_source= conn_sf.connected_sf(source_account)
    connection_source.execute("select get_ddl('procedure',"+"'" + database_name+"."+schema_name+"."+procedure_name+"("+argument_pro+")"+"'"+",True);")
    procedure_DDL_value = connection_source.fetchone()
    database_in_source=pd.DataFrame(procedure_DDL_value)
    return database_in_source.iloc[0][0]
                              
                        


  

    
