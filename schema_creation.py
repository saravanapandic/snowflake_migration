import pandas as pd
import query_in_target as Qit


def schema_create_join_Db(Db_list,Schema_list,target_SF_account):
    for schema_list in  Schema_list:
        var_stm_crt='create or replace schema '+ schema_list +';'
        Qit.executed_in_target_sf_schema_create(target_SF_account,var_stm_crt,Db_list)
        print ('schema create on '+Db_list +'database successfully')

def schema_check(database_name,schema_Db_value,target_Sf_Account):
    try:
        for database_count in range (len(database_name)):
            schema=[]
            for schema_Db_count in range (len(schema_Db_value)):
                # if((database_name.iloc[database_i])==(schema_Db_value.iloc[schema_Db_i][1])):
                if database_name.iloc[database_count][0]== schema_Db_value.iloc[schema_Db_count][1]:
                    schema.append(schema_Db_value.iloc[schema_Db_count][0])
            if (len(schema)):
                schema_create_join_Db(database_name.iloc[database_count][0],schema,target_Sf_Account)
    except:
        print ("error on schema checking process")



# a=[("saravna"),("vignesh")]
# b=[("sashanth","saravana"),("saravana","vignesh")]
# df=pd.DataFrame(a)
# df_d=pd.DataFrame(b)
# schema_check(df,df_d,'gdgfgfhh')


 
       



