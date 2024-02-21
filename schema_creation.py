import pandas as pd
import query_in_target as Qit


def schema_create_join_Db(Db_list,Schema_list,target_SF_account):
    count_schema_targets=0
    for schema_list in  Schema_list:
        var_stm_crt='create or replace schema '+ schema_list +';'
        Qit.executed_in_target_sf_schema_create(target_SF_account,var_stm_crt,Db_list)
        count_schema_targets +=1
        # print ('schema name: {schema_name} on database: '+Db_list +'database successfully')
    return count_schema_targets

def schema_check(database_name,schema_Db_value,target_Sf_Account):
    try:
        
        
        for database_count in range (len(database_name)):
            schema=[]
            count_schema_target=0
            for schema_Db_count in range (len(schema_Db_value)):
                # if((database_name.iloc[database_i])==(schema_Db_value.iloc[schema_Db_i][1])):
                if database_name.iloc[database_count][0]== schema_Db_value.iloc[schema_Db_count][1]:
                    schema.append(schema_Db_value.iloc[schema_Db_count][0])
            count_schema_source=len(schema)
            if (len(schema)):
                count_schema_target=schema_create_join_Db(database_name.iloc[database_count][0],schema,target_Sf_Account)
            if count_schema_source==count_schema_target:
                print("-------------------------------------all schema avaiable  on database:{database_name} are successfully to target snowflake account---------------------------------".format(database_name=database_name.iloc[database_count][0]))
            else:
                print("-------------------------------------missing some schema to load in target account--------------------------------------------")
    except Exception as error:
        print ("error on schema checking process")
        print(error)



# a=[("saravna"),("vignesh")]
# b=[("sashanth","saravana"),("saravana","vignesh")]
# df=pd.DataFrame(a)
# df_d=pd.DataFrame(b)
# schema_check(df,df_d,'gdgfgfhh')


 
       



