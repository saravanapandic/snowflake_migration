import snowflake.connector
import pandas  as pd

def connected_table_sf(user_details):
    try:
        conn = snowflake.connector.connect(
            user=user_details[0],
            password=user_details[1],
            account=user_details[2],
            warehouse=user_details[3],
            )
        curs_schema=conn.cursor()
        return curs_schema
    except :
        print('snowflake connected is failed on table')



target_account_details=('Govarthanan','Snowflake@777@','mt55505.ap-south-1.aws','COMPUTE_WH')


connection_source= connected_table_sf(target_account_details)
connection_source.execute(CREATE OR REPLACE PROCEDURE "GENERATE_TESTCASE"()
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS CALLER
AS 'declare
    schema_name resultset default( select distinct(schemaname),databasename from METADATACONTROLDB.META.COLUMNINFORMATION order by schemaname desc );
    table_name resultset;
    cur_schema_name cursor for schema_name;
    --cur_table_name cursor for table_name;
    database_schema_change varchar;
    database_schema_change_copy varchar;
    part_var_1 varchar;
    part_var_2 varchar;
    part_var_3 varchar;
    part_var_2_copy varchar := '''';
    part_var_3_copy varchar := '''';
    part_var_4 varchar;
    part_var_5 varchar;
    part_var_6 varchar;
    part_var_6_copy varchar;
    part_var_7 varchar;
    part_var_8 varchar ;
    column_type varchar;
    database_name_var varchar;
    column_name_array VARIANT;
    column_name varchar;
    column_count_var int;
    schema_name_var varchar;
    table_name_var varchar;
BEGIN
    part_var_1 :=(''version: 2\\n'' || ''sources:\\n'');
    for schema_for in cur_schema_name do
      set schema_name_var := schema_for.schemaname;
      set database_name_var :=  schema_for.databasename;


      set database_schema_change := ''  - name: '' || :schema_name_var || ''_test\\n'' ||
                                    ''    description: testcase for intermediate database\\n'' ||
                                    ''    database: '' || :database_name_var || ''\\n'';

      set part_var_2 := ''    schema: ''|| :schema_name_var || ''\\n'' ||
                        ''    tables:\\n'';
       table_name :=(select distinct(tablename) from METADATACONTROLDB.META.COLUMNINFORMATION where schemaname=:schema_name_var and ACTION=''I'' );
      let cur_table_name cursor for table_name;
      SET part_var_3_copy := '''';
         for table_for in cur_table_name do

            set table_name_var := table_for.tablename;
            ------------------------------------------------------------------------------------------------------------------
            set column_count_var := (select ARRAY_SIZE(array_agg(columnname)) from METADATACONTROLDB.META.COLUMNINFORMATION where
                            schemaname=:schema_name_var and tablename= :table_name_var);
            select array_agg(columnname) into :column_name_array from METADATACONTROLDB.META.COLUMNINFORMATION where
                            schemaname=:schema_name_var and tablename= :table_name_var;
            -------------------------------------------------------------------------------------------------------------------
            set part_var_3 := ''      - name: '' || :table_name_var || ''\\n'' ||
                              ''        description: table_test\\n'' ||
                              ''        tests:\\n'';
            set part_var_4 := ''          - dbt_expectations.expect_table_column_count_to_equal:\\n'' ||
                              ''              value: '' || :column_count_var || ''\\n'';
            set part_var_5 := ''          - dbt_expectations.expect_table_columns_to_contain_set:\\n'' ||
                              ''              column_list: '' || :column_name_array || ''\\n'' ||
                              ''        columns:\\n'';
            set part_var_6_copy :='''';
            ---------------------------------------------------------------------------------------------------------------------
            for columncount_iteration in 1 to column_count_var do
            select GET(:column_name_array,:columncount_iteration-1) into :column_name;
            set part_var_6 := ''          - name: ''||:column_name|| ''\\n'' ||
                              ''            description: column_test\\n'' ||
                              ''            tests:\\n'';
            if(exists(select 1 from METADATACONTROLDB.META.COLUMNINFORMATION where ISNULLABLE =''NO'' and schemaname=:schema_name_var and tablename= :table_name_var and COLUMNNAME= :column_name )) then
            begin
            set part_var_7 :=''              - not_null\\n'';
            set part_var_6_copy := part_var_6_copy||part_var_6||part_var_7;
            end;
            else
            begin
            set part_var_6_copy := part_var_6_copy||part_var_6;
            end;
            end if;
            --column type test case----------------------------------
            if(exists(select 1 from METADATACONTROLDB.META.COLUMNINFORMATION where schemaname=:schema_name_var and tablename= :table_name_var and COLUMNNAME= :column_name  and datatype in (''xml'',''geography'',''uniqueidentifier'',''hierarchyid'',''image'',''nvarchar'',''ntext'',''nchar'',''varchar'',''char'',''text'')))then
           begin
            set   column_type:=''varchar'';
            set  part_var_8:=(''              - dbt_expectations.expect_column_values_to_be_of_type:\\n'' ||
                              ''                  column_type: ''|| :column_type || ''\\n'');
                  set part_var_6_copy := part_var_6_copy||part_var_8;
            end;
            ELSEIF(exists(select 1 from METADATACONTROLDB.META.COLUMNINFORMATION where schemaname=:schema_name_var and tablename= :table_name_var  and  datatype in (''bigint'',''decimal'',''int'',''money'',''image'',''numeric'',''smallint'',''smallmoney'',''tinyint'')))then
            begin
            set   column_type:=''number'';
            set   part_var_8:=(''              - dbt_expectations.expect_column_values_to_be_of_type:\\n'' ||
                               ''                  column_type: ''|| :column_type || ''\\n'');
                  set part_var_6_copy := part_var_6_copy||part_var_8;
            end;
             ELSEIF(exists(select 1 from METADATACONTROLDB.META.COLUMNINFORMATION where schemaname=:schema_name_var and tablename= :table_name_var  and COLUMNNAME= :column_name  and  datatype in (''float'')))then
            begin
            set   column_type:=''float'';
            set   part_var_8:=(''              - dbt_expectations.expect_column_values_to_be_of_type:\\n'' ||
                               ''                  column_type: ''|| :column_type || ''\\n'');
                  set part_var_6_copy := part_var_6_copy||part_var_8;
            end;
            ELSEIF(exists(select 1 from METADATACONTROLDB.META.COLUMNINFORMATION where schemaname=:schema_name_var and tablename= :table_name_var  and COLUMNNAME= :column_name  and  datatype in (''date'')))then
            begin
            set   column_type:=''date'';
            set   part_var_8:=(''              - dbt_expectations.expect_column_values_to_be_of_type:\\n'' ||
                               ''                  column_type: ''|| :column_type || ''\\n'');
                  set part_var_6_copy := part_var_6_copy||part_var_8;
            end;
            end if ;
            end for;
            set part_var_3_copy := part_var_3_copy || part_var_3 || part_var_4|| part_var_5||part_var_6_copy;
         end for;
      set part_var_2_copy := database_schema_change || part_var_2;
      set part_var_2_copy := part_var_2_copy || part_var_3_copy;

     part_var_1 := part_var_1 ||part_var_2_copy;
    end for;
    return part_var_1;
end';)
