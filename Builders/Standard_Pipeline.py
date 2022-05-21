import pandas as pd

class Pipeline():
  
  def Build_Table_External_Table(self,delta_database:str, delta_table:str,snowflake_database:str, snowflake_table:str,stage_name:str, creation_type:str, search_pattern)->str:
    
    self.deltaschema   =  spark.sql(f'''DESCRIBE {delta_database}.{delta_table}''')
    self.deltalocation =  spark.sql(f"describe detail {app}.{t}").collect()[0]["location"]
    
    self.deltaschema  =   self.deltaschema                                                           \
                          .withColumn("External_Table_Columns", Column_Data('col_name','data_type')) \
                          .withColumn("Table_Name", lit(object_name))
    
    #converting to pandas dataframe since its simpler to operate on #
    self.deltaschema                                   = self.deltaschema.toPandas()[:-5] # remove partition and table properties info #
    self.deltaschema                                   = self.deltaschema.groupby('Table_Name')['External_Table_Columns'].apply(list).reset_index(name='Column_DDL')
    self.deltaschema['Column_DDL']                     = self.deltaschema.apply(lambda x : str(tuple(x['Column_DDL'])).replace("'",""), axis = 1)
    self.deltaschema['search_pattern']                 = self.deltaschema.apply(lambda x: fr'{search_pattern}', axis = 1)
    self.deltaschema['CREATE_EXTERNAL_TABLE_DDL']     = df.apply(lambda x : f'''
     CREATE OR REPLACE EXTERNAL TABLE {snowflake_database}.{snowflake_table}
     {x['Column_DDL']} 
     with Location @{stage_name}
     pattern = '{x['search_pattern']}'
      FILE_FORMAT = {snowflake_db}.STAGING.PARQUET_FORMAT_{snowflake_schema}
      table_format = delta
      auto_refresh = false
       ''', axis = 1)
    
     return self.deltaschema['CREATE_EXTERNAL_TABLE_DDL'].replace('\n','', regex=True) .to_string(index=False)
    

    
    
    
