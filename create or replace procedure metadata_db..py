create or replace procedure metadata_db.dbo.generic_processor_py (domain varchar, db_type varchar, etl_name varchar,etl_runid varchar,input_database varchar,input_schema varchar, input_table varchar,destination_database varchar,destination_schema varchar, destination_table varchar, load_mode varchar,etl_start timestamp, source_count int, integrity_check boolean, data_monitoring boolean, monitoring_column varchar)
--returns table (number_of_rows_inserted int, number_of_rows_updated int, number_of_rows_deleted int)
returns string
LANGUAGE PYTHON

RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'          
EXECUTE AS CALLER  
as 
$$
import pandas as pd
import datetime
import pytz
from dateutil.relativedelta import relativedelta

def run(session, domain, db_type, etl_name, etl_runid, input_database,input_schema, input_table,destination_database,destination_schema, destination_table, load_mode, etl_start, source_count,integrity_check, data_monitoring,monitoring_column):
    
    
    show_table_columns = pd.DataFrame(session.sql("select distinct column_name, ordinal_position from \
    "+str(destination_database)+".information_schema.columns where table_name = '"+str(destination_table)+"' and table_schema = '"+str(destination_schema)+"' and \
    table_catalog = '"+str(destination_database)+"' \
    and column_name not in ('AUDIT_INSERTED','AUDIT_UPDATED') order by ordinal_position").collect()) 
    
    show_table_columns['update_column_names'] = ('target.'+show_table_columns['COLUMN_NAME']+' = source.'+show_table_columns['COLUMN_NAME'])
    show_table_columns['insert_column_names'] = ('source.'+show_table_columns['COLUMN_NAME'])
    show_table_columns['update_columns_where_not_same'] = ('target.'+show_table_columns['COLUMN_NAME']+' <> source.'+show_table_columns['COLUMN_NAME'])
    table_column_names = ','.join(show_table_columns['COLUMN_NAME'])
    update_column_names = ','.join(show_table_columns['update_column_names'])
    insert_column_names = ','.join(show_table_columns['insert_column_names'])
    update_columns_where_not_same = ' OR '.join(show_table_columns['update_columns_where_not_same'])
    if load_mode == 'FULL':
           
            session.sql("truncate table "+destination_database+"."+destination_schema+"."+destination_table).collect()
            results = pd.DataFrame(session.sql("insert into "+destination_database+"."+destination_schema+"."+destination_table+" ("+table_column_names+") select distinct "+table_column_names+" from "+input_database+"."+input_schema+"."+input_table).collect())
            results['number of rows deleted'] = 0
            results['number of rows updated'] = 0
    
    elif load_mode == 'DELTA':
            query = "SHOW PRIMARY KEYS IN "+destination_database+"."+destination_schema+"."+destination_table
            show_primary_keys = pd.DataFrame(session.sql("SHOW PRIMARY KEYS IN "+destination_database+"."+destination_schema+"."+destination_table).collect())
            show_primary_keys['primary_keys'] = ('target.'+show_primary_keys['column_name']+' = source.'+show_primary_keys['column_name'])
            show_primary_keys['primary_keys_incremental'] = ('target.'+show_primary_keys['column_name']+' is null')
            primary_keys = ' AND '.join(show_primary_keys['primary_keys'])
            primary_keys_incremental = ' AND '.join(show_primary_keys['primary_keys_incremental'])
            
            delta_update = pd.DataFrame(session.sql("update "+destination_database+"."+destination_schema+"."+destination_table+" as target set "+update_column_names+",target.AUDIT_UPDATED = current_timestamp() from "+input_database+"."+input_schema+"."+input_table+" source where "+primary_keys+" and ("+update_columns_where_not_same+")").collect())
            
            
            
            delta_insert = pd.DataFrame(session.sql("insert into "+destination_database+"."+destination_schema+"."+destination_table+" ("+table_column_names+") select distinct "+insert_column_names+" from "+input_database+"."+input_schema+"."+input_table+" source left join "+destination_database+"."+destination_schema+"."+destination_table+" target on "+primary_keys+" where "+primary_keys_incremental).collect())
            result_frames = [delta_update, delta_insert]
            results = pd.concat(result_frames, axis = 1)
            results['number of rows deleted'] = 0
            
    
    
    
    elif load_mode == 'PERIODREPLACE':
            current_period = (datetime.datetime.now()).strftime("%b-%Y")
            previous_period = (datetime.datetime.now()- relativedelta(months=1)).strftime("%b-%Y")
            
            periodreplace_delete = pd.DataFrame(session.sql("delete from "+destination_database+"."+destination_schema+"."+destination_table+" where UPPER("+monitoring_column+") in (UPPER('"+current_period+"'),UPPER('"+previous_period+"'))").collect())
            
            periodreplace_insert = pd.DataFrame(session.sql("insert into "+destination_database+"."+destination_schema+"."+destination_table+" ("+table_column_names+") select distinct "+table_column_names+" from "+input_database+"."+input_schema+"."+input_table+"  where UPPER("+monitoring_column+") in (UPPER('"+current_period+"'),UPPER('"+previous_period+"'))").collect())
            result_frames = [periodreplace_delete, periodreplace_insert]
            results = pd.concat(result_frames, axis = 1)
            results['number of rows updated'] = 0
    
    else:
            raise
            
            
    destination_count = (pd.DataFrame(session.sql("select count(*) as count from "+destination_database+"."+destination_schema+"."+destination_table).collect()))['COUNT'].to_string(index=False)        
    etl_end = datetime.datetime.now()
    etl_end = etl_end.astimezone(pytz.timezone('America/Chicago'))
    
    session.sql("insert into metadata_db.dbo.etl_log_data_processor (domain,db_type,etl_name,etl_runid,input_database,input_schema,input_object,destination_database,destination_schema,destination_table,load_mode,inserted_rows,updated_rows,deleted_rows,source_count,destination_count,etl_start,etl_end,etl_status) values ('"+domain+"', '"+db_type+"', '"+etl_name+"', '"+etl_runid+"', '"+input_database+"', '"+input_schema+"', '"+input_table+"', '"+destination_database+"','"+destination_schema+"','"+destination_table+"', '"+load_mode+"', '"+str(results['number of rows inserted'].to_string(index=False))+"', '"+str(results['number of rows updated'].to_string(index=False))+"', '"+str(results['number of rows deleted'].to_string(index=False))+"','"+str(source_count)+"','"+str(destination_count)+"', '"+str(etl_start)+"', '"+str(etl_end)+"', 'Success')").collect()
    
    final_result = 'Inserted: '+str(results['number of rows inserted'].to_string(index=False))+'; Updated: '+str(results['number of rows updated'].to_string(index=False))+'; Deleted: '+str(results['number of rows deleted'].to_string(index=False))
      
    return final_result
    
    #return query
    
$$
;