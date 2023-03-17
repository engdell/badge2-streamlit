--DEMO - SETUP

CREATE DATABASE IF NOT EXISTS buildlocal;

USE DATABASE buildlocal;

CREATE WAREHOUSE IF NOT EXISTS buildlocal_wh
  WAREHOUSE_SIZE = 'X-SMALL'
  AUTO_SUSPEND = 600
  AUTO_RESUME = TRUE
  MIN_CLUSTER_COUNT = 1
  MAX_CLUSTER_COUNT = 1;

USE WAREHOUSE buildlocal_wh;

--DEMO - SEMI STRUCTURED DATA

CREATE OR REPLACE TABLE my_table (
  id INT,
  my_json_column VARIANT
);

INSERT INTO my_table
    select column1 as id, parse_json(column2) as my_json_column
    from values 	(1, '{"name": "Daniel", "city": "Dallas", "ssn": "123-45-6789"}'), 
                		(2, '{"name": "Joe", "city": "San Francisco", "ssn": "456-12-6789"}'), 
                		(3, '{"name": "Mike", "city": "New York", "ssn": "789-45-6789"}'),
                		(4, '{"name": "Leif", "city": "Stockholm", "ssn": "987-65-4321"}');

SELECT * FROM buildlocal.public.my_table;

SELECT * FROM my_table
WHERE my_json_column:city = 'Stockholm';

--DEMO - ZERO COPY CLONING

CREATE OR REPLACE TABLE my_table_clone CLONE my_table;

SELECT * FROM my_table_clone
WHERE my_json_column:city = 'Stockholm';



--DEMO - TIME TRAVEL
-- Drop the table
DROP TABLE my_table;

-- Returns an ERROR because the table was deleted!
SELECT * FROM my_table
WHERE my_json_column:city = 'Stockholm';

--Get the table back!
UNDROP TABLE my_table;

-- It works again!
SELECT * FROM my_table
WHERE my_json_column:city = 'Stockholm';



--DEMO - EXTERNAL TABLE

create or replace stage CITIBIKE_STAGE
  url = "s3://sfquickstarts/VHOL Snowflake for Data Lake/Data/"  
  file_format=(type=parquet);

select $1 from @citibike_stage/2019 limit 100;




--DEMO - TAG-BASED MASKING

CREATE OR REPLACE TABLE my_users_table AS 
SELECT id, my_json_column:name::string as name, my_json_column:city::string as city, my_json_column:ssn::string as ssn from my_table;
SELECT * FROM my_users_table;
CREATE OR REPLACE tag buildlocal_PII;
CREATE OR REPLACE masking policy buildlocal_MASK_PII as (val string) returns string ->
  case
    when current_role() IN ('ACCOUNTADMIN') then '***MASKED***'
  end;
ALTER tag buildlocal_PII set masking policy buildlocal_MASK_PII;
ALTER TABLE my_users_table ALTER COLUMN SSN set tag buildlocal_PII = 'tag-based policies';
SELECT * FROM my_users_table;
