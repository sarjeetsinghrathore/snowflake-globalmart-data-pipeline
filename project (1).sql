-- select /*
-- Global_mart_db => Database

-- Storage integration schema -> storage_integration
--   format_json => format for my json file ( removed outer array )
-- s3_stage => external stage name
-- raw SCHEMA => raw table 
-- s3 => iot => iot json file 
-- */

create database if not exists Global_mart_db
COMMENT= 'GlobalMart retail data platform';

-- schema fo the storage integration (schema)
create schema if not exists Global_mart_db.storage_integration
comment ='storage integration,file formats, external stages';

describe database Global_mart_db;

use global_mart_db.storage_integration;
CREATE or replace STORAGE INTEGRATION s3_integration
type =  external_stage
storage_provider = 's3'
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::009152047350:role/snowfake_s3_data'
STORAGE_ALLOWED_LOCATIONS = ('s3://sarjeet-python-7773/');

describe storage integration s3_integration;  

CREATE OR REPLACE STAGE global_mart_db.storage_integration.s3_stage
STORAGE_INTEGRATION = s3_integration
url = 's3://sarjeet-python-7773/';

list @global_mart_db.storage_integration.s3_stage/iot;

-- Bronze -- exact copy of source files, never modified
CREATE SCHEMA IF NOT EXISTS global_mart_db.raw;

Create or replace table global_mart_db.raw.pos_batch_jan (
    transaction_id STRING,
    store_id STRING,
    store_name STRING,
    store_city STRING,
    store_region STRING,
    cashier_id STRING,
    customer_id STRING,
    transaction_date DATE,
    transaction_time TIME,
    product_sku STRING,
    product_name STRING,
    category STRING,
    subcategory STRING,
    quantity INT,
    unit_price FLOAT,
    discount_pct INT,
    total_amount FLOAT,
    payment_method STRING,
    loyalty_points INT
);

alter table global_mart_db.raw.pos_batch_jan
add column load_ts timestamp ,
source_file string;

select * from  global_mart_db.raw.pos_batch_jan;
-- create table 
select * from global_mart_db.raw.iot_events_raw ; 

select * from  global_mart_db.raw.iot_events_raw;
create or replace table global_mart_db.raw.iot_events_raw( 
    event_id string,
    event_type string,
    store_id string,
    store_name string,
    events_ts TIMESTAMP,
    device_id string,
    raw_payload variant,
    
    source_file string, 
    loaded_at TIMESTAMP 
    
    
);
select * from global_mart_db.raw.iot_events_raw;


select * from global_mart_db.raw.iot_events_raw;



select * from global_mart_db.raw.pos_batch_jan;




--  create table parquet 


-- ## parquet bronze table 
select * from global_mart_db.raw.erp_parquet_raw;
create or replace Table global_mart_db.raw.erp_parquet_raw (

order_id          VARCHAR(20),
order_date        TIMESTAMP_NTZ,
store_id          VARCHAR(20),
store_city        VARCHAR(50),
supplier_id       VARCHAR(25),
supplier_name     VARCHAR(150),
supplier_city     VARCHAR(50),
product_sku       VARCHAR(50),
category          VARCHAR(60),
quantity_ordered  INTEGER,
quantity_received INTEGER,
unit_cost         FLOAT,
total_cost        FLOAT,
order_status      VARCHAR(30),       -- pending/shipped / deliverd/delayed
expected_delivery DATE,
actual_delivery   DATE,
warehouse_id      VARCHAR(25),
lead_time_days    INTEGER,
is_late           BOOLEAN,
source_file       VARCHAR(400),
load_time         TIMESTAMP_NTZ

);


-- 







-- file format is for the json file  global_Mart_db.raw
CREATE OR REPLACE FILE FORMAT Global_mart_db.storage_integration.format_json
type     = 'JSON'
STRIP_OUTER_ARRAY = TRUE; -- FILES are arrays of object : [{....}], [{....}], => {}

USE SCHEMA GLOBAL_MART_DB.RAW;
-- table for raw data 
create or replace table GLOBAL_MART_DB.RAW.events_raw(raw_col variant);

-- copy into json raw format table 
copy into  GLOBAL_MART_DB.RAW.events_raw
from @Global_mart_db.storage_integration.s3_stage/iot
FILE_FORMAT = (FORMAT_NAME = 'global_mart_db.storage_integration.format_json');

-- fle format of csv 
create or replace file format global_mart_db.storage_integration.format_csv
    type           ='CSV'
    FIELD_DELIMITER = ','
    SKIP_HEADER      = 1
    TRIM_SPACE      = TRUE
    NULL_IF       = ('NULL' , 'null', '','N/A')
    DATE_FORMAT   = 'YYYY-MM-DD'
    TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS'
    COMMENT         = 'CSV FORMAT FOR POS BATCH FILES' ;



-- file format of Parquet file 
Create or Replace file format  global_mart_db.storage_integration.Parq_format
    type = PARQUET
    Compression = auto
    BINARY_AS_TEXT = FALSE
    COMMENT         = 'Parquet FORMAT FOR  Parquet FILES' ;



    
-- manuualy cpy 
COPY INTO global_mart_db.raw.pos_batch_jan
FROM (
SELECT
$1,$2,$3,$4,$5,
$6,$7,$8,$9,$10,
$11,$12,$13,$14,$15,
$16,$17,$18,$19,
CURRENT_TIMESTAMP(),
METADATA$FILENAME
FROM @global_mart_db.storage_integration.s3_stage/pos)
FILE_FORMAT = (FORMAT_NAME = 'global_mart_db.storage_integration.format_csv');
list @global_mart_db.storage_integration.s3_stage/pos;
select * from global_mart_db.raw.pos_batch_jan;



-- snowpipe of CSV

create or replace pipe Global_mart_db.storage_integration.csv_raw_pipe
auto_ingest  = true 
as 
COPY INTO global_mart_db.raw.pos_batch_jan
FROM (
SELECT
$1,$2,$3,$4,$5,
$6,$7,$8,$9,$10,
$11,$12,$13,$14,$15,
$16,$17,$18,$19,
CURRENT_TIMESTAMP(),
METADATA$FILENAME
FROM @global_mart_db.storage_integration.s3_stage/pos
)
FILE_FORMAT = (FORMAT_NAME = 'global_mart_db.storage_integration.format_csv');


DESC PIPE Global_mart_db.storage_integration.csv_raw_pipe;

select * from global_mart_db.raw.pos_batch_jan ;
list @global_mart_db.storage_integration.s3_stage/pos ;
alter pipe Global_mart_db.storage_integration.csv_raw_pipe refresh;




-- snowpipe for JSON FILE 



alter pipe global_mart_db.raw.json_pipe_raw refresh;
create or replace pipe global_mart_db.raw.json_pipe_raw
auto_ingest  = true 
as 
copy into  global_mart_db.raw.iot_events_raw
from   

 (

SELECT
  
$1:event_id::STRING,
$1:event_type::STRING,
$1:store_id::STRING,
$1:store_name::STRING,

$1:timestamp::TIMESTAMP as events_ts,     
$1:device_id::STRING,
$1 as raw_payload,
METADATA$FILENAME,
CURRENT_TIMESTAMP(),

--iot folder  me se acces kiya jo bucket me hai 

FROM @global_mart_db.storage_integration.s3_stage/iot)
FILE_FORMAT = (FORMAT_NAME = 'Global_mart_db.storage_integration.format_json');

desc pipe global_mart_db.raw.json_pipe_raw;






select * from global_mart_db.raw.erp_parquet_raw;

-- crate pipe for parquet 
desc pipe global_mart_db.raw.parquet_pipe_raw;
Create or Replace pipe global_mart_db.raw.parquet_pipe_raw

auto_ingest  = true 

as 
copy into global_mart_db.raw.erp_parquet_raw

from (


select 


$1:order_id::VARCHAR(20),

$1:order_date::TIMESTAMP_NTZ,

$1:store_id::VARCHAR(20),

$1:store_city::VARCHAR(50),

$1:supplier_id::VARCHAR(25),

$1:supplier_name::VARCHAR(150),

$1:supplier_city::VARCHAR(50),

$1:product_sku::VARCHAR(50),

$1:category::VARCHAR(60),

$1:quantity_ordered::INTEGER,

$1:quantity_received::INTEGER,

$1:unit_cost::FLOAT,

$1:total_cost::FLOAT,

$1:order_status::VARCHAR(30),

$1:expected_delivery::DATE,

$1:actual_delivery::DATE,

$1:warehouse_id::VARCHAR(25),

$1:lead_time_days::INTEGER,

$1:is_late::BOOLEAN,

METADATA$FILENAME,

CURRENT_TIMESTAMP()

FROM @global_mart_db.storage_integration.s3_stage/parquet

)

FILE_FORMAT = (
FORMAT_NAME = 'global_mart_db.storage_integration.Parq_format'
);









-- create stream 
create or replace stream global_mart_db.raw.stream_csv_raw
on table global_mart_db.raw.pos_batch_jan
append_only = True;


select * from global_mart_db.raw.stream_csv_raw ;

create or replace stream global_mart_db.raw.stream_json_raw
on   table global_mart_db.raw.iot_events_raw
append_only = True;


select * from  global_mart_db.raw.stream_json_raw;

alter pipe global_mart_db.raw.json_pipe_raw refresh;



create or replace stream global_mart_db.raw.stream_parquet_raw
on   table global_mart_db.raw.erp_parquet_raw
append_only = True;

select * from  global_mart_db.raw.stream_parquet_raw ; 












-- ##############################################################
--                       Silver Layer 
--##################################################################


-- create schema
CREATE SCHEMA IF NOT EXISTS GLOBAL_MART_DB.STAGING;


truncate table GLOBAL_MART_DB.STAGING.STAGE_CSV_TRANSACTION;
 select * from GLOBAL_MART_DB.STAGING.STAGE_CSV_TRANSACTION ;
CREATE  OR REPLACE TABLE GLOBAL_MART_DB.STAGING.STAGE_CSV_TRANSACTION
(
 transaction_id STRING,
    store_id STRING,
    store_name STRING,
    store_city STRING,
    store_region STRING,
    cashier_id STRING,
    customer_id STRING,
    transaction_date DATE,
    transaction_time TIME,
    product_sku STRING,
    product_name STRING,
    category STRING,
    subcategory STRING,
    quantity INT,
    unit_price FLOAT,
    discount_pct INT,
    total_amount FLOAT,
    payment_method STRING,
    loyalty_points INT,
     load_ts timestamp,
     source_file string,

     transactions_ts timestamp, -- new add for date+time
     line_total float,
     processing_time timestamp ); -- new add for processing time 

select * from GLOBAL_MART_DB.STAGING.STAGE_CSV_TRANSACTION;


-- create table json (silver Layer )
-- create table for *******json file*********** Target Table

create or replace table global_mart_db.staging.stg_json_sensor (
    event_id string,
    event_type string,
    store_id string,
    store_name string,
    events_ts TIMESTAMP,
    device_id string,
    
    firmware VARCHAR, -- add new columns
    battery_pct INT,
    store_floor_sensor INT,
    sensor VARCHAR,
    sensor_value float ,
    sensor_unit varchar,
    
    source_file string,
    loaded_at TIMESTAMP , 
    processing_time TIMESTAMP
);



-- create table of parquet (silver layer )

select * from global_mart_db.staging.stg_parquet_orders ;
create or replace table global_mart_db.staging.stg_parquet_orders (

order_id          VARCHAR(20),
order_date        TIMESTAMP_NTZ,
store_id          VARCHAR(20),
store_city        VARCHAR(50),
supplier_id       VARCHAR(25),
supplier_name     VARCHAR(150),
supplier_city     VARCHAR(50),
product_sku       VARCHAR(50),
category          VARCHAR(60),
quantity_ordered  INTEGER,
quantity_received INTEGER,
unit_cost         FLOAT,
total_cost        FLOAT,
order_status      VARCHAR(30),       -- pending/shipped / deliverd/delayed
expected_delivery DATE,
actual_delivery   DATE,
warehouse_id      VARCHAR(25),
lead_time_days    INTEGER,
is_late           BOOLEAN,
source_file       VARCHAR(400),
load_time         TIMESTAMP_NTZ,
processing_time  TIMESTAMP_NTZ

)
DATA_RETENTION_TIME_IN_DAYS = 10;














 select * from global_mart_db.staging.stg_json_sensor;

 -- merge into json by task 
create or replace task global_mart_db.staging.json_merge_task

    warehouse = compute_wh
    schedule = '2 minute'
    
    as
    
    merge into global_mart_db.staging.stg_json_sensor  t
    
     using 
     (  
    select 
    event_id,
    event_type,
    store_id,
    store_name,
    events_ts,
    device_id,
    
    raw_payload:metadata:firmware::VARCHAR AS firmware,
    raw_payload:metadata:battery_pct::INT AS battery_pct,
    raw_payload:metadata:store_floor::INT AS store_floor_sensor,
    
    f.value:sensor::VARCHAR AS sensor,
    f.value:value::VARCHAR AS sensor_value,
    f.value:unit::varchar as sensor_unit,
    
    source_file,
    loaded_at,
    CURRENT_TIMESTAMP() AS processing_time,
    
    METADATA$ACTION,
    METADATA$ISUPDATE 
    
    FROM global_mart_db.raw.stream_json_raw, 
    LATERAL FLATTEN(input => raw_payload:readings) f 
    
    )  s
    
    ON t.event_id = s.event_id
    
    WHEN MATCHED
    
    AND s.METADATA$ACTION = 'INSERT'
    AND s.METADATA$ISUPDATE  = TRUE
    
    THEN UPDATE SET
    
    t.event_type = s.event_type,
    t.store_id = s.store_id,
    t.store_name = s.store_name,
    t.events_ts = s.events_ts,
    t.device_id = s.device_id,
    t.firmware = s.firmware,
    t.battery_pct = s.battery_pct,
    t.store_floor_sensor = s.store_floor_sensor,
    t.sensor = s.sensor,
    t.sensor_value = s.sensor_value,
    t.sensor_unit = s.sensor_unit,
    t.source_file = s.source_file,
    t.loaded_at = s.loaded_at,
    t.processing_time = s.processing_time
    
    when not matched 
    
    AND s.METADATA$ACTION = 'INSERT'
    AND s.METADATA$ISUPDATE  = false
    
    then 
        insert
    
        ( event_id,
        event_type,
        store_id,
        store_name,
        events_ts,
        device_id,
        firmware,
        battery_pct,
        store_floor_sensor,
        sensor,
        sensor_value,
        sensor_unit,
        source_file,
        loaded_at,
        processing_time 
        )
        values(
    
            s.event_id,
            s.event_type,
            s.store_id,
            s.store_name,
            s.events_ts,
            s.device_id,
            s.firmware,
            s.battery_pct,
            s.store_floor_sensor,
            s.sensor,
            s.sensor_value,
            s.sensor_unit,
            s.source_file,
            s.loaded_at,
            s.processing_time 
            )
    
            when matched 
            AND s.METADATA$ACTION = 'DELETE'
           AND s.METADATA$ISUPDATE  = false
    
           then delete ;

ALTER TASK global_mart_db.staging.json_merge_task suspend;
ALTER TASK global_mart_db.staging.json_merge_task resume;
show tasks;








-- create task for csv 
-- merge into csv
create or replace task GLOBAL_MART_DB.STAGING.csv_merge_task

warehouse = compute_wh
schedule = '2 minute' 

as




merge into GLOBAL_MART_DB.STAGING.STAGE_CSV_TRANSACTION s


using (
 

select 
 
    transaction_id,
    store_id,
    store_name,
    store_city,
    store_region,
    cashier_id,
    customer_id,
    transaction_date,
    transaction_time,
     product_sku,
    product_name,
    
    upper(category) as category , 

     subcategory,

         CASE
            WHEN  quantity > 0 THEN quantity
            ELSE 0
        END AS quantity , 

    CASE

        WHEN unit_price > 0 THEN unit_price
    
        ELSE 0
    
    END AS unit_price ,

    case 
        when discount_pct >=0 then discount_pct
        else 0
    end as discount_pct,
        

   
    total_amount,
   
    

    CASE

WHEN UPPER(payment_method) = 'CREDIT CARD'
THEN 'CC'

WHEN UPPER(payment_method) = 'DEBIT CARD'
THEN 'DC'

ELSE payment_method

END AS payment_method,


loyalty_points,
 load_ts,
    source_file,
    
  TO_TIMESTAMP(CONCAT(transaction_date,' ', transaction_time )) AS transaction_ts,    
      
 (quantity*unit_price)-(quantity*unit_price*discount_pct/100) as line_total,

    CURRENT_TIMESTAMP() AS processing_time,
    METADATA$ACTION ,
    METADATA$ISUPDATE 
-- FROM global_mart_db.raw.pos_batch_jan ;
 from global_mart_db.raw.stream_csv_raw 
  ) st

 on s.transaction_id = st.transaction_id
when matched 
and
st.METADATA$ACTION = 'INSERT'
And st.METADATA$ISUPDATE = TRUE

then update set 
s.store_id = st.store_id,
s.store_name = st.store_name,
s.store_city = st.store_city,
s.store_region = st.store_region,
s.cashier_id = st.cashier_id,
s.customer_id = st.customer_id,
s.transaction_date = st.transaction_date,
s.transaction_time = st.transaction_time,
s.transactions_ts = st.transaction_ts,
s.product_sku = st.product_sku,
s.product_name = st.product_name,
s.category = st.category,
s.subcategory = st.subcategory,
s.quantity = st.quantity,
s.unit_price = st.unit_price,
s.discount_pct = st.discount_pct,
s.total_amount = st.total_amount,
s.line_total = st.line_total,
s.payment_method = st.payment_method,
s.loyalty_points = st.loyalty_points,
s.load_ts = st.load_ts,
s.source_file = st.source_file,
s.processing_time = st.processing_time


when not matched and 
st.METADATA$ACTION = 'INSERT' and st.METADATA$ISUPDATE = 'FALSE'

then insert (
transaction_id,
store_id,
store_name,
store_city,
store_region,
cashier_id,
customer_id,
transaction_date,
transaction_time,
transactions_ts,
product_sku,
product_name,
category,
subcategory,
quantity,
unit_price,
discount_pct,
total_amount,

payment_method,
loyalty_points,
load_ts,
source_file,
line_total,
processing_time
)
VALUES (

st.transaction_id,
st.store_id,
st.store_name,
st.store_city,
st.store_region,
st.cashier_id,
st.customer_id,
st.transaction_date,
st.transaction_time,
st.transaction_ts,
st.product_sku,
st.product_name,
st.category,
st.subcategory,
st.quantity,
st.unit_price,
st.discount_pct,
st.total_amount,
st.payment_method,
st.loyalty_points,
st.load_ts,
st.source_file,
st.line_total,
st.processing_time   


)


when matched and 

 st.METADATA$ACTION = 'DELETE' and st.METADATA$ISUPDATE = 'FALSE'

THEN DELETE;


alter task GLOBAL_MART_DB.STAGING.csv_merge_task suspend; 

show tasks;


select * from GLOBAL_MART_DB.STAGING.STAGE_CSV_TRANSACTION s;
 

    








merge into global_mart_db.staging.stg_json_sensor  t

 using 
 ( 
select 
event_id,
event_type,
store_id,
store_name,
events_ts,
device_id,

raw_payload:metadata:firmware::VARCHAR AS firmware,
raw_payload:metadata:battery_pct::INT AS battery_pct,
raw_payload:metadata:store_floor::INT AS store_floor_sensor,

f.value:sensor::VARCHAR AS sensor,
f.value:value::VARCHAR AS sensor_value,
f.value:unit::varchar as sensor_unit,

source_file,
loaded_at,
CURRENT_TIMESTAMP() AS processing_time,

METADATA$ACTION,
METADATA$ISUPDATE

FROM global_mart_db.raw.stream_json_row, 
LATERAL FLATTEN(input => raw_payload:readings) f 

)  s

ON t.event_id = s.event_id

WHEN MATCHED

AND s.METADATA$ACTION = 'INSERT'
AND s.METADATA$ISUPDATE  = TRUE

THEN UPDATE SET

t.event_type = s.event_type,
t.store_id = s.store_id,
t.store_name = s.store_name,
t.events_ts = s.events_ts,
t.device_id = s.device_id,
t.firmware = s.firmware,
t.battery_pct = s.battery_pct,
t.store_floor_sensor = s.store_floor_sensor,
t.sensor = s.sensor,
t.sensor_value = s.sensor_value,
t.sensor_unit = s.sensor_unit,
t.source_file = s.source_file,
t.loaded_at = s.loaded_at,
t.processing_time = s.processing_time

when not matched 

AND s.METADATA$ACTION = 'INSERT'
AND s.METADATA$ISUPDATE  = false

then 
    insert

    ( event_id,
    event_type,
    store_id,
    store_name,
    events_ts,
    device_id,
    firmware,
    battery_pct,
    store_floor_sensor,
    sensor,
    sensor_value,
    sensor_unit,
    source_file,
    loaded_at,
    processing_time 
    )
    values(

        s.event_id,
        s.event_type,
        s.store_id,
        s.store_name,
        s.events_ts,
        s.device_id,
        s.firmware,
        s.battery_pct,
        s.store_floor_sensor,
        s.sensor,
        s.sensor_value,
        s.sensor_unit,
        s.source_file,
        s.loaded_at,
        s.processing_time 
        )

        when matched 
        AND s.METADATA$ACTION = 'DELETE'
       AND s.METADATA$ISUPDATE  = false

       then delete ;

select * from global_mart_db.staging.stg_json_sensor;









-- merge into parquet file

create or replace task global_mart_db.staging.Parq_ord_merge_task

    warehouse = compute_wh
    schedule = '2 minute'
    
    as

merge into  global_mart_db.staging.stg_parquet_orders  sp 

using (
select 
        order_id,
        order_date,
        store_id,
        store_city,
        supplier_id,
        supplier_name,
        supplier_city,
        product_sku,
        category,
        quantity_ordered,
        quantity_received,
        unit_cost,
        total_cost,
        order_status,
        expected_delivery,
        actual_delivery,
        warehouse_id,
        lead_time_days,
        is_late,
        source_file,
        load_time,

        CURRENT_TIMESTAMP() AS processing_time,

        METADATA$ACTION ,
        METADATA$ISUPDATE

     from global_mart_db.raw.stream_parquet_raw ) st


ON sp.order_id = st.order_id



when matched and 

 st.METADATA$ACTION = 'DELETE' and st.METADATA$ISUPDATE = FALSE

THEN DELETE 


when matched then update
set
    sp.order_status      = st.order_status,
    sp.quantity_received = st.quantity_received,
    sp.actual_delivery   = st.actual_delivery,
    sp.is_late        = st.is_late,
    sp.processing_time      = st.processing_time


when not matched and 
st.METADATA$ACTION = 'INSERT' and st.METADATA$ISUPDATE = FALSE
then insert (
            
       order_id,
        order_date,
        store_id,
        store_city,
        supplier_id,
        supplier_name,
        supplier_city,
        product_sku,
        category,
        quantity_ordered,
        quantity_received,
        unit_cost,
        total_cost,
        order_status,
        expected_delivery,
        actual_delivery,
        warehouse_id,
        lead_time_days,
        is_late,
        source_file,
        load_time,
        processing_time  )

values (

        st.order_id,
       st.order_date,
        st.store_id,
        st.store_city,
        st.supplier_id,
        st.supplier_name,
        st.supplier_city,
        st.product_sku,
        st.category,
        st.quantity_ordered,
        st.quantity_received,
        st.unit_cost,
        st.total_cost,
        st.order_status,
        st.expected_delivery,
        st.actual_delivery,
        st.warehouse_id,
        st.lead_time_days,
        st.is_late,
        st.source_file,
        st.load_time,
        st.processing_time );


show tasks ; 
ALTER TASK global_mart_db.staging.Parq_ord_merge_task suspend ; 
ALTER TASK global_mart_db.staging.Csv_merge_task suspend;
show tasks;


select * from global_mart_db.staging.stg_parquet_orders ;
--desc task global_mart_db.staging.Parq_ord_merge_tas;