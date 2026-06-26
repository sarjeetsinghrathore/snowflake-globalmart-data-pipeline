--  golden layer 

create or replace schema Global_mart_db ;
 


select count(transaction_id ), count( distinct transaction_id) from global_mart_db.staging.stg_csv_transaction;
 select *  from GLOBAL_MART_DB.STAGING.STAGE_CSV_TRANSACTION ;


-- create or replace table global_mart_db.marts.daily_sales_fact(
--  report_date timestamp,--
--  store_id string,
--  store_city string,
--  store_name string,
--  category string,
--  total_revenue_generated int,
--  Store_region string,
--  total_units_sold int,
--  total_transaction_done int,--
--  average_cart_size int,--*
--  total_unique_customer int,
--  date_update date) --
--  ;




          create or replace table global_mart_db.marts.daily_sales_fact as 
  select 
          transaction_date as report_date,
          Store_region,
          store_city ,
          store_name,
          store_id,
          category ,
          
          sum(Total_amount) as  total_revenue_generated,  --  Revenue: (Units Sold × Price) -  Discounts  
           sum(quantity) as total_units_sold ,
            count(transaction_id) as total_transaction_done,
            avg(Total_amount) as average_cart_size,
             count(distinct customer_id) as total_unique_customer,
             current_date as date_update
             
          from   GLOBAL_MART_DB.STAGING.STAGE_CSV_TRANSACTION
          group by transaction_date ,Store_region,store_city ,store_name,store_id  , category  order by transaction_date,store_id;
 select * from global_mart_db.marts.daily_sales_fact;

 --------------- table 2 --------------

 /*with csv_cte as (
select store_id,store_name, category,sum(line_total) as line_total ,sum(quantity) as number_of_unites_sold,count(distinct customer_id) as unique_customer_id   from GLOBAL_MART_DB.STAGING.STAGE_CSV_TRANSACTION group by store_id ,store_name,category)
,
 parque_cte as (
select store_id , category , avg(unit_cost) as per_item_cost from  global_mart_db.staging.stg_parquet_erp group by store_id,category  order by store_id,category)


select
  p.store_id,
  c.store_name,
  p.category,
  c.line_total as total_revenue_generated,
   c.number_of_unites_sold *p.per_item_cost as  total_cost_generated,
  (c.line_total -total_cost_generated) as gross_profit_margin ,
  (gross_profit_margin/total_revenue_generated)*100 as gross_margin_percentage,
  c.number_of_unites_sold,
   c.unique_customer_id 
  from csv_cte as c   
  join parque_cte as p 
  on  LOWER(trim(p.store_id)) = LOWER(trim(c.store_id))  
  and LOWER(trim(p.category)) = LOWER(trim(c.category))  
  order by  c.store_id,p.category;
*/




  ---- 2. gross margin fact
CREATE OR REPLACE table  global_mart_db.marts.gross_mergen_fct as
WITH csv_cte AS (
    SELECT store_id, store_name,  category,
        SUM(quantity) AS number_of_units_sold,
        SUM(line_total) AS line_total,
        COUNT(DISTINCT customer_id) AS unique_customer_id
    FROM GLOBAL_MART_DB.STAGING.STAGE_CSV_TRANSACTION s
    GROUP BY store_id, store_name, category
),
parquet_cte AS (
    SELECT store_id, category,
    AVG(total_cost / ifnull(quantity_received, 0)) AS per_item_cost
    FROM global_mart_db.staging.stg_parquet_orders
    GROUP BY store_id, category
)

SELECT
    p.store_id,  c.store_name, p.category,
    c.line_total AS total_revenue_generated, 
    (c.number_of_units_sold * p.per_item_cost) AS total_cost_generated,
    c.line_total - (c.number_of_units_sold * p.per_item_cost) AS gross_profit_margin,
    ( ( c.line_total -  (c.number_of_units_sold * p.per_item_cost) ) / c.line_total) * 100 AS gross_margin_percentage,
    c.number_of_units_sold,
    c.unique_customer_id
FROM csv_cte c
JOIN parquet_cte p
    ON LOWER(c.store_id) = LOWER(p.store_id)
    AND LOWER(c.category) = LOWER(p.category)
ORDER BY c.store_id,p.category;


select * from global_mart_db.marts.gross_mergen_fct;




---
-- 3. sensor_iot_fact
create or replace table  global_mart_db.marts.pivot_parq_sensor as
SELECT *
FROM (SELECT 
    date(events_ts) as event_date,
        store_id,
        store_name,
        sensor,
        sensor_value FROM global_mart_db.staging.stg_json_sensor)
PIVOT (
AVG(sensor_value) FOR sensor IN 
        (
        'footfall' AS avg_footfall,
        'weight_kg' AS avg_weight,
        'temp_c' AS avg_temp,
        'power_kw' as avg_power,
        'humidity_pct' as avg_humidity
        ))
        order by event_date, store_id;




 select * from  global_mart_db.marts.pivot_parq_sensor ;


 
