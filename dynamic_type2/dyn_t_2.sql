/*
 * 
 * Slowly changing dimensions
 * 
 * 
 */
 
 
--------------------------------------------------------------------
-- setting up the warehouse and environment
--------------------------------------------------------------------
CREATE WAREHOUSE IF NOT EXISTS demo_wh WAREHOUSE_SIZE = XSMALL;
/* or replace 'demo_wh' in the script below 
   with the name of an existing warehouse that 
   you have access to. */

CREATE OR REPLACE DATABASE dyn_test;
CREATE OR REPLACE SCHEMA dyn_t_2; 



---------------------------------------------------------------------------------------------------------------------
-- Prepare the base tables
---------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE TABLE source_system_customer
(
 customer_id         number(38,0) NOT NULL,
 name                varchar NOT NULL,
 address             varchar NOT NULL,
 location_id         number(38,0) NOT NULL,
 phone               varchar(15) NOT NULL,
 account_balance_usd number(12,2) NOT NULL,
 market_segment      varchar(10) NOT NULL,
 comment             varchar COMMENT 'user comments',

 CONSTRAINT pk_customer PRIMARY KEY ( customer_id ) 
)
COMMENT = 'loaded from snowflake_sample_data.tpch_sf10.customer'
AS 
SELECT c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment  
FROM snowflake_sample_data.tpch_sf10.customer
;

CREATE OR REPLACE TABLE src_customer
(
 customer_id         number(38,0) NOT NULL,
 name                varchar NOT NULL,
 address             varchar NOT NULL,
 location_id         number(38,0) NOT NULL,
 phone               varchar(15) NOT NULL,
 account_balance_usd number(12,2) NOT NULL,
 market_segment      varchar(10) NOT NULL,
 comment             varchar COMMENT 'base load of one fourth of total records',
 __ldts              timestamp_ntz NOT NULL DEFAULT current_timestamp(),

 CONSTRAINT pk_customer PRIMARY KEY ( customer_id, __ldts )
)
COMMENT = 'source customers for loading changes'
as 
SELECT customer_id
	, name
	, address
	, location_id
	, phone
    , account_balance_usd
	, market_segment
	, comment
    , current_timestamp()
FROM source_system_customer
WHERE true 
AND MOD(customer_id,4)= 0 --load one quarter of existing recrods
;

--create a clone of src_customer for future exercises
CREATE OR REPLACE TABLE src_customer_bak CLONE src_customer; 


CREATE OR REPLACE TASK load_src_customer
WAREHOUSE = demo_wh
SCHEDULE = '10 minute'
AS 
INSERT INTO src_customer (
SELECT	customer_id
	, name
	, address
	, location_id
	, phone
    --if customer id ends in 3, vary the balance amount
    , iff(mod(customer_id,3)= 0,  (account_balance_usd+random()/100000000000000000)::number(32,2), account_balance_usd)
	, market_segment
	, comment
    , current_timestamp()
FROM source_system_customer SAMPLE (1000 ROWS)
)
;

---------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------


/*

create dynamic type 2 table

for all window functions, partition by the business key, order by the system timestamp column
ex. ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY __ldts ASC) AS __rec_version,

*/



CREATE OR REPLACE DYNAMIC TABLE dyn_customer 
TARGET_LAG = '10 MINUTE'
WAREHOUSE = demo_wh
AS
SELECT
    customer_id || '|' || __ldts AS dim_skey,
    customer_id AS customer_id,
    __ldts AS __from_dts,
    IFF(
        LEAD(__ldts) OVER (PARTITION BY customer_id ORDER BY __ldts ASC) IS NULL,  
        '9999-12-31'::TIMESTAMP_NTZ,
        DATEADD(NANOSECOND, -1, LEAD(__ldts) OVER (PARTITION BY customer_id ORDER BY __ldts ASC))
    ) AS __to_dts,
    name AS name,
    address AS address,
    location_id AS location_id,
    phone AS phone,
    account_balance_usd AS account_balance_usd,
    market_segment AS market_segment,
    comment AS comment,
    __ldts AS __ldts,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY __ldts ASC) AS __rec_version,
    IFF(__to_dts = '9999-12-31'::TIMESTAMP_NTZ, TRUE, FALSE) AS __is_latest,
    FIRST_VALUE(__ldts) OVER (PARTITION BY customer_id ORDER BY __ldts ASC) AS __create_dts,
    __ldts AS __update_dts,
    LAST_VALUE(__ldts) OVER (PARTITION BY customer_id ORDER BY __ldts ASC) AS __last_update_dts,
    SHA1_BINARY(
        NVL(UPPER(TRIM(name::VARCHAR)), '^^') || '|' ||
        NVL(UPPER(TRIM(location_id::VARCHAR)), '^^') || '|' ||
        NVL(UPPER(TRIM(phone::VARCHAR)), '^^') || '|' ||
        NVL(UPPER(TRIM(address::VARCHAR)), '^^') || '|' ||
        NVL(UPPER(TRIM(account_balance_usd::VARCHAR)), '^^') || '|' ||
        NVL(UPPER(TRIM(market_segment::VARCHAR)), '^^') || '|' ||
        NVL(UPPER(TRIM(comment::VARCHAR)), '^^')
    )::BINARY(20) AS __t2diff_hash,
    IFNULL(
        LAG(__t2diff_hash) OVER (PARTITION BY customer_id ORDER BY __ldts ASC),
        SHA1_BINARY('*null*')
    ) AS __prev_t2diff_hash,      
FROM src_customer 
WHERE TRUE 
-- do not include records without changes
QUALIFY __t2diff_hash != __prev_t2diff_hash
;



/*

create dynamic type 2 table with type 1 columns

*/

CREATE OR REPLACE DYNAMIC TABLE dyn_customer 
TARGET_LAG = '1 MINUTE'
WAREHOUSE = demo_wh
AS
SELECT *, 
--   __to_dts and __is_latest window functions now need to iterate over the final result set, not the initial select
  IFF(
      LEAD(__ldts) OVER (PARTITION BY customer_id ORDER BY __ldts ASC) IS NULL,  
      '9999-12-31'::TIMESTAMP_NTZ,
      DATEADD(NANOSECOND, -1, LEAD(__ldts) OVER (PARTITION BY customer_id ORDER BY __ldts ASC))
  ) AS __to_dts,
  IFF(__to_dts = '9999-12-31'::TIMESTAMP_NTZ, TRUE, FALSE) AS __is_latest
FROM (
    SELECT
        customer_id || '|' || __ldts AS dim_skey,
        customer_id AS customer_id,
        __ldts AS __from_dts,
        name AS name,
        -- TYPE 1 FIELDS
        LAST_VALUE(address) OVER (PARTITION BY customer_id ORDER BY __ldts ASC) AS address,
        LAST_VALUE(location_id) OVER (PARTITION BY customer_id ORDER BY __ldts ASC) AS location_id,
        LAST_VALUE(phone) OVER (PARTITION BY customer_id ORDER BY __ldts ASC) AS phone,
        -- END TYPE 1 FIELDS 
        account_balance_usd AS account_balance_usd,
        market_segment AS market_segment,
        comment AS comment,
        __ldts AS __ldts,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY __ldts ASC) AS __rec_version,
        FIRST_VALUE(__ldts) OVER (PARTITION BY customer_id ORDER BY __ldts ASC) AS __create_dts,
        src_customer.__ldts AS __update_dts,
        LAST_VALUE(__ldts) OVER (PARTITION BY customer_id ORDER BY __ldts ASC) AS __last_update_dts,
        SHA1_BINARY(
            NVL(UPPER(TRIM(name::VARCHAR)), '^^') || '|' ||
            NVL(UPPER(TRIM(account_balance_usd::VARCHAR)), '^^') || '|' ||
            NVL(UPPER(TRIM(market_segment::VARCHAR)), '^^') || '|' ||
            NVL(UPPER(TRIM(comment::VARCHAR)), '^^')
        )::BINARY(20) AS __t2diff_hash,
        IFNULL(
            LAG(__t2diff_hash) OVER (PARTITION BY customer_id ORDER BY __ldts ASC),
            SHA1_BINARY('*null*')
        ) AS __prev_t2diff_hash,      
        SHA1_BINARY(
            NVL(UPPER(TRIM(location_id::VARCHAR)), '^^') || '|' ||
            NVL(UPPER(TRIM(phone::VARCHAR)), '^^') || '|' ||
            NVL(UPPER(TRIM(address::VARCHAR)), '^^')
        )::BINARY(20) AS __t1diff_hash
    FROM src_customer 
    WHERE TRUE 
    QUALIFY __t2diff_hash != __prev_t2diff_hash
)
;
