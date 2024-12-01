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


--create the type 2 dimension as a standard table for performance testing
CREATE OR REPLACE TABLE dim_customer
(
    CUSTOMER_ID number(38,0) NOT NULL, 
    __FROM_DTS timestamp_ntz NOT NULL DEFAULT CURRENT_TIMESTAMP() COMMENT 'when record was loaded into the warehouse', 
    __TO_DTS timestamp_ntz NOT NULL DEFAULT CURRENT_TIMESTAMP() COMMENT 'effective date or surrogate high date (9999-12-31)', 
    NAME varchar(16777216) NOT NULL, 
    ADDRESS varchar(16777216) NOT NULL, 
    LOCATION_ID number(38,0) NOT NULL, 
    PHONE varchar(15) NOT NULL, 
    ACCOUNT_BALANCE_USD number(12,2) NOT NULL, 
    MARKET_SEGMENT varchar(10) NOT NULL, 
    COMMENT varchar(16777216), 
    __LOAD_DTS timestamp_ltz(9), 
    __REC_VERSION number(38,0) NOT NULL DEFAULT 1 COMMENT 'incremental change version counter for the record', 
    __IS_LATEST boolean NOT NULL DEFAULT true COMMENT 'true only on the latest effective dated record', 
    __CREATE_DTS timestamp_ntz NOT NULL DEFAULT CURRENT_TIMESTAMP() COMMENT 'date when record was first created', 
    __UPDATE_DTS timestamp_ntz NOT NULL DEFAULT CURRENT_TIMESTAMP() COMMENT 'date when record was last updated', 
    __T2DIFF_HASH binary(20) NOT NULL COMMENT 'hash of all columns used for quick compare', 
    __T1DIFF_HASH binary(20) NOT NULL COMMENT 'hash of all columns used for quick compare'
    , CONSTRAINT  PK_dim_CUSTOMER_1 PRIMARY KEY (CUSTOMER_ID, __FROM_DTS) RELY 
    
)
;



---------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------


/*

create dynamic type 2 table

for all window functions, partition by the business key, order by the system timestamp column
ex. ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY __ldts ASC) AS __rec_version,

*/



CREATE OR REPLACE DYNAMIC TABLE dyn_customer_t2 
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
    ) AS __prev_t2diff_hash      
FROM src_customer 
WHERE TRUE 
-- do not include records without changes
QUALIFY __t2diff_hash != __prev_t2diff_hash
;



/*

create dynamic type 2 table with type 1 columns

*/

CREATE OR REPLACE DYNAMIC TABLE dyn_customer_t2n1 
TARGET_LAG = '10 MINUTE'
WAREHOUSE = demo_wh
AS
SELECT *, 
--   system column window functions now need to iterate over the final result set, not the initial select
  IFF(
      LEAD(__ldts) OVER (PARTITION BY customer_id ORDER BY __ldts ASC) IS NULL,  
      '9999-12-31'::TIMESTAMP_NTZ,
      DATEADD(NANOSECOND, -1, LEAD(__ldts) OVER (PARTITION BY customer_id ORDER BY __ldts ASC))
  ) AS __to_dts,
  ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY __ldts ASC) AS __rec_version,
  FIRST_VALUE(__ldts) OVER (PARTITION BY customer_id ORDER BY __ldts ASC) AS __create_dts,
  LAST_VALUE(__ldts) OVER (PARTITION BY customer_id ORDER BY __ldts ASC) AS __last_update_dts,
  IFF(__to_dts = '9999-12-31'::TIMESTAMP_NTZ, TRUE, FALSE) AS __is_latest
FROM (
    SELECT
        customer_id || '|' || __ldts AS dim_skey,
        customer_id AS customer_id,        
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
        __ldts AS __update_dts,
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
        )::BINARY(20) AS __t1diff_hash,
        __ldts AS __from_dts
    FROM src_customer 
    WHERE TRUE 
    QUALIFY __t2diff_hash != __prev_t2diff_hash
)
;





/*

load the type 2 dimension as a standard table for performance testing

*/

MERGE INTO dim_customer AS TxObject
USING (
    WITH logic AS ( 
        with CUSTOMER as (
    SELECT
        CUSTOMER_ID,
        NAME,
        ADDRESS,
        LOCATION_ID,
        PHONE,
        ACCOUNT_BALANCE_USD,
        MARKET_SEGMENT,
        COMMENT,
        __ldts as __LOAD_DTS
    FROM SRC_CUSTOMER
    WHERE TRUE 
    AND __ldts  = (SELECT MAX(__ldts) FROM SRC_CUSTOMER)  

)
,
common as (
    SELECT
        CUSTOMER.CUSTOMER_ID,
        __LOAD_DTS  AS __FROM_DTS,
        '9999-12-31'::TIMESTAMP_NTZ AS __TO_DTS,
        CUSTOMER.NAME,
        CUSTOMER.ADDRESS,
        CUSTOMER.LOCATION_ID,
        CUSTOMER.PHONE,
        CUSTOMER.ACCOUNT_BALANCE_USD,
        CUSTOMER.MARKET_SEGMENT,
        CUSTOMER.COMMENT,
        CUSTOMER.__LOAD_DTS,
        1 AS __REC_VERSION,
        true AS __IS_LATEST,
        CURRENT_TIMESTAMP() AS __CREATE_DTS,
        CURRENT_TIMESTAMP() AS __UPDATE_DTS,
        SHA1_BINARY(
            NVL(UPPER(TRIM(CUSTOMER_ID::VARCHAR)),'^^') || '|' || 
            NVL(UPPER(TRIM(NAME::VARCHAR)),'^^') || '|' || 
            NVL(UPPER(TRIM(ADDRESS::VARCHAR)),'^^') || '|' || 
            NVL(UPPER(TRIM(LOCATION_ID::VARCHAR)),'^^') || '|' || 
            NVL(UPPER(TRIM(PHONE::VARCHAR)),'^^') || '|' || 
            NVL(UPPER(TRIM(ACCOUNT_BALANCE_USD::VARCHAR)),'^^') || '|' || 
            NVL(UPPER(TRIM(MARKET_SEGMENT::VARCHAR)),'^^') || '|' || 
            NVL(UPPER(TRIM(COMMENT::VARCHAR)),'^^') 
                )::BINARY(20) AS __T2DIFF_HASH,
        SHA1_BINARY('*null*')::BINARY(20) AS __T1DIFF_HASH
    FROM CUSTOMER
)

SELECT * FROM common 
    )

    , new_recs AS (
    SELECT logic.* 
    , 'new recs' as __tmp_update_type      
    FROM logic 
    LEFT JOIN dim_customer  dim
    ON 
        logic.CUSTOMER_ID = dim.CUSTOMER_ID  
    
    WHERE
    dim.CUSTOMER_ID IS NULL
    )

    , t2_insert AS (
    SELECT
        logic.CUSTOMER_ID, 
        
        logic.__FROM_DTS, 
        
        logic.__TO_DTS, 
        
        logic.NAME, 
        
        logic.ADDRESS, 
        
        logic.LOCATION_ID, 
        
        logic.PHONE, 
        
        logic.ACCOUNT_BALANCE_USD, 
        
        logic.MARKET_SEGMENT, 
        
        logic.COMMENT, 
        
        logic.__LOAD_DTS, 
        
        dim.__REC_VERSION  + 1 AS __REC_VERSION,
        logic.__IS_LATEST, 
        
        dim.__CREATE_DTS,
        logic.__UPDATE_DTS, 
        
        logic.__T2DIFF_HASH, 
        
        logic.__T1DIFF_HASH
           
    , 't2 insert' as __tmp_update_type       
    FROM logic 
    LEFT JOIN dim_customer dim 
    ON 
        logic.CUSTOMER_ID = dim.CUSTOMER_ID  
    WHERE TRUE  
    AND dim.__IS_LATEST = 'Y'
    AND logic.__T2DIFF_HASH != dim.__T2DIFF_HASH
    )

    , t2_expire as (
    SELECT
        dim.CUSTOMER_ID, 
        
        dim.__FROM_DTS, 
        
        DATEADD(NANOSECOND,-1, logic.__FROM_DTS) AS __TO_DTS,
        dim.NAME, 
        
        dim.ADDRESS, 
        
        dim.LOCATION_ID, 
        
        dim.PHONE, 
        
        dim.ACCOUNT_BALANCE_USD, 
        
        dim.MARKET_SEGMENT, 
        
        dim.COMMENT, 
        
        dim.__LOAD_DTS, 
        
        dim.__REC_VERSION, 
        
        FALSE AS __IS_LATEST,
        dim.__CREATE_DTS, 
        
        dim.__UPDATE_DTS, 
        
        dim.__T2DIFF_HASH, 
        
        dim.__T1DIFF_HASH
            
    , 't2 expire' as __tmp_update_type      
    FROM logic 
    INNER JOIN dim_customer dim 
    ON 
        logic.CUSTOMER_ID = dim.CUSTOMER_ID  
    WHERE TRUE  
    AND dim.__IS_LATEST = 'Y'
    AND logic.__T2DIFF_HASH != dim.__T2DIFF_HASH    

    ) 

 

    , allChanges as (
        SELECT * FROM new_recs
        UNION ALL
        SELECT * FROM t2_insert 
        UNION ALL
        SELECT * FROM t2_expire    
    )

    SELECT * FROM allChanges


) AS TxLogic 


ON  
        TxObject.CUSTOMER_ID = TxLogic.CUSTOMER_ID 
    AND  TxObject.__REC_VERSION = TxLogic.__REC_VERSION

WHEN MATCHED 
THEN UPDATE
    SET         
        TxObject.CUSTOMER_ID = TxLogic.CUSTOMER_ID,         
        TxObject.__FROM_DTS = TxLogic.__FROM_DTS,         
        TxObject.__TO_DTS = TxLogic.__TO_DTS,         
        TxObject.NAME = TxLogic.NAME,         
        TxObject.ADDRESS = TxLogic.ADDRESS,         
        TxObject.LOCATION_ID = TxLogic.LOCATION_ID,         
        TxObject.PHONE = TxLogic.PHONE,         
        TxObject.ACCOUNT_BALANCE_USD = TxLogic.ACCOUNT_BALANCE_USD,         
        TxObject.MARKET_SEGMENT = TxLogic.MARKET_SEGMENT,         
        TxObject.COMMENT = TxLogic.COMMENT,         
        TxObject.__LOAD_DTS = TxLogic.__LOAD_DTS,         
        TxObject.__REC_VERSION = TxLogic.__REC_VERSION,         
        TxObject.__IS_LATEST = TxLogic.__IS_LATEST,         
        TxObject.__CREATE_DTS = TxLogic.__CREATE_DTS,         
        TxObject.__UPDATE_DTS = TxLogic.__UPDATE_DTS,         
        TxObject.__T2DIFF_HASH = TxLogic.__T2DIFF_HASH,         
        TxObject.__T1DIFF_HASH = TxLogic.__T1DIFF_HASH
WHEN NOT MATCHED THEN
    INSERT (
        CUSTOMER_ID,
        __FROM_DTS,
        __TO_DTS,
        NAME,
        ADDRESS,
        LOCATION_ID,
        PHONE,
        ACCOUNT_BALANCE_USD,
        MARKET_SEGMENT,
        COMMENT,
        __LOAD_DTS,
        __REC_VERSION,
        __IS_LATEST,
        __CREATE_DTS,
        __UPDATE_DTS,
        __T2DIFF_HASH,
        __T1DIFF_HASH)
    VALUES (
        TxLogic.CUSTOMER_ID,
        TxLogic.__FROM_DTS,
        TxLogic.__TO_DTS,
        TxLogic.NAME,
        TxLogic.ADDRESS,
        TxLogic.LOCATION_ID,
        TxLogic.PHONE,
        TxLogic.ACCOUNT_BALANCE_USD,
        TxLogic.MARKET_SEGMENT,
        TxLogic.COMMENT,
        TxLogic.__LOAD_DTS,
        TxLogic.__REC_VERSION,
        TxLogic.__IS_LATEST,
        TxLogic.__CREATE_DTS,
        TxLogic.__UPDATE_DTS,
        TxLogic.__T2DIFF_HASH,
        TxLogic.__T1DIFF_HASH)
;
