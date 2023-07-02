/*1. CREATE SCHEMA (IF NEEDED)

-- CREATE SCHEMA `dmn01-rsksoi-bld-01-2017`.dmn01_rsk_mvs_raw;

/*2. CREATE SIMULATED TABLE WITH 1 MILLION RECORDS*/

/*2A. CREATE INITIAL VARIABLE FOR TABLE*/
/*--https://dba.stackexchange.com/questions/130392/generate-and-insert-1-million-rows-into-simple-table*/
/*NB: NEED TO UPDATE ROW COUNT (/100 AS p) IN BELOW CALCULATION, DEPENDING ON TABLE SIZE*/

CREATE OR REPLACE TABLE dmn01_rsk_mvs_raw.T1 AS
WITH RECURSIVE Ten AS
(
  SELECT 1  as one UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL
  SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL
  SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1
)
SELECT
row_number() over() AS id,
(row_number() over())/100 AS prop
FROM Ten
CROSS JOIN Ten T100
CROSS JOIN Ten T1000
--CROSS JOIN Ten T10000
--CROSS JOIN Ten T100000
--CROSS JOIN Ten T1000000
--CROSS JOIN Ten T10000000
--ORDER BY id
;

/*3. BUILDING THE TABLE OUT TO CREATE THE TABLE REQUIRED AS INPUT TABLE TO RWA CALCUATION*/

/*3a. Define the fields in the table*/
ALTER TABLE dmn01_rsk_mvs_raw.T1
  ADD COLUMN ACC_OWNER_ID string
  ,ADD COLUMN START_DATE date
  ;


/*3b. Simulate key variables, Step 1: Use Random Normal Variables to Generate Drawn and Undrawn.  Also allocate "||input_col||"ortions of the population to asset classes*/
/*For ref on using FARM_FINGERPRINT: --https://stackoverflow.com/questions/46019624/how-to-do-repeatable-sampling-in-bigquery-standard-sql*/

UPDATE dmn01_rsk_mvs_raw.T1
  SET PRODUCT = 'GC (exc CRE)'
  ,CRE_FLAG = 0
  ,Reg_CCF = 0.75
  ,DRAWN = (SQRT(-2*LOG(nullif(abs(cast("0x"||substring(to_hex(sha256(cast(id as string))), 61, 64) as int64)),0)/100000.000))*SIN(2*ACOS(-1)*nullif(abs(cast("0x"||substring(to_hex(sha256(cast(id+1 as string))), 61, 64) as int64)),0)/100000.000)) * 6000000 + 2000000 /*random normal with mean 2,000,000 and standard deviation 6,000,000*/
  --,PROVISIONS = dmn01_rsk_mvs_raw.cr_sample_fnPerRowRand()
  ,UNDRAWN = (SQRT(-2*LOG(nullif(abs(cast("0x"||substring(to_hex(sha256(cast(id+2 as string))), 61, 64) as int64)),0)/100000.000))*SIN(2*ACOS(-1)*nullif(abs(cast("0x"||substring(to_hex(sha256(cast(id+3 as string))), 61, 64) as int64)),0)/100000.000)) * 12000000 + 3000000 /*random normal with mean 3,000,000 and standard deviation 12,000,000*/
  --,DEFAULT_STATUS varchar(10)
  --,COUNTRY varchar(100)
  --,ASSET_CLASS_IND int
  --,RISK_GRADE int
  --,BU_ID int
  --,BUS_UNIT varchar(50)
  --,SUB_BUSINESS_UNIT_NAME varchar(50)
  --,PRA_ASSET_CLASS varchar(50)
  --,SECTOR varchar(50)
  --,LEGAL_ENTITY varchar(50)
  --,DEFAULT_IND int
  ,RWA_SCALING =
  CASE WHEN prop <= 0.07 AND prop >0 THEN 1
  WHEN prop <= 0.9 and prop > 0.88 THEN 1
  ELSE 0 END
  ,BASEL_APPROACH =
  CASE WHEN prop > 0.9 THEN 'Standardised'
  ELSE 'F-IRB' END
  ,BASEL_ASSET_CLASS =
  CASE WHEN prop <= 0.75 AND prop > 0 THEN 'CORPORATES - MAIN'
  WHEN prop <= 0.85 AND prop >0.75 THEN 'CORPORATES - SME'
  WHEN prop <= 0.9 AND prop >0.85 THEN 'INSTITUTIONS'
  WHEN prop <= 0.98 AND prop >0.9 THEN 'CORPORATES'
  WHEN prop <= 0.9 THEN 'INSTITUTIONS' END
  --,SLOTTING_CATEGORY int
  ,MATURITY = (SQRT(-2*LOG(nullif(abs(cast("0x"||substring(to_hex(sha256(cast(id+4 as string))), 61, 64) as int64)),0)/100000.000))*SIN(2*ACOS(-1)*nullif(abs(cast("0x"||substring(to_hex(sha256(cast(id+5 as string))), 61, 64) as int64)),0)/100000.000)) * 1.5 + 2.5 /*random normal with mean 2,000,000 and standard deviation 6,000,000*/
  ,TURNOVER = (SQRT(-2*LOG(nullif(abs(cast("0x"||substring(to_hex(sha256(cast(id+6 as string))), 61, 64) as int64)),0)/100000.000))*SIN(2*ACOS(-1)*nullif(abs(cast("0x"||substring(to_hex(sha256(cast(id+7 as string))), 61, 64) as int64)),0)/100000.000)) * 10 + 25 /*random normal with mean 2,000,000 and standard deviation 6,000,000*/
  ,SME_SCALAR_IND =
  CASE WHEN prop <= 0.85 AND prop > 0.75 THEN 1 /*F-IRB Corporate SME all set to have SME scalar*/
  WHEN prop <= 0.75 AND prop > 0.72 THEN 1 /*F-IRB Corporate Main: small portion set to have SME scalar*/
  WHEN prop <= 0.86 AND prop > 0.85 THEN 1 /*F-IRB Institutions: small portion set to have SME scalar*/
  ELSE 0 END
  ,SME_SCALAR_VALUE =
  CASE WHEN prop <= 0.85 AND prop > 0.75 THEN 0.7619 /*F-IRB Corporate SME all set to have SME scalar*/
  WHEN prop <= 0.75 AND prop > 0.72 THEN 0.7619 /*F-IRB Corporate Main: small portion set to have SME scalar*/
  WHEN prop <= 0.86 AND prop > 0.85 THEN 0.7619 /*F-IRB Institutions: small portion set to have SME scalar*/
  ELSE 1 END
  ,RATING_ID = 'corp'
  ,LGD_EXTERNAL = 45/100.00
  --,R_EAD float
  ,RAND_NORM_1 = (SQRT(-2*LOG(nullif(abs(cast("0x"||substring(to_hex(sha256(cast(id+8 as string))), 61, 64) as int64)),0)/100000.000))*SIN(2*ACOS(-1)*nullif(abs(cast("0x"||substring(to_hex(sha256(cast(id+9 as string))), 61, 64) as int64)),0)/100000.000))
  ,RAND_NORM_2 = (SQRT(-2*LOG(nullif(abs(cast("0x"||substring(to_hex(sha256(cast(id+10 as string))), 61, 64) as int64)),0)/100000.000))*SIN(2*ACOS(-1)*nullif(abs(cast("0x"||substring(to_hex(sha256(cast(id+11 as string))), 61, 64) as int64)),0)/100000.000))
  ,RAND_UNIF_1 = abs(cast("0x"||substring(to_hex(sha256(cast(id+12 as string))), 61, 64) as int64))/100000.000
  ,RAND_UNIF_2 = abs(cast("0x"||substring(to_hex(sha256(cast(id+13 as string))), 61, 64) as int64))/100000.000
  WHERE true;


/*3c. Merge on rating information to generate PDs and Provision rates.
To control distribution across rating grades, took "||input_col||"ortions from Global Corporates (exc CRE) Dec, generated cumulative percentiles, turned those into thresholds in normal space
and then compared random norm variables to thresholds.
This step merges on info based on the random normal variable outturn compared to the thresholds
RATING_INFO table imported into DB as a csv file*/

create or replace table dmn01_rsk_mvs_raw.T2 as
select *
from dmn01_rsk_mvs_raw.T1
join dmn01_rsk_mvs_raw.RATING_INFO
--option SUBINTERVAL
on T1.RAND_NORM_1 < RATING_INFO.FREQ_THRESH_UPPER and T1.RAND_NORM_1 >= RATING_INFO.FREQ_THRESH_LOWER;

drop table dmn01_rsk_mvs_raw.T1;

/*3d. Cap and Floor fields where ap"||input_col||"riate (wouldn't need to do that if 'Greatest' and 'Least' functions supported).
Also make 'second step' adjustments where field values depend on outcomes of first update*/

UPDATE dmn01_rsk_mvs_raw.T2
  SET
  R_PD = R_PD_PC/100.0000
  ,DRAWN =
  CASE WHEN DRAWN < 0 THEN 0
  ELSE DRAWN END

  ,UNDRAWN =
  CASE WHEN UNDRAWN < 0 THEN 0
  ELSE UNDRAWN END

  ,PROVISIONS =
  CASE WHEN (DRAWN < 0) AND (UNDRAWN < 0) THEN 0
  WHEN (DRAWN < 0) AND (UNDRAWN >0) THEN UNDRAWN * 0.5 * PROV_RATE_PC / 100.00
  WHEN (DRAWN > 0) AND (UNDRAWN <0) THEN DRAWN * PROV_RATE_PC / 100.00
  ELSE (DRAWN + 0.5 * UNDRAWN) * PROV_RATE_PC / 100.00 END

  ,DEFAULT_STATUS =
  CASE WHEN CMS_RATING = 20 THEN 'DEF'
  ELSE 'NOT_DEF' END

  ,COUNTRY =
  /*BOS COUNTRY DISTRIBUTION*/
  CASE WHEN (RAND_UNIF_1 <= 0.09580038) AND (RAND_UNIF_2 <= 0.002325581) THEN 'AMERICAS'
  WHEN (RAND_UNIF_1 <= 0.09580038) AND (RAND_UNIF_2 > 0.002325581) AND (RAND_UNIF_2 <= 0.004651163) THEN 'MIDDLE EAST'
  WHEN (RAND_UNIF_1 <= 0.09580038) AND (RAND_UNIF_2 > 0.004651163) AND (RAND_UNIF_2 <= 0.990697674) THEN 'UNITED KINGDOM OF GREAT BRITAIN AND NORTHERN IRELAND'
  WHEN (RAND_UNIF_1 <= 0.09580038) AND (RAND_UNIF_2 > 0.990697674) AND (RAND_UNIF_2 <= 1) THEN 'UNITED STATES OF AMERICA'

  /*LLOYDS COUNTRY DISTRIBUTION*/
  WHEN (RAND_UNIF_1 > 0.09580038) AND (RAND_UNIF_1 <= 0.84159519) AND (RAND_UNIF_2 <= 0.0003970881) THEN 'AFRICA'
  WHEN (RAND_UNIF_1 > 0.09580038) AND (RAND_UNIF_1 <= 0.84159519) AND (RAND_UNIF_2 > 0.0003970881) AND (RAND_UNIF_2 <= 0.0018937284) THEN 'AMERICAS'
  WHEN (RAND_UNIF_1 > 0.09580038) AND (RAND_UNIF_1 <= 0.84159519) AND (RAND_UNIF_2 > 0.0018937284) AND (RAND_UNIF_2 <= 0.005426870) THEN 'ASIA'
  WHEN (RAND_UNIF_1 > 0.09580038) AND (RAND_UNIF_1 <= 0.84159519) AND (RAND_UNIF_2 > 0.005426870) AND (RAND_UNIF_2 <= 0.060754467) THEN 'EURO AREA'
  WHEN (RAND_UNIF_1 > 0.09580038) AND (RAND_UNIF_1 <= 0.84159519) AND (RAND_UNIF_2 > 0.060754467) AND (RAND_UNIF_2 <= 0.064857710) THEN 'GERMANY'
  WHEN (RAND_UNIF_1 > 0.09580038) AND (RAND_UNIF_1 <= 0.84159519) AND (RAND_UNIF_2 > 0.064857710) AND (RAND_UNIF_2 <= 0.070284580) THEN 'OTHER EUROPE'
  WHEN (RAND_UNIF_1 > 0.09580038) AND (RAND_UNIF_1 <= 0.84159519) AND (RAND_UNIF_2 > 0.070284580) AND (RAND_UNIF_2 <= 0.9522171) THEN 'UNITED KINGDOM OF GREAT BRITAIN AND NORTHERN IRELAND'
  WHEN (RAND_UNIF_1 > 0.09580038) AND (RAND_UNIF_1 <= 0.84159519) AND (RAND_UNIF_2 > 0.9522171) THEN 'UNITED STATES OF AMERICA'

  /*LLOYDS COUNTRY DISTRIBUTION*/
  WHEN (RAND_UNIF_1 > 0.84159519) AND (RAND_UNIF_2 <= 0.001779359) THEN 'AMERICAS'
  WHEN (RAND_UNIF_1 > 0.84159519) AND (RAND_UNIF_2 > 0.001779359) AND (RAND_UNIF_2 <= 0.049822064) THEN 'ASIA'
  WHEN (RAND_UNIF_1 > 0.84159519) AND (RAND_UNIF_2 > 0.049822064) AND (RAND_UNIF_2 <= 0.078291815) THEN 'EURO AREA'
  WHEN (RAND_UNIF_1 > 0.84159519) AND (RAND_UNIF_2 > 0.078291815) AND (RAND_UNIF_2 <= 0.080071174) THEN 'GERMANY'
  WHEN (RAND_UNIF_1 > 0.84159519) AND (RAND_UNIF_2 > 0.080071174) AND (RAND_UNIF_2 <= 0.122775801) THEN 'OTHER EUROPE'
  WHEN (RAND_UNIF_1 > 0.84159519) AND (RAND_UNIF_2 > 0.122775801) AND (RAND_UNIF_2 <= 0.4252669) THEN 'UNITED KINGDOM OF GREAT BRITAIN AND NORTHERN IRELAND'
  WHEN (RAND_UNIF_1 > 0.84159519) AND (RAND_UNIF_2 > 0.4252669) THEN 'UNITED STATES OF AMERICA'

  END

  ,ASSET_CLASS_IND =
  CASE WHEN BASEL_ASSET_CLASS = 'CORPORATES - SME' THEN 2
  WHEN BASEL_ASSET_CLASS = 'RETAIL, OF WHICH SME' THEN 3
  WHEN BASEL_ASSET_CLASS = 'RETAIL MORTGAGES' THEN 4
  WHEN BASEL_ASSET_CLASS = 'SUPERVISORY SLOTTING' THEN 5
  WHEN BASEL_APPROACH ='Standardised' THEN 6
  WHEN RWA_SCALING = 1 THEN 7
  ELSE 1 END

  ,RISK_GRADE = CMS_RATING
  --,BU_ID int
  --,BUS_UNIT varchar(50)
  --,SUB_BUSINESS_UNIT_NAME varchar(50)
  --,PRA_ASSET_CLASS varchar(50)
  --,SECTOR varchar(50)

  ,LEGAL_ENTITY =
  CASE WHEN RAND_UNIF_1 <= 0.09580038 THEN 'BOS'
  WHEN (RAND_UNIF_1 > 0.09580038) AND (RAND_UNIF_1 <= 0.84159519) THEN 'LLOY'
  WHEN (RAND_UNIF_1 > 0.84159519) AND (RAND_UNIF_1 <= 1) THEN 'LBCM'
  END

  ,DEFAULT_IND =
  CASE WHEN CMS_RATING = 20 THEN 1
  ELSE 0 END
  --,RWA_SCALING float
  --,SLOTTING_CATEGORY int
  ,MATURITY =
  CASE WHEN MATURITY < 1 THEN 1
  WHEN MATURITY > 5 THEN 5
  ELSE MATURITY END

  ,TURNOVER =
  CASE WHEN TURNOVER < 5 THEN 5
  WHEN TURNOVER > 50 THEN 50
  ELSE TURNOVER END

  --,SME_SCALAR
  --,RATING_ID
  --,LGD_EXTERNAL

  ,R_EAD =
  CASE WHEN (DRAWN < 0) AND (UNDRAWN < 0) THEN 0
  WHEN (DRAWN < 0) AND (UNDRAWN > 0) THEN Reg_CCF * UNDRAWN
  WHEN (DRAWN > 0) AND (UNDRAWN < 0) THEN DRAWN
  ELSE DRAWN + Reg_CCF * UNDRAWN END

  WHERE true;