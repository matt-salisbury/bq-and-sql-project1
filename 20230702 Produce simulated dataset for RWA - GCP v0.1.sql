/*1. CREATE SCHEMA (IF NEEDED)*/

--CREATE SCHEMA `dmn01-rskcda-ide-17-a7be`.dmn01_rsk_mvs_b3p1;

/*2. CREATE SIMULATED TABLE WITH 1 MILLION RECORDS*/

/*2A. CREATE INITIAL VARIABLE FOR TABLE*/
/*--https://dba.stackexchange.com/questions/130392/generate-and-insert-1-million-rows-into-simple-table*/
/*NB: NEED TO UPDATE ROW COUNT (/100 AS p) IN BELOW CALCULATION, DEPENDING ON TABLE SIZE*/

CREATE OR REPLACE TABLE dmn01_rsk_mvs_b3p1.T1 AS
WITH RECURSIVE Ten AS
(
  SELECT 1  as one UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL
  SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL
  SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1
)
SELECT
row_number() over() AS id,
(row_number() over())/1000 AS prop
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
ALTER TABLE dmn01_rsk_mvs_b3p1.T1
  add column FACILITY_ID string
  ,add column CUSTOMER_ID string
  ,add column LBG_PRODUCT_CODE string
  ,add column LBG_MODEL_CODE float64
  ,add column CRAG_ID string
  ,add column LARGE_EXPOSURE_GROUP_ID string
  ,add column COUNTRY_OF_DOMICLE string
  ,add column UK_LOCAL_AUTHORITY_IND int64
  ,add column DRAWN_PRE_CRM float64
  ,add column CRM float64
  ,add column PRODUCT_TYPE_FOR_CCF_CLASSIFICATION string
  ,add column OFF_BALANCE_SHEET_EXP_PRE_CCF float64
  ,add column ST_EXPOSURE_CLASS string
  ,add column RFB_FLAG int64
  ,add column RFB_SUBSIDIARY_FLAG int64
  ,add column NRFB_SUBSIDIARY_FLAG int64
  ,add column LEGAL_ENTITY_DETAILED string
  ,add column COMPANY_CODE_SNS_SPLIT string
  ,add column ECAI_INSTITUTION_RATING_OBLIGOR string
  ,add column ECAI_RATING_SCALE_OBLIGOR string
  ,add column ECAI_INSTITION_RATING_SHORT_TERM_EXPOSURE_SPECIFIC string
  ,add column ECAI_RATING_SCALE_SHORT_TERM_EXPOSURE_SPECIFIC string
  ,add column EXP_SPECIFIC_SHORT_TERM_ECAI_RATING string
  ,add column ECAI_INSTITION_RATING_SHORT_TERM_FROM_ANY_OTHER_EXPOSURE string
  ,add column ECAI_RATING_SCALE_SHORT_TERM_FROM_ANY_OTHER_EXPOSURE string
  ,add column WORST_SHORT_TERM_ECAI_RATING_FROM_ANY_OTHER_EXPOSURE string
  ,add column EXPORT_CREDIT_AGENCY_RATING_MEIP int64
  ,add column EXPORT_CREDIT_AGENCY_INSTITUTION string
  ,add column MATURITY_AT_ORIGINATION float64
  ,add column EXP_RELATED_TO_GOODS_CROSS_INT_BORDERS_FLAG int64
  ,add column FI_CET1_RATIO float64
  ,add column FI_LEVERAGE_RATIO float64
  ,add column OBLIGOR_SOVEREIGN string
  ,add column ECAI_INSTITION_RATING_SOVEREIGN string
  ,add column SOVEREIGN_ECAI_RATING string
  ,add column SOVEREIGN_FLOOR_APPLICABLE int64
  ,add column UNRATED_INSTITION_CREDIT_ASSESSMENT string
  ,add column UNRATED_CORP_INV_GRADE_IND int64
  ,add column CORP_SPECIALIST_LENDING_CAT int64
  ,add column TRANSACTOR_CLASSIFICATION int64
  ,add column EXP_TO_OWN_PENSION_FUND_OR_OWN_EMPLOYEE int64
  ,add column OBLIGOR_CLASSIFICATION_IND_OR_FIRM string
  ,add column GROUP_TOTAL_GROSS_EXP_INC_CON_COUNTERPARTIES float64
  ,add column PRODUCT_TYPE_FOR_RETAIL_CLASSIFICATION string
  ,add column RETAIL_RESIDENTIAL_IND int64
  ,add column RE_IND int64
  ,add column RE_UNHEDGED_LENDING_WITH_CUR_MISMATCH float64
  ,add column RE_LE_BOOKING_HOLDS_1ST_CHARGE int64
  ,add column RE_CRE_IND int64
  ,add column RE_DEV_FLAG int64
  ,add column SECURED_FLAG int64
  ,add column RE_2ND_CHARGES_AT_PARI_PASSU_INC_LOAN float64
  ,add column RE_TOTAL_OTHER_CLAIMS_ON_PROPERTY_TO_BE_DEDUCTED_FROM_VALUATION float64
  ,add column RE_VAL_AT_ORIG float64
  ,add column RE_DATE_OF_ORIG_VAL float64
  ,add column RE_VAL_AT_LATEST_FA float64
  ,add column RE_DATE_OF_VAL_AT_LATEST_FA float64
  ,add column RE_VAL_AT_LATEST_VAL float64
  ,add column RE_REASON_FOR_LATEST_VAL string
  ,add column RE_LTV_AT_ORIG_PRESENT_AT_20250101 int64
  ,add column RE_INDEX_VALUATION float64
  ,add column RE_DATE_OF_INDEX_VALUATION date
  ,add column RE_VAL_PLEDGED_OFFSET_DEPOSIT float64
  ,add column RE_DATE_OF_VALUE_OF_LEDGED_OFFSET_DEPOSIT float64
  ,add column RE_SECURED_ON_HMO int64
  ,add column RE_DEPENDS_ON_RENTS int64
  ,add column RE_PRIMARY_RESIDENCE int64
  ,add column RE_OBLIGOR_NUM_PROPERTIES_EXC_PRIMARY_RES int64
  ,add column SOCIAL_HOUSING_FLAG int64
  ,add column RE_ADC_PRE_SALES_SIGNIF int64
  ,add column RE_ADC_BOR_EQ_SIGNIF int64
  ,add column DEFAULT_IND int64
  ,add column PROVISIONS float64
  ,add column HIGH_RISK_ITEM_IND int64
  ,add column COVERED_BOND_IND int64
  ,add column CIU_IND int64
  ,add column EQUITY_IND int64
  ,add column EQUITY_CAP_DEDUCT_TREAT_IND int64
  ,add column EQ_ARTICLE_89_IND int64
  ,add column EQ_ARTICLE_48_IND int64
  ,add column VC_IND int64
  ,add column SUB_DEBT_IND int64
  ,add column OTHER_ITEMS_TYPE string
  ,add column TURNOVER_LATEST float64
  ,add column TURNOVER_1YR_PRIOR float64
  ,add column TURNOVER_2YR_PRIOR float64
  ,add column TOTAL_ASSESTS_OBLIGOR float64
  ,add column SME_IND int64
  ,add column RESIDUAL_VALUE_IND int64
  ,add column RESIDUAL_VALUE float64
  ,add column YEARS_TO_RESIDUAL_VALUE_REALISATION float64
  --END OF B3p1 STANDARDISED REQUIRED VARIABLES
  ,add column BASEL_RESIDUAL_MATURITY float64
  ,add column IRB_APPROACH string
  ,add column IRB_EXPOSURE_SUB_CLASS string
  ,add column ROLL_OUT_ASSET_CLASS string
  ,add column LARGE_OR_UNREG_FINANCIAL_SECTOR_ENT_IND int64
  ,add column CORP_SME_IND int64
  ,add column HVCRE_IND int64
  ,add column IRB_RATING_SCALE string
  ,add column IRB_RATING_GRADE string
  ,add column SLOTTING_RATING_GRADE int64
  ,add column R_LGD float64
  ,add column R_CCF float64
  ;


/*3b. Simulate key variables, Step 1: Use Random Normal Variables to Generate Drawn and Undrawn.  Also allocate "||input_col||"ortions of the population to asset classes*/
/*For ref on using FARM_FINGERPRINT: --https://stackoverflow.com/questions/46019624/how-to-do-repeatable-sampling-in-bigquery-standard-sql*/

UPDATE dmn01_rsk_mvs_b3p1.T1
  SET PRODUCT = 'GC (exc CRE)'
  ,CRE_FLAG = 0
  ,Reg_CCF = 0.75
  ,DRAWN = (SQRT(-2*LOG(nullif(abs(cast("0x"||substring(to_hex(sha256(cast(id as string))), 61, 64) as int64)),0)/100000.000))*SIN(2*ACOS(-1)*nullif(abs(cast("0x"||substring(to_hex(sha256(cast(id+1 as string))), 61, 64) as int64)),0)/100000.000)) * 6000000 + 2000000 /*random normal with mean 2,000,000 and standard deviation 6,000,000*/
  --,PROVISIONS = dmn01_rsk_mvs_b3p1.cr_sample_fnPerRowRand()
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

create or replace table dmn01_rsk_mvs_b3p1.T2 as
select *
from dmn01_rsk_mvs_b3p1.T1
join dmn01_rsk_mvs_b3p1.RATING_INFO
--option SUBINTERVAL
on T1.RAND_NORM_1 < RATING_INFO.FREQ_THRESH_UPPER and T1.RAND_NORM_1 >= RATING_INFO.FREQ_THRESH_LOWER;

drop table dmn01_rsk_mvs_b3p1.T1;

/*3d. Cap and Floor fields where ap"||input_col||"riate (wouldn't need to do that if 'Greatest' and 'Least' functions supported).
Also make 'second step' adjustments where field values depend on outcomes of first update*/

UPDATE dmn01_rsk_mvs_b3p1.T2
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