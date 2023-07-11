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
  ,add column ECAI_RATING_GRADE_OBLIGOR string
  ,add column ECAI_INSTITUTION_RATING_SHORT_TERM_EXPOSURE_SPECIFIC string
  ,add column ECAI_RATING_SCALE_SHORT_TERM_EXPOSURE_SPECIFIC string
  ,add column EXP_SPECIFIC_SHORT_TERM_ECAI_RATING string
  ,add column ECAI_INSTITUTION_RATING_SHORT_TERM_FROM_ANY_OTHER_EXPOSURE string
  ,add column ECAI_RATING_SCALE_SHORT_TERM_FROM_ANY_OTHER_EXPOSURE string
  ,add column WORST_SHORT_TERM_ECAI_RATING_FROM_ANY_OTHER_EXPOSURE string
  ,add column EXPORT_CREDIT_AGENCY_RATING_MEIP int64
  ,add column EXPORT_CREDIT_AGENCY_INSTITUTION string
  ,add column MATURITY_AT_ORIGINATION float64
  ,add column EXP_RELATED_TO_GOODS_CROSS_INT_BORDERS_FLAG int64
  ,add column FI_CET1_RATIO float64
  ,add column FI_LEVERAGE_RATIO float64
  ,add column OBLIGOR_SOVEREIGN string
  ,add column ECAI_INSTITUTION_RATING_SOVEREIGN string
  ,add column ECAI_RATING_SCALE_SOVEREIGN string
  ,add column SOVEREIGN_ECAI_RATING string
  ,add column SOVEREIGN_FLOOR_APPLICABLE int64
  ,add column UNRATED_INSTITUTION_CREDIT_ASSESSMENT string
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
  ,add column RE_DATE_OF_ORIG_VAL date
  ,add column RE_VAL_AT_LATEST_FA float64
  ,add column RE_DATE_OF_VAL_AT_LATEST_FA date
  ,add column RE_VAL_AT_LATEST_VAL float64
  ,add column RE_DATE_OF_LATEST_VAL date
  ,add column RE_REASON_FOR_LATEST_VAL string
  ,add column RE_LTV_AT_ORIG_PRESENT_AT_20250101 int64
  ,add column RE_INDEX_VALUATION float64
  ,add column RE_DATE_OF_INDEX_VALUATION date
  ,add column RE_VAL_PLEDGED_OFFSET_DEPOSIT float64
  ,add column RE_DATE_OF_VALUE_OF_LEDGED_OFFSET_DEPOSIT date
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
  SET FACILITY_ID = cast(nullif(abs(cast("0x"||substring(to_hex(sha256(cast(id as string))), 61, 64) as int64)),0) as string)
  , CUSTOMER_ID = cast(nullif(abs(cast("0x"||substring(to_hex(sha256(cast(id as string))), 61, 64) as int64)),0) as string)
  , LBG_PRODUCT_CODE = '1000'
  --, LBG_MODEL_CODE string
  --, CRAG_ID string
  --, LARGE_EXPOSURE_GROUP_ID string
  , COUNTRY_OF_DOMICLE = 'United Kingdom of Great Britain and Northern Ireland'
  , UK_LOCAL_AUTHORITY_IND = 0
  , DRAWN_PRE_CRM = (SQRT(-2*LOG(nullif(abs(cast("0x"||substring(to_hex(sha256(cast(id as string))), 61, 64) as int64)),0)/100000.000))*SIN(2*ACOS(-1)*nullif(abs(cast("0x"||substring(to_hex(sha256(cast(id+1 as string))), 61, 64) as int64)),0)/100000.000)) * 6000000 + 2000000 /*random normal with mean 2,000,000 and standard deviation 6,000,000*/
  , CRM = (SQRT(-2*LOG(nullif(abs(cast("0x"||substring(to_hex(sha256(cast(id as string))), 61, 64) as int64)),0)/100000.000))*SIN(2*ACOS(-1)*nullif(abs(cast("0x"||substring(to_hex(sha256(cast(id+1 as string))), 61, 64) as int64)),0)/100000.000)) * 6000000 + 2000000 *
    abs(cast("0x"||substring(to_hex(sha256(cast(id+11 as string))), 61, 64) as int64))/100000.000 * 0.4 /*drawn multiplied by random number between 0 and 0.4*/
  
  , PRODUCT_TYPE_FOR_CCF_CLASSIFICATION = 
    CASE WHEN prop <= 0.89 AND prop > 0 THEN 'Acceptances'
    WHEN prop <= 0.944 AND prop >0.89 THEN 'Documentary credits with shipment as collateral and maturity < 1 year'
    WHEN prop >0.944 THEN 'Unconditionally cancellable' END
  
  , OFF_BALANCE_SHEET_EXP_PRE_CCF = (SQRT(-2*LOG(nullif(abs(cast("0x"||substring(to_hex(sha256(cast(id+2 as string))), 61, 64) as int64)),0)/100000.000))*SIN(2*ACOS(-1)*nullif(abs(cast("0x"||substring(to_hex(sha256(cast(id+3 as string))), 61, 64) as int64)),0)/100000.000)) * 12000000 + 3000000 /*random normal with mean 3,000,000 and standard deviation 12,000,000*/
  
  , ST_EXPOSURE_CLASS =
    CASE WHEN prop <= 0.058824 AND prop > 0 THEN 'Central Government and Central Banks'
    WHEN prop <= 0.11767 AND prop >0.058824 THEN 'Regional Government and Local Authorities'
    WHEN prop <= 0.176471 AND prop >0.11767 THEN 'Public Sector Entities'
    WHEN prop <= 0.235294 AND prop >0.176471 THEN 'Multilateral Development Banks'
    WHEN prop <= 0.294118 AND prop >0.235294 THEN 'Exposures to International Organisations'
    WHEN prop <= 0.352941 AND prop >0.294118 THEN 'Institutions'
    WHEN prop <= 0.411765 AND prop >0.352941 THEN 'Corporates'
    WHEN prop <= 0.470588 AND prop >0.411765 THEN 'Retail Exposures'
    WHEN prop <= 0.529412 AND prop >0.470588 THEN 'Real Estate'
    WHEN prop <= 0.588235 AND prop >0.529412 THEN 'Exposures in Default'
    WHEN prop <= 0.647059 AND prop >0.588235 THEN 'Particularly High Risk Items'
    WHEN prop <= 0.705882 AND prop >0.647059 THEN 'Covered Bonds'
    WHEN prop <= 0.764706 AND prop >0.705882 THEN 'Securitisations'
    WHEN prop <= 0.823529 AND prop >0.764706 THEN 'Exposures to institutions and corporates with short term credit assessments'
    WHEN prop <= 0.882353 AND prop >0.823529 THEN 'Exposures in the form of units or shares in Collective Investment Undertakings'
    WHEN prop <= 0.941176 AND prop >0.882353 THEN 'Subordinated Debt, Equity and Other Own Funds Investments'
    WHEN prop > 0.941176 THEN 'Other Items' END
  , RFB_FLAG =
    CASE WHEN (prop <= 0.411765 AND prop >0.352941 AND abs(cast("0x"||substring(to_hex(sha256(cast(id+62 as string))), 61, 64) as int64))/100000.000 > 0.2) THEN 1
    ELSE 0 END
  , RFB_SUBSIDIARY_FLAG =
    CASE WHEN (prop <= 0.411765 AND prop >0.352941 AND abs(cast("0x"||substring(to_hex(sha256(cast(id+62 as string))), 61, 64) as int64))/100000.000 > 0.2) THEN 1
    ELSE 0 END
  , NRFB_SUBSIDIARY_FLAG =
    CASE WHEN (prop <= 0.411765 AND prop >0.352941 AND abs(cast("0x"||substring(to_hex(sha256(cast(id+62 as string))), 61, 64) as int64))/100000.000 < 0.2) THEN 1
    ELSE 0 END
  , LEGAL_ENTITY_DETAILED = 'B N Other'
  --, COMPANY_CODE_SNS_SPLIT string
  
  , ECAI_INSTITUTION_RATING_OBLIGOR = 
    CASE WHEN prop <= 0.40 AND prop >0.37 THEN "Moody's Investor Service"
    ELSE 'No rating available' END

  , ECAI_RATING_SCALE_OBLIGOR = 
    CASE WHEN prop <= 0.40 AND prop >0.37 THEN 'Global long-term rating scale'
    ELSE 'No rating available' END
  
  , ECAI_RATING_GRADE_OBLIGOR = 
    CASE WHEN prop <= 0.375 AND prop >0.37 THEN 'Aa'
    WHEN prop <= 0.38 AND prop >0.375 THEN 'A'
    WHEN prop <= 0.3825 AND prop >0.38 THEN 'Baa'
    WHEN prop <= 0.385 AND prop >0.3825 THEN 'Ba'
    WHEN prop <= 0.395 AND prop >0.385 THEN 'B'
    WHEN prop <= 0.40 AND prop >0.395 THEN 'Caa'
    ELSE 'No rating available' END

  , ECAI_INSTITUTION_RATING_SHORT_TERM_EXPOSURE_SPECIFIC =
    CASE WHEN prop <= 0.823529 AND prop >0.764706 THEN "Moody's Investor Service" 
    ELSE 'No Rating Available' END

  , ECAI_RATING_SCALE_SHORT_TERM_EXPOSURE_SPECIFIC =
    CASE WHEN prop <= 0.823529 AND prop >0.764706 THEN 'Global short-term rating scale' 
    ELSE 'No Rating Available' END
  
  , EXP_SPECIFIC_SHORT_TERM_ECAI_RATING =
    CASE WHEN prop <= 0.823529 AND prop >0.764706 THEN 'P-1'
    ELSE 'No Rating Available' END

  , ECAI_INSTITUTION_RATING_SHORT_TERM_FROM_ANY_OTHER_EXPOSURE =
    CASE WHEN prop <= 0.823529 AND prop >0.8 THEN "Moody's Investor Service"
    ELSE 'No Rating Available' END
  
  , ECAI_RATING_SCALE_SHORT_TERM_FROM_ANY_OTHER_EXPOSURE =
    CASE WHEN prop <= 0.823529 AND prop >0.8 THEN 'Global short-term rating scale'
    ELSE 'No Rating Available' END

  , WORST_SHORT_TERM_ECAI_RATING_FROM_ANY_OTHER_EXPOSURE =
    CASE WHEN prop <= 0.823529 AND prop >0.8 THEN 'P-2'
    ELSE 'No Exposure Specific Short Term ECAI Rating Available' END
  
  , EXPORT_CREDIT_AGENCY_RATING_MEIP =
    CASE WHEN prop <= 0.345 and prop > 0.33 THEN 1
    WHEN prop <= 0.352941 and prop > 0.345 THEN 6
    ELSE 0 END

  , EXPORT_CREDIT_AGENCY_INSTITUTION = 'to be filled'
  , MATURITY_AT_ORIGINATION = round(abs(cast("0x"||substring(to_hex(sha256(cast(id+12 as string))), 61, 64) as int64))/100000.000 * 12, 1)
  , EXP_RELATED_TO_GOODS_CROSS_INT_BORDERS_FLAG =
    CASE WHEN prop <= 0.352941 and prop > 0.32 THEN 1
    ELSE 0 END
  , FI_CET1_RATIO = round(abs(cast("0x"||substring(to_hex(sha256(cast(id+72 as string))), 61, 64) as int64))/100000.000 * 4 + 12, 1)
  , FI_LEVERAGE_RATIO = round(abs(cast("0x"||substring(to_hex(sha256(cast(id+62 as string))), 61, 64) as int64))/100000.000 * 4 + 3, 1)
  , OBLIGOR_SOVEREIGN = 'UK'
  , ECAI_INSTITUTION_RATING_SOVEREIGN = "Moody's Investor Service"
  , ECAI_RATING_SCALE_SOVEREIGN = 'Global long-term rating scale'
  , SOVEREIGN_ECAI_RATING = 'Aa'
  , SOVEREIGN_FLOOR_APPLICABLE = cast(round(abs(cast("0x"||substring(to_hex(sha256(cast(id+13 as string))), 61, 64) as int64))/100000.000, 0) as int64)

  , UNRATED_INSTITUTION_CREDIT_ASSESSMENT =
  CASE WHEN abs(cast("0x"||substring(to_hex(sha256(cast(id+13 as string))), 61, 64) as int64))/100000.000 > 0.5 THEN 'A'
  ELSE 'B' END
  , UNRATED_CORP_INV_GRADE_IND =
  CASE WHEN (prop <= 0.411765 AND prop >0.352941 AND abs(cast("0x"||substring(to_hex(sha256(cast(id+9 as string))), 61, 64) as int64))/100000.000 > 0.7) THEN 1
  ELSE 0 END
  , CORP_SPECIALIST_LENDING_CAT = cast(round(abs(cast("0x"||substring(to_hex(sha256(cast(id+9 as string))), 61, 64) as int64))/100000.000 * 5, 0) as int64)
  , TRANSACTOR_CLASSIFICATION = 
  CASE WHEN abs(cast("0x"||substring(to_hex(sha256(cast(id+17 as string))), 61, 64) as int64))/100000.000 > 0.7 THEN 1
  ELSE 0 END
  , EXP_TO_OWN_PENSION_FUND_OR_OWN_EMPLOYEE = 
  CASE WHEN abs(cast("0x"||substring(to_hex(sha256(cast(id+15 as string))), 61, 64) as int64))/100000.000 > 0.9 THEN 1
  ELSE 0 END
  , OBLIGOR_CLASSIFICATION_IND_OR_FIRM  = 
  CASE WHEN abs(cast("0x"||substring(to_hex(sha256(cast(id+16 as string))), 61, 64) as int64))/100000.000 > 0.7 THEN 'Corp'
  ELSE 'Natural Person' END
  , GROUP_TOTAL_GROSS_EXP_INC_CON_COUNTERPARTIES = round(abs(cast("0x"||substring(to_hex(sha256(cast(id+19 as string))), 61, 64) as int64))/100000.000 * 1000000, 2)
  , PRODUCT_TYPE_FOR_RETAIL_CLASSIFICATION = 
  CASE WHEN prop <= 0.43 AND prop >0.411765 THEN 'Revolving facility'
  WHEN prop <= 0.44 AND prop >0.43 THEN 'Term Loan'
  WHEN prop <= 0.45 AND prop >0.44 THEN 'Credit Card'
  WHEN prop <= 0.46 AND prop >0.45 THEN 'PCA'
  WHEN prop <= 0.465 AND prop >0.46 THEN 'Vehicle Finance'
  WHEN prop <= 0.470588 AND prop >0.465 THEN 'Student Loan'
  ELSE NULL END
  , RETAIL_RESIDENTIAL_IND =
  CASE WHEN (prop <= 0.529412 AND prop >0.470588 AND abs(cast("0x"||substring(to_hex(sha256(cast(id+3 as string))), 61, 64) as int64))/100000.000 > 0.6) THEN 1
  ELSE 0 END
  , RE_IND = 
  CASE WHEN prop <= 0.529412 AND prop >0.470588 THEN 1
  WHEN (prop <= 0.588235 AND prop >0.529412 AND abs(cast("0x"||substring(to_hex(sha256(cast(id+23 as string))), 61, 64) as int64))/100000.000 > 0.5) THEN 1
  ELSE 0 END
  , RE_UNHEDGED_LENDING_WITH_CUR_MISMATCH =
  CASE WHEN (prop <= 0.470588 AND prop >0.411765 AND abs(cast("0x"||substring(to_hex(sha256(cast(id+24 as string))), 61, 64) as int64))/100000.000 > 0.5) THEN 1
  ELSE 0 END
  , RE_LE_BOOKING_HOLDS_1ST_CHARGE =
  CASE WHEN (prop <= 0.529412 AND prop >0.470588 AND abs(cast("0x"||substring(to_hex(sha256(cast(id+25 as string))), 61, 64) as int64))/100000.000 > 0.5) THEN 1
  ELSE 0 END
  , RE_CRE_IND =
  CASE WHEN (prop <= 0.529412 AND prop >0.470588 AND abs(cast("0x"||substring(to_hex(sha256(cast(id+25 as string))), 61, 64) as int64))/100000.000 > 0.2) THEN 1
  ELSE 0 END
  , RE_DEV_FLAG =
  CASE WHEN (prop <= 0.529412 AND prop >0.470588 AND abs(cast("0x"||substring(to_hex(sha256(cast(id+26 as string))), 61, 64) as int64))/100000.000 > 0.2) THEN 1
  ELSE 0 END
  , SECURED_FLAG =
  CASE WHEN (prop <= 0.529412 AND prop >0.470588 AND abs(cast("0x"||substring(to_hex(sha256(cast(id+25 as string))), 61, 64) as int64))/100000.000 > 0.6) THEN 1
  ELSE 0 END
  , RE_2ND_CHARGES_AT_PARI_PASSU_INC_LOAN =
  CASE WHEN ((prop <= 0.529412 AND prop >0.470588 AND abs(cast("0x"||substring(to_hex(sha256(cast(id+25 as string))), 61, 64) as int64))/100000.000 > 0.6) AND
  (prop <= 0.529412 AND prop >0.470588 AND abs(cast("0x"||substring(to_hex(sha256(cast(id+25 as string))), 61, 64) as int64))/100000.000 < 0.5)) THEN 1
  ELSE 0 END /*secured but don't hold the first charge*/
  , RE_TOTAL_OTHER_CLAIMS_ON_PROPERTY_TO_BE_DEDUCTED_FROM_VALUATION = 
  CASE WHEN ((prop <= 0.529412 AND prop >0.470588 AND abs(cast("0x"||substring(to_hex(sha256(cast(id+25 as string))), 61, 64) as int64))/100000.000 > 0.6) AND
  (prop <= 0.529412 AND prop >0.470588 AND abs(cast("0x"||substring(to_hex(sha256(cast(id+25 as string))), 61, 64) as int64))/100000.000 < 0.5)) THEN
  (SQRT(-2*LOG(nullif(abs(cast("0x"||substring(to_hex(sha256(cast(id as string))), 61, 64) as int64)),0)/100000.000))*SIN(2*ACOS(-1)*nullif(abs(cast("0x"||substring(to_hex(sha256(cast(id+1 as string))), 61, 64) as int64)),0)/100000.000)) * 6000000 + 2000000 *
  (abs(cast("0x"||substring(to_hex(sha256(cast(id+25 as string))), 61, 64) as int64))/100000.000 * 0.75 + 1) * 0.3 /*Valuation at origination x 0.3*/
  ELSE 0 END
  , RE_VAL_AT_ORIG = 
  CASE WHEN prop <= 0.529412 AND prop >0.470588 THEN
  (SQRT(-2*LOG(nullif(abs(cast("0x"||substring(to_hex(sha256(cast(id as string))), 61, 64) as int64)),0)/100000.000))*SIN(2*ACOS(-1)*nullif(abs(cast("0x"||substring(to_hex(sha256(cast(id+1 as string))), 61, 64) as int64)),0)/100000.000)) * 6000000 + 2000000 *
  (abs(cast("0x"||substring(to_hex(sha256(cast(id+25 as string))), 61, 64) as int64))/100000.000 * 0.75 + 1) /*Drawn balance x random factor in range 1 to 1.75*/
  ELSE NULL END

  , RE_DATE_OF_ORIG_VAL = DATE(2003, 12, 1) + id
  , RE_VAL_AT_LATEST_FA =
  CASE WHEN prop <= 0.529412 AND prop >0.470588 THEN
  (SQRT(-2*LOG(nullif(abs(cast("0x"||substring(to_hex(sha256(cast(id as string))), 61, 64) as int64)),0)/100000.000))*SIN(2*ACOS(-1)*nullif(abs(cast("0x"||substring(to_hex(sha256(cast(id+1 as string))), 61, 64) as int64)),0)/100000.000)) * 6000000 + 2000000 *
  (abs(cast("0x"||substring(to_hex(sha256(cast(id+25 as string))), 61, 64) as int64))/100000.000 * 0.75 + 1) * 1.5 /*Valuation at orig x 1.5*/
  ELSE NULL END
  , RE_DATE_OF_VAL_AT_LATEST_FA = DATE(2005, 12, 1)
  , RE_VAL_AT_LATEST_VAL =
  CASE WHEN prop <= 0.529412 AND prop >0.470588 THEN
  (SQRT(-2*LOG(nullif(abs(cast("0x"||substring(to_hex(sha256(cast(id as string))), 61, 64) as int64)),0)/100000.000))*SIN(2*ACOS(-1)*nullif(abs(cast("0x"||substring(to_hex(sha256(cast(id+1 as string))), 61, 64) as int64)),0)/100000.000)) * 6000000 + 2000000 *
  (abs(cast("0x"||substring(to_hex(sha256(cast(id+25 as string))), 61, 64) as int64))/100000.000 * 0.75 + 1) * 1.7 /*Valuation at orig x 1.7*/
  ELSE NULL END
  , RE_DATE_OF_LATEST_VAL = DATE(2007, 12, 1) + id
  , RE_REASON_FOR_LATEST_VAL =
  CASE WHEN prop <= 0.49 AND prop >0.470588 THEN 'FA'
  WHEN prop <= 0.505 AND prop >0.49 THEN 'Remort'
  WHEN prop <= 0.515 AND prop >0.505 THEN 'Recap'
  WHEN prop <= 0.529412 AND prop >0.515 THEN 'Other'
  ELSE NULL END
  , RE_LTV_AT_ORIG_PRESENT_AT_20250101 =
  CASE WHEN prop <= 0.529412 AND prop >0.470588 THEN 1
  ELSE 0 END
  , RE_INDEX_VALUATION =
  CASE WHEN prop <= 0.529412 AND prop >0.470588 THEN
  (SQRT(-2*LOG(nullif(abs(cast("0x"||substring(to_hex(sha256(cast(id as string))), 61, 64) as int64)),0)/100000.000))*SIN(2*ACOS(-1)*nullif(abs(cast("0x"||substring(to_hex(sha256(cast(id+1 as string))), 61, 64) as int64)),0)/100000.000)) * 6000000 + 2000000 *
  (abs(cast("0x"||substring(to_hex(sha256(cast(id+25 as string))), 61, 64) as int64))/100000.000 * 0.75 + 1) * 1.7 * 1.2 /*Valuation at latest valuation x 1.2*/
  ELSE NULL END
  , RE_DATE_OF_INDEX_VALUATION = 
  CASE WHEN prop <= 0.529412 AND prop >0.470588 THEN CURRENT_DATE - 31
  ELSE NULL END
  , RE_VAL_PLEDGED_OFFSET_DEPOSIT = 
  CASE WHEN prop <= 0.529412 AND prop >0.470588 THEN (SQRT(-2*LOG(nullif(abs(cast("0x"||substring(to_hex(sha256(cast(id as string))), 61, 64) as int64)),0)/100000.000))*SIN(2*ACOS(-1)*nullif(abs(cast("0x"||substring(to_hex(sha256(cast(id+1 as string))), 61, 64) as int64)),0)/100000.000)) * 6000000 + 2000000
  * 0.1 /*Drawn balance x 0.1*/
  ELSE NULL END
  , RE_DATE_OF_VALUE_OF_LEDGED_OFFSET_DEPOSIT =
  CASE WHEN prop <= 0.529412 AND prop >0.470588 THEN CURRENT_DATE
  ELSE NULL END
  , RE_SECURED_ON_HMO =
  CASE WHEN (prop <= 0.529412 AND prop >0.470588 AND abs(cast("0x"||substring(to_hex(sha256(cast(id+27 as string))), 61, 64) as int64))/100000.000 > 0.9) THEN 1
  ELSE 0 END
  , RE_DEPENDS_ON_RENTS =
  CASE WHEN (prop <= 0.529412 AND prop >0.470588 AND abs(cast("0x"||substring(to_hex(sha256(cast(id+28 as string))), 61, 64) as int64))/100000.000 > 0.8) THEN 1
  ELSE 0 END
  , RE_PRIMARY_RESIDENCE =
    CASE WHEN (prop <= 0.529412 AND prop >0.470588 AND abs(cast("0x"||substring(to_hex(sha256(cast(id+28 as string))), 61, 64) as int64))/100000.000 < 0.8) THEN 1
  ELSE 0 END  /*if not dependent on rent*/
  , RE_OBLIGOR_NUM_PROPERTIES_EXC_PRIMARY_RES =
  CASE WHEN (prop <= 0.529412 AND prop >0.470588 AND abs(cast("0x"||substring(to_hex(sha256(cast(id+28 as string))), 61, 64) as int64))/100000.000 > 0.8) THEN 
  cast(round(abs(cast("0x"||substring(to_hex(sha256(cast(id+27 as string))), 61, 64) as int64))/100000.000 * 4, 0) as int64)
  ELSE 0 END
  , SOCIAL_HOUSING_FLAG =
  CASE WHEN (prop <= 0.529412 AND prop >0.470588 AND abs(cast("0x"||substring(to_hex(sha256(cast(id+22 as string))), 61, 64) as int64))/100000.000 > 0.8) THEN 1
  ELSE 0 END
  , RE_ADC_PRE_SALES_SIGNIF =
  CASE WHEN (prop <= 0.529412 AND prop >0.470588 
  AND abs(cast("0x"||substring(to_hex(sha256(cast(id+26 as string))), 61, 64) as int64))/100000.000 > 0.2 
  AND abs(cast("0x"||substring(to_hex(sha256(cast(id+20 as string))), 61, 64) as int64))/100000.000 > 0.5) THEN 1 /*If development flag is 1 and rand var is greater than 0.5*/
  ELSE 0 END
  , RE_ADC_BOR_EQ_SIGNIF =
  CASE WHEN (prop <= 0.529412 AND prop >0.470588 
  AND abs(cast("0x"||substring(to_hex(sha256(cast(id+26 as string))), 61, 64) as int64))/100000.000 > 0.2 
  AND abs(cast("0x"||substring(to_hex(sha256(cast(id+28 as string))), 61, 64) as int64))/100000.000 > 0.5) THEN 1 /*If development flag is 1 and rand var is greater than 0.5*/
  ELSE 0 END
  , DEFAULT_IND =
  CASE WHEN prop <= 0.588235 AND prop >0.529412 THEN 1
  ELSE 0 END
  , PROVISIONS =
  CASE WHEN prop <= 0.588235 AND prop >0.529412 THEN 
  (SQRT(-2*LOG(nullif(abs(cast("0x"||substring(to_hex(sha256(cast(id as string))), 61, 64) as int64)),0)/100000.000))*SIN(2*ACOS(-1)*nullif(abs(cast("0x"||substring(to_hex(sha256(cast(id+1 as string))), 61, 64) as int64)),0)/100000.000)) * 6000000 + 2000000 *
  (abs(cast("0x"||substring(to_hex(sha256(cast(id+22 as string))), 61, 64) as int64))/100000.000 * 0.4) /*Drawn balance x random factor in range 0 to 0.4*/
  ELSE 
  (SQRT(-2*LOG(nullif(abs(cast("0x"||substring(to_hex(sha256(cast(id as string))), 61, 64) as int64)),0)/100000.000))*SIN(2*ACOS(-1)*nullif(abs(cast("0x"||substring(to_hex(sha256(cast(id+1 as string))), 61, 64) as int64)),0)/100000.000)) * 6000000 + 2000000 *
  (abs(cast("0x"||substring(to_hex(sha256(cast(id+29 as string))), 61, 64) as int64))/100000.000 * 0.01) END /*Drawn balance x random factor in range 0 to 0.01*/
  , HIGH_RISK_ITEM_IND =
  CASE WHEN prop <= 0.647059 AND prop >0.588235 THEN 1
  ELSE 0 END
  , COVERED_BOND_IND =
  CASE WHEN prop <= 0.705882 AND prop >0.647059 THEN 1
  ELSE 0 END
  , CIU_IND =
  CASE WHEN prop <= 0.882353 AND prop >0.823529 THEN 1
  ELSE 0 END
  , EQUITY_IND =
  CASE WHEN prop <= 0.90 AND prop >0.882353 THEN 1
  ELSE 0 END
  , EQUITY_CAP_DEDUCT_TREAT_IND =
  CASE WHEN prop <= 0.91 AND prop >0.90 THEN 1
  ELSE 0 END
  , EQ_ARTICLE_89_IND =
  CASE WHEN prop <= 0.92 AND prop >0.91 THEN 1
  ELSE 0 END
  , EQ_ARTICLE_48_IND =
  CASE WHEN prop <= 0.93 AND prop >0.92 THEN 1
  ELSE 0 END
  , VC_IND =
  CASE WHEN prop <= 0.938 AND prop >0.93 THEN 1
  ELSE 0 END
  , SUB_DEBT_IND =
  CASE WHEN prop <= 0.941176 AND prop >0.938 THEN 1
  ELSE 0 END
  , OTHER_ITEMS_TYPE =
  CASE WHEN prop > 0.941176 AND prop <= 0.97 THEN 'Tangible Assets'
  WHEN prop > 0.97 AND prop <= 1 THEN 'Cash in process of collection' END
  , TURNOVER_LATEST =
  (SQRT(-2*LOG(nullif(abs(cast("0x"||substring(to_hex(sha256(cast(id as string))), 61, 64) as int64)),0)/100000.000))*SIN(2*ACOS(-1)*nullif(abs(cast("0x"||substring(to_hex(sha256(cast(id+1 as string))), 61, 64) as int64)),0)/100000.000)) * 6000000 + 2000000 *
  (abs(cast("0x"||substring(to_hex(sha256(cast(id+22 as string))), 61, 64) as int64))/100000.000 + 3) /*Drawn balance x random factor in range 3 to 4*/
  , TURNOVER_1YR_PRIOR =
  (SQRT(-2*LOG(nullif(abs(cast("0x"||substring(to_hex(sha256(cast(id as string))), 61, 64) as int64)),0)/100000.000))*SIN(2*ACOS(-1)*nullif(abs(cast("0x"||substring(to_hex(sha256(cast(id+1 as string))), 61, 64) as int64)),0)/100000.000)) * 6000000 + 2000000 *
  (abs(cast("0x"||substring(to_hex(sha256(cast(id+22 as string))), 61, 64) as int64))/100000.000 + 2.9) /*Latest turnover with reduced multiple on drawn: range 2.9 to 3.9*/
  , TURNOVER_2YR_PRIOR =
  (SQRT(-2*LOG(nullif(abs(cast("0x"||substring(to_hex(sha256(cast(id as string))), 61, 64) as int64)),0)/100000.000))*SIN(2*ACOS(-1)*nullif(abs(cast("0x"||substring(to_hex(sha256(cast(id+1 as string))), 61, 64) as int64)),0)/100000.000)) * 6000000 + 2000000 *
  (abs(cast("0x"||substring(to_hex(sha256(cast(id+22 as string))), 61, 64) as int64))/100000.000 + 2.8) /*Latest turnover with reduced multiple on drawn: range 2.8 to 3.8*/
  , TOTAL_ASSESTS_OBLIGOR =
  (SQRT(-2*LOG(nullif(abs(cast("0x"||substring(to_hex(sha256(cast(id as string))), 61, 64) as int64)),0)/100000.000))*SIN(2*ACOS(-1)*nullif(abs(cast("0x"||substring(to_hex(sha256(cast(id+1 as string))), 61, 64) as int64)),0)/100000.000)) * 6000000 + 2000000 *
  (abs(cast("0x"||substring(to_hex(sha256(cast(id+22 as string))), 61, 64) as int64))/100000.000 + 5) /*Latest turnover with reduced multiple on drawn: range 2.8 to 3.8*/
  , SME_IND =
  CASE WHEN (abs(cast("0x"||substring(to_hex(sha256(cast(id+16 as string))), 61, 64) as int64))/100000.000 > 0.7 AND 
  (SQRT(-2*LOG(nullif(abs(cast("0x"||substring(to_hex(sha256(cast(id as string))), 61, 64) as int64)),0)/100000.000))*SIN(2*ACOS(-1)*nullif(abs(cast("0x"||substring(to_hex(sha256(cast(id+1 as string))), 61, 64) as int64)),0)/100000.000)) * 6000000 + 2000000 < 8800000) THEN 1 
  ELSE 0 END /*when OBLIGOR_CLASSIFICATION_IND_OR_FIRM is corp and drawn less than £880k, 1 else 0*/
  , RESIDUAL_VALUE_IND =
  CASE WHEN prop <= 0.97 AND prop >0.941176 THEN 1
  ELSE 0 END
  , RESIDUAL_VALUE = 
  (SQRT(-2*LOG(nullif(abs(cast("0x"||substring(to_hex(sha256(cast(id as string))), 61, 64) as int64)),0)/100000.000))*SIN(2*ACOS(-1)*nullif(abs(cast("0x"||substring(to_hex(sha256(cast(id+1 as string))), 61, 64) as int64)),0)/100000.000)) * 6000000 + 2000000 *
  (abs(cast("0x"||substring(to_hex(sha256(cast(id+22 as string))), 61, 64) as int64))/100000.000 * 0.1 + 0.7) /*Drawn balance x random factor in range 0.7 to 0.8*/
  , YEARS_TO_RESIDUAL_VALUE_REALISATION = 
  CASE WHEN prop <= 0.97 AND prop >0.941176 THEN (abs(cast("0x"||substring(to_hex(sha256(cast(id+7 as string))), 61, 64) as int64))/100000.000 * 3) /*random between 0 and 3*/
  ELSE 0 END
  /*END OF B3p1 STANDARDISED REQUIRED VARIABLES*/
  , BASEL_RESIDUAL_MATURITY = (abs(cast("0x"||substring(to_hex(sha256(cast(id+9 as string))), 61, 64) as int64))/100000.000 * 5) /*random between 0 and 5*/
  , IRB_APPROACH =
  CASE WHEN prop <= 0.470588 AND prop >0.411765 THEN 'Advanced'
  WHEN prop <= 0.365 AND prop >0.352941 THEN 'Slotting'
  WHEN prop <= 0.53 AND prop >0.529412 THEN 'Slotting'
  WHEN (prop <= 0.529412 AND prop >0.470588 AND abs(cast("0x"||substring(to_hex(sha256(cast(id+25 as string))), 61, 64) as int64))/100000.000 > 0.2) THEN 'Slotting' /*RE and CRE*/
  ELSE 'Foundation' END

  , IRB_EXPOSURE_SUB_CLASS =
  CASE WHEN prop <= 0.058824 AND prop > 0 THEN 'Central Government and Central Banks'
  WHEN prop <= 0.11767 AND prop >0.058824 THEN 'Institutions Quasi-sovereigns'
  WHEN prop <= 0.176471 AND prop >0.11767 THEN 'Institutions Quasi-sovereigns'
  WHEN prop <= 0.235294 AND prop >0.176471 THEN 'Institutions Quasi-sovereigns'
  WHEN prop <= 0.294118 AND prop >0.235294 THEN 'Institutions Quasi-sovereigns'
  WHEN prop <= 0.352941 AND prop >0.294118 THEN 'Institutions Other institutions'
    
  WHEN prop <= 0.365 AND prop >0.352941 THEN 'Corporate Specialised lending exposures'
  WHEN prop <= 0.38 AND prop >0.365 THEN 'Corporate Financial Corporates and large corporates'
  WHEN prop <= 0.411765 AND prop >0.38 THEN 'Corporates Other general corporates'

  WHEN prop <= 0.435 AND prop >0.411765 THEN 'Retail Exposures Qualifying revolving retail exposures'
  WHEN prop <= 0.45 AND prop >0.435 THEN 'Retail Exposures secured by residential immovable property'
  WHEN prop <= 0.470588 AND prop >0.45 THEN 'Retail Exposures Other retail'

  WHEN (prop <= 0.529412 AND prop >0.470588 AND abs(cast("0x"||substring(to_hex(sha256(cast(id+25 as string))), 61, 64) as int64))/100000.000 > 0.2) THEN 'Corporate Specialised lending exposures' /*RE and CRE*/
  WHEN (prop <= 0.529412 AND prop >0.470588 AND abs(cast("0x"||substring(to_hex(sha256(cast(id+25 as string))), 61, 64) as int64))/100000.000 > 0.2) THEN 'Retail Exposures secured by residential immovable property' /*RE and not CRE*/
    
  WHEN prop <= 0.53 AND prop >0.529412 THEN 'Corporate Specialised lending exposures'
  WHEN prop <= 0.545 AND prop >0.53 THEN 'Corporate Financial Corporates and large corporates'
  WHEN prop <= 0.56 AND prop >0.545 THEN 'Corporates Other general corporates'
  WHEN prop <= 0.57 AND prop >0.56 THEN 'Retail Exposures Qualifying revolving retail exposures'
  WHEN prop <= 0.58 AND prop >0.57 THEN 'Retail Exposures secured by residential immovable property'
  WHEN prop <= 0.588235 AND prop >0.58 THEN 'Retail Exposures Other retail'

  WHEN prop <= 0.647059 AND prop >0.588235 THEN 'Corporates Other general corporates'
  WHEN prop <= 0.705882 AND prop >0.647059 THEN 'Corporate Financial Corporates and large corporates'
  WHEN prop <= 0.764706 AND prop >0.705882 THEN 'Items representing securitisation positions'
  WHEN prop <= 0.823529 AND prop >0.764706 THEN 'Corporates Other general corporates'
  WHEN prop <= 0.882353 AND prop >0.823529 THEN 'Other non-credit obligation assets'
  WHEN prop <= 0.941176 AND prop >0.882353 THEN 'Equity exposures Other equity'
  WHEN prop > 0.941176 THEN 'Other non-credit obligation assets' END
  
  , ROLL_OUT_ASSET_CLASS =
  CASE WHEN prop <= 0.352941 AND prop > 0 THEN 'Institutions'
    
  WHEN prop <= 0.365 AND prop >0.352941 THEN 'Specialised lending'
  WHEN prop <= 0.411765 AND prop >0.365 THEN 'FC, LC & General Corporate'

  WHEN prop <= 0.435 AND prop >0.411765 THEN 'QRRE'
  WHEN prop <= 0.45 AND prop >0.435 THEN 'Retail Residential Property'
  WHEN prop <= 0.470588 AND prop >0.45 THEN 'Other retail'

  WHEN (prop <= 0.529412 AND prop >0.470588 AND abs(cast("0x"||substring(to_hex(sha256(cast(id+25 as string))), 61, 64) as int64))/100000.000 > 0.2) THEN 'Specialised lending'/*RE and CRE*/
  WHEN (prop <= 0.529412 AND prop >0.470588 AND abs(cast("0x"||substring(to_hex(sha256(cast(id+25 as string))), 61, 64) as int64))/100000.000 > 0.2) THEN 'Retail Residential Property' /*RE and not CRE*/
    
  WHEN prop <= 0.53 AND prop >0.529412 THEN 'Specialised lending'
  WHEN prop <= 0.56 AND prop >0.53 THEN 'FC, LC & General Corporate'
  WHEN prop <= 0.57 AND prop >0.56 THEN 'QRRE'
  WHEN prop <= 0.58 AND prop >0.57 THEN 'Retail Residential Property'
  WHEN prop <= 0.588235 AND prop >0.58 THEN 'Other retail'

  WHEN prop <= 0.705882 AND prop >0.588235 THEN 'FC, LC & General Corporate'
  WHEN prop <= 0.764706 AND prop >0.705882 THEN 'Purchased Receivables within Corporate'
  WHEN prop <= 0.823529 AND prop >0.764706 THEN 'FC, LC & General Corporate'
  WHEN prop <= 0.882353 AND prop >0.823529 THEN 'Other retail'
  WHEN prop <= 0.941176 AND prop >0.882353 THEN 'Purchased Receivables within Corporate'
  WHEN prop > 0.941176 THEN 'Other retail' END


  , LARGE_OR_UNREG_FINANCIAL_SECTOR_ENT_IND =
  CASE WHEN (abs(cast("0x"||substring(to_hex(sha256(cast(id+16 as string))), 61, 64) as int64))/100000.000 > 0.5) THEN 1
  ELSE 0 END
  , CORP_SME_IND = 
  CASE WHEN ((abs(cast("0x"||substring(to_hex(sha256(cast(id+16 as string))), 61, 64) as int64))/100000.000 > 0.7 AND 
  (SQRT(-2*LOG(nullif(abs(cast("0x"||substring(to_hex(sha256(cast(id as string))), 61, 64) as int64)),0)/100000.000))*SIN(2*ACOS(-1)*nullif(abs(cast("0x"||substring(to_hex(sha256(cast(id+1 as string))), 61, 64) as int64)),0)/100000.000)) * 6000000 + 2000000 < 8800000) AND
  (abs(cast("0x"||substring(to_hex(sha256(cast(id+22 as string))), 61, 64) as int64))/100000.000 + 3) < 44000000) THEN 1 
  ELSE 0 END /*when OBLIGOR_CLASSIFICATION_IND_OR_FIRM is corp, drawn is over £880k and turnover less than £44 mill, 1 else 0*/
  , HVCRE_IND =
  CASE WHEN (prop <= 0.529412 AND prop >0.5 AND abs(cast("0x"||substring(to_hex(sha256(cast(id+25 as string))), 61, 64) as int64))/100000.000 > 0.2) THEN 1
  ELSE 0 END /*subset of CRE flag (where prop >0.5 is only difference from CRE flag)*/
  , IRB_RATING_SCALE = 
  CASE WHEN (abs(cast("0x"||substring(to_hex(sha256(cast(id+16 as string))), 61, 64) as int64))/100000.000 > 0.7 AND 
  (SQRT(-2*LOG(nullif(abs(cast("0x"||substring(to_hex(sha256(cast(id as string))), 61, 64) as int64)),0)/100000.000))*SIN(2*ACOS(-1)*nullif(abs(cast("0x"||substring(to_hex(sha256(cast(id+1 as string))), 61, 64) as int64)),0)/100000.000)) * 6000000 + 2000000 < 8800000) THEN 'BDCS' 
  ELSE 'CMS' END /*when SME (OBLIGOR_CLASSIFICATION_IND_OR_FIRM is corp and drawn less than £880k), BDCS else CMS*/
  , IRB_RATING_GRADE =
  /*BDCS*/
  CASE WHEN (abs(cast("0x"||substring(to_hex(sha256(cast(id+16 as string))), 61, 64) as int64))/100000.000 > 0.7 AND
  (abs(cast("0x"||substring(to_hex(sha256(cast(id+16 as string))), 61, 64) as int64))/100000.000 <= 0.8 AND 
  (SQRT(-2*LOG(nullif(abs(cast("0x"||substring(to_hex(sha256(cast(id as string))), 61, 64) as int64)),0)/100000.000))*SIN(2*ACOS(-1)*nullif(abs(cast("0x"||substring(to_hex(sha256(cast(id+1 as string))), 61, 64) as int64)),0)/100000.000)) * 6000000 + 2000000 < 8800000)) THEN 'A'

  WHEN (abs(cast("0x"||substring(to_hex(sha256(cast(id+16 as string))), 61, 64) as int64))/100000.000 > 0.8 AND
  (abs(cast("0x"||substring(to_hex(sha256(cast(id+16 as string))), 61, 64) as int64))/100000.000 <= 0.9 AND 
  (SQRT(-2*LOG(nullif(abs(cast("0x"||substring(to_hex(sha256(cast(id as string))), 61, 64) as int64)),0)/100000.000))*SIN(2*ACOS(-1)*nullif(abs(cast("0x"||substring(to_hex(sha256(cast(id+1 as string))), 61, 64) as int64)),0)/100000.000)) * 6000000 + 2000000 < 8800000)) THEN 'B'

  WHEN (abs(cast("0x"||substring(to_hex(sha256(cast(id+16 as string))), 61, 64) as int64))/100000.000 > 0.9 AND
  (SQRT(-2*LOG(nullif(abs(cast("0x"||substring(to_hex(sha256(cast(id as string))), 61, 64) as int64)),0)/100000.000))*SIN(2*ACOS(-1)*nullif(abs(cast("0x"||substring(to_hex(sha256(cast(id+1 as string))), 61, 64) as int64)),0)/100000.000)) * 6000000 + 2000000 < 8800000) THEN 'C'

  WHEN (abs(cast("0x"||substring(to_hex(sha256(cast(id+16 as string))), 61, 64) as int64))/100000.000 > 0.7 AND 
  (SQRT(-2*LOG(nullif(abs(cast("0x"||substring(to_hex(sha256(cast(id as string))), 61, 64) as int64)),0)/100000.000))*SIN(2*ACOS(-1)*nullif(abs(cast("0x"||substring(to_hex(sha256(cast(id+1 as string))), 61, 64) as int64)),0)/100000.000)) * 6000000 + 2000000 < 8800000 AND
  prop <= 0.588235 AND prop >0.529412) THEN 'Y'
  
  /*CMS*/
  WHEN (abs(cast("0x"||substring(to_hex(sha256(cast(id+16 as string))), 61, 64) as int64))/100000.000 <= 0.7 AND 
  (SQRT(-2*LOG(nullif(abs(cast("0x"||substring(to_hex(sha256(cast(id as string))), 61, 64) as int64)),0)/100000.000))*SIN(2*ACOS(-1)*nullif(abs(cast("0x"||substring(to_hex(sha256(cast(id+1 as string))), 61, 64) as int64)),0)/100000.000)) * 6000000 + 2000000 < 8800000) THEN 
  cast(round(abs(cast("0x"||substring(to_hex(sha256(cast(id+33 as string))), 61, 64) as int64))/100000.000 * 18 + 1, 0) as string)

  WHEN (abs(cast("0x"||substring(to_hex(sha256(cast(id+16 as string))), 61, 64) as int64))/100000.000 <= 0.7 AND 
  (SQRT(-2*LOG(nullif(abs(cast("0x"||substring(to_hex(sha256(cast(id as string))), 61, 64) as int64)),0)/100000.000))*SIN(2*ACOS(-1)*nullif(abs(cast("0x"||substring(to_hex(sha256(cast(id+1 as string))), 61, 64) as int64)),0)/100000.000)) * 6000000 + 2000000 < 8800000 AND
  prop <= 0.588235 AND prop >0.529412) THEN '20' END

  , SLOTTING_RATING_GRADE = 2
  , R_LGD = 45
  , R_CCF = 75
  WHERE true;
   

SELECT
  *
  /*Add required intermediate variables*/
  ,t1.DRAWN_PRE_CRM - t1.CRM AS DRAWN_POST_CRM
  ,t2.STANDARDISED_CCF AS STANDARDISED_CCF
  ,t2.STANDARDISED_CCF * t1.OFF_BALANCE_SHEET_EXP_PRE_CCF AS OFF_BALANCE_SHEET_EXP_POST_CCF
  ,t3.CONSOLIDATION_LEVEL AS CONSOLIDATION_LEVEL
  ,t4.CQS AS CQS_FROM_ECAI_MAPPING
  ,t5.CQS AS CQS_FROM_ECAI_MAPPING_EXPOSURE_SPECIFIC_SHORT_TERM
  ,t6.CQS AS CQS_FROM_ECAI_MAPPING_WORST_SHORT_TERM_ECAI_RATING_FROM_ANY_OTHER_EXPOSURE
  ,t7.CQS AS CQS_OF_SOVEREIGN
  
  ,CASE WHEN t1.WORST_SHORT_TERM_ECAI_RATING_FROM_ANY_OTHER_EXPOSURE = 'No Exposure Specific Short Term ECAI Rating Available' THEN 0
  WHEN t1.WORST_SHORT_TERM_ECAI_RATING_FROM_ANY_OTHER_EXPOSURE IS NULL THEN 0
  WHEN t1.EXP_SPECIFIC_SHORT_TERM_ECAI_RATING = 'No Exposure Specific Short Term ECAI Rating Available' THEN 0
  WHEN t1.EXP_SPECIFIC_SHORT_TERM_ECAI_RATING IS NULL THEN 0
  ELSE 1 END AS EXP_SPECIFIC_SHORT_TERM_ECAI_RATING_PRESENT_IND
  
  ,CASE WHEN t1.ST_EXPOSURE_CLASS = 'Institutions' AND t1.MATURITY_AT_ORIGINATION < 3 THEN 1
  WHEN t1.ST_EXPOSURE_CLASS = 'Institutions' AND t1.MATURITY_AT_ORIGINATION >= 3 AND t1.MATURITY_AT_ORIGINATION < 6 AND
  t1.EXP_RELATED_TO_GOODS_CROSS_INT_BORDERS_FLAG = 1 THEN 2
  ELSE 0 END AS SHORT_TERM_TREAT_CAT_MATURITY_COMPONENT
  
  ,CASE WHEN (t1.OBLIGOR_CLASSIFICATION_IND_OR_FIRM = 'Individual' AND t1.GROUP_TOTAL_GROSS_EXP_INC_CON_COUNTERPARTIES < 880000) THEN 1
  WHEN (t1.OBLIGOR_CLASSIFICATION_IND_OR_FIRM = 'Corp' AND t1.GROUP_TOTAL_GROSS_EXP_INC_CON_COUNTERPARTIES < 880000 AND
  (t1.TURNOVER_LATEST + t1.TURNOVER_1YR_PRIOR + t1.TURNOVER_2YR_PRIOR)/3 < 44000000) THEN 1
  ELSE 0 END AS RETAIL_EXP_IND
  
  ,CASE WHEN t1.ST_EXPOSURE_CLASS = 'Retail Exposures' AND t1.RE_UNHEDGED_LENDING_WITH_CUR_MISMATCH = 1 THEN 1.5
  ELSE 1 END AS RE_CURR_MISMATCH_MULTIPLIER
  ,t1.DRAWN_PRE_CRM / COALESCE(t1.RE_VAL_AT_ORIG, t1.RE_INDEX_VALUATION) AS ST_REG_LTV

  /*End of intermediate variables*/
  
  /*Start of indicator & categoric variables to create basid Standardised Risk Weight lookup reference, prior to over-rides*/
  ,t8.ST_EXPOSURE_CLASS_KEY AS ST_EXPOSURE_CLASS_KEY
  ,CASE WHEN t8.ST_EXPOSURE_CLASS_KEY = 14 THEN 1
  WHEN t1.ECAI_RATING_GRADE_OBLIGOR <> 'No rating available' AND t8.ST_EXPOSURE_CLASS_USES_CQS_IND = 1 THEN 1
  ELSE 0 END AS ECAI_RATING_AVAILABLE_IND
  
  ,CASE WHEN t1.WORST_SHORT_TERM_ECAI_RATING_FROM_ANY_OTHER_EXPOSURE <> 'No Exposure Specific Short Term ECAI Rating Available' AND t1.ST_EXPOSURE_CLASS = 'Exposures to institutions and corporates with short term credit assessments' THEN 1
  WHEN t1.WORST_SHORT_TERM_ECAI_RATING_FROM_ANY_OTHER_EXPOSURE IS NOT NULL AND t1.ST_EXPOSURE_CLASS = 'Exposures to institutions and corporates with short term credit assessments' THEN 1
  WHEN t1.EXP_SPECIFIC_SHORT_TERM_ECAI_RATING <> 'No Exposure Specific Short Term ECAI Rating Available' THEN 1
  WHEN t1.EXP_SPECIFIC_SHORT_TERM_ECAI_RATING IS NOT NULL AND t1.ST_EXPOSURE_CLASS = 'Exposures to institutions and corporates with short term credit assessments' THEN 1
  WHEN t1.ST_EXPOSURE_CLASS = 'Institutions' AND t1.MATURITY_AT_ORIGINATION < 3 THEN 2
  WHEN t1.ST_EXPOSURE_CLASS = 'Institutions' AND t1.MATURITY_AT_ORIGINATION >= 3 AND t1.MATURITY_AT_ORIGINATION < 6 AND
  t1.EXP_RELATED_TO_GOODS_CROSS_INT_BORDERS_FLAG = 1 THEN 2
  ELSE 0 END AS SHORT_TERM_TREAT_CAT

  ,CASE WHEN t1.ST_EXPOSURE_CLASS = 'Retail Exposures' AND t1.TRANSACTOR_CLASSIFICATION = 1 THEN 1
  WHEN t1.ST_EXPOSURE_CLASS = 'Retail Exposures' AND t1.TRANSACTOR_CLASSIFICATION = 2 THEN 2
  WHEN t1.ST_EXPOSURE_CLASS = 'Retail Exposures' AND t1.EXP_TO_OWN_PENSION_FUND_OR_OWN_EMPLOYEE = 1 THEN 3
  ELSE 0 END AS RETAIL_CAT

  ,CASE WHEN t1.ST_EXPOSURE_CLASS = 'Real Estate' AND t1.SME_IND = 1 THEN 1
  ELSE 0 END AS RE_SME_IND

  ,CASE WHEN t1.ST_EXPOSURE_CLASS = 'Real Estate' AND t1.RE_SECURED_ON_HMO = 1 AND t1.RE_PRIMARY_RESIDENCE = 0 AND t1.RE_OBLIGOR_NUM_PROPERTIES_EXC_PRIMARY_RES <4 AND t1.SOCIAL_HOUSING_FLAG = 0 THEN 1
  WHEN t1.ST_EXPOSURE_CLASS = 'Real Estate' AND t1.RE_DEPENDS_ON_RENTS = 1 AND t1.RE_PRIMARY_RESIDENCE = 0 AND t1.RE_OBLIGOR_NUM_PROPERTIES_EXC_PRIMARY_RES <4 AND t1.SOCIAL_HOUSING_FLAG = 0 THEN 1
  ELSE 0 END AS RE_DEP_CASHFLOW_IND

  ,CASE WHEN t1.RE_CRE_IND = 0 AND t1.ST_EXPOSURE_CLASS = 'Real Estate' AND (t1.DRAWN_PRE_CRM + t1.OFF_BALANCE_SHEET_EXP_PRE_CCF) / COALESCE(t1.RE_VAL_AT_ORIG, t1.RE_INDEX_VALUATION) <= 0.5 THEN 1
  WHEN t1.RE_CRE_IND = 0 AND t1.ST_EXPOSURE_CLASS = 'Real Estate' AND 
  (t1.DRAWN_PRE_CRM + t1.OFF_BALANCE_SHEET_EXP_PRE_CCF) / COALESCE(t1.RE_VAL_AT_ORIG, t1.RE_INDEX_VALUATION) > 0.5 AND 
  (t1.DRAWN_PRE_CRM + t1.OFF_BALANCE_SHEET_EXP_PRE_CCF) / COALESCE(t1.RE_VAL_AT_ORIG, t1.RE_INDEX_VALUATION) <= 0.6 THEN 2
  WHEN t1.RE_CRE_IND = 0 AND t1.ST_EXPOSURE_CLASS = 'Real Estate' AND 
  (t1.DRAWN_PRE_CRM + t1.OFF_BALANCE_SHEET_EXP_PRE_CCF) / COALESCE(t1.RE_VAL_AT_ORIG, t1.RE_INDEX_VALUATION) > 0.6 AND 
  (t1.DRAWN_PRE_CRM + t1.OFF_BALANCE_SHEET_EXP_PRE_CCF) / COALESCE(t1.RE_VAL_AT_ORIG, t1.RE_INDEX_VALUATION) <= 0.8 THEN 3
    WHEN t1.RE_CRE_IND = 0 AND t1.ST_EXPOSURE_CLASS = 'Real Estate' AND 
  (t1.DRAWN_PRE_CRM + t1.OFF_BALANCE_SHEET_EXP_PRE_CCF) / COALESCE(t1.RE_VAL_AT_ORIG, t1.RE_INDEX_VALUATION) > 0.8 AND 
  (t1.DRAWN_PRE_CRM + t1.OFF_BALANCE_SHEET_EXP_PRE_CCF) / COALESCE(t1.RE_VAL_AT_ORIG, t1.RE_INDEX_VALUATION) <= 0.9 THEN 4
  WHEN t1.RE_CRE_IND = 0 AND t1.ST_EXPOSURE_CLASS = 'Real Estate' AND 
  (t1.DRAWN_PRE_CRM + t1.OFF_BALANCE_SHEET_EXP_PRE_CCF) / COALESCE(t1.RE_VAL_AT_ORIG, t1.RE_INDEX_VALUATION) > 0.9 AND 
  (t1.DRAWN_PRE_CRM + t1.OFF_BALANCE_SHEET_EXP_PRE_CCF) / COALESCE(t1.RE_VAL_AT_ORIG, t1.RE_INDEX_VALUATION) <= 1.0 THEN 5
    WHEN t1.RE_CRE_IND = 0 AND t1.ST_EXPOSURE_CLASS = 'Real Estate' AND 
  (t1.DRAWN_PRE_CRM + t1.OFF_BALANCE_SHEET_EXP_PRE_CCF) / COALESCE(t1.RE_VAL_AT_ORIG, t1.RE_INDEX_VALUATION) > 1.0 THEN 6
  ELSE 0 END AS RE_LTV_BAND

  ,CASE WHEN t1.ST_EXPOSURE_CLASS = 'Exposures in Default' AND t1.RETAIL_RESIDENTIAL_IND = 1 THEN 3
  WHEN t1.ST_EXPOSURE_CLASS = 'Exposures in Default' AND t1.RETAIL_RESIDENTIAL_IND = 0 AND ((t1.CRM + t1.PROVISIONS) / t1.DRAWN_PRE_CRM) < 0.2 THEN 1
  WHEN t1.ST_EXPOSURE_CLASS = 'Exposures in Default' AND t1.RETAIL_RESIDENTIAL_IND = 0 AND ((t1.CRM + t1.PROVISIONS) / t1.DRAWN_PRE_CRM) >= 0.2 THEN 2 
  ELSE 0 END AS DEF_CRM_LT_20_IND

  ,CASE WHEN t1.ST_EXPOSURE_CLASS = 'Subordinated Debt, Equity and Other Own Funds Investments' AND t1.EQUITY_CAP_DEDUCT_TREAT_IND = 1 THEN 1
  WHEN t1.ST_EXPOSURE_CLASS = 'Subordinated Debt, Equity and Other Own Funds Investments' AND t1.EQ_ARTICLE_89_IND = 1 THEN 2
  WHEN t1.ST_EXPOSURE_CLASS = 'Subordinated Debt, Equity and Other Own Funds Investments' AND t1.EQ_ARTICLE_48_IND = 1 THEN 3
  WHEN t1.ST_EXPOSURE_CLASS = 'Subordinated Debt, Equity and Other Own Funds Investments' AND t1.VC_IND = 1 THEN 4
  WHEN t1.ST_EXPOSURE_CLASS = 'Subordinated Debt, Equity and Other Own Funds Investments' AND t1.SUB_DEBT_IND = 1 THEN 5
  ELSE 0 END AS EQ_TREATMENT

  ,CASE WHEN t1.ST_EXPOSURE_CLASS = 'Other Items' AND t1.OTHER_ITEMS_TYPE = 'Tangible Assets' THEN 1
  WHEN t1.ST_EXPOSURE_CLASS = 'Other Items' AND t1.OTHER_ITEMS_TYPE = 'Cash in process of collection' THEN 2
  ELSE 0 END AS OTHER_ITEMS_TYPE_CODE

  ,CASE WHEN t1.ST_EXPOSURE_CLASS = 'Exposures to institutions and corporates with short term credit assessments' AND 
  t1.EXP_SPECIFIC_SHORT_TERM_ECAI_RATING <> 'No Exposure Specific Short Term ECAI Rating Available' THEN t5.CQS
  WHEN t1.ST_EXPOSURE_CLASS = 'Regional Government and Local Authorities' AND t1.ECAI_RATING_GRADE_OBLIGOR = 'No Rating Available' THEN t7.CQS
  WHEN t1.ST_EXPOSURE_CLASS = 'Public Sector Entities' AND t1.ECAI_RATING_GRADE_OBLIGOR = 'No Rating Available' THEN t7.CQS
  WHEN t1.ST_EXPOSURE_CLASS = 'Institutions' AND t1.ECAI_RATING_GRADE_OBLIGOR = 'No Rating Available' AND
  t1.UNRATED_INSTITUTION_CREDIT_ASSESSMENT = 'A' THEN 7
  WHEN t1.ST_EXPOSURE_CLASS = 'Institutions' AND t1.ECAI_RATING_GRADE_OBLIGOR = 'No Rating Available' AND
  t1.UNRATED_INSTITUTION_CREDIT_ASSESSMENT = 'B' THEN 8
  WHEN t1.ST_EXPOSURE_CLASS = 'Institutions' AND t1.ECAI_RATING_GRADE_OBLIGOR = 'No Rating Available' AND
  t1.UNRATED_INSTITUTION_CREDIT_ASSESSMENT = 'C' THEN 9
  WHEN t1.ST_EXPOSURE_CLASS <> 'Exposures to institutions and corporates with short term credit assessments' AND 
  t8.ST_EXPOSURE_CLASS_USES_CQS_IND = 1 AND t1.ECAI_RATING_GRADE_OBLIGOR = 'No Rating Available' THEN t4.CQS END AS CQS

FROM `dmn01_rsk_mvs_b3p1.T1` t1
JOIN `dmn01_rsk_mvs_b3p1.PRODUCT_TYPE_FOR_STANDARDISED_CCF` t2 ON t1.PRODUCT_TYPE_FOR_CCF_CLASSIFICATION = t2.PRODUCT_TYPE_FOR_STANDARDISED_CCF
JOIN `dmn01_rsk_mvs_b3p1.SUBSIDIARIES_AND_LEGAL_ENTITIES` t3 ON t1.LEGAL_ENTITY_DETAILED = t3.LEGAL_ENTITY_DETAILED

JOIN `dmn01_rsk_mvs_b3p1.ECAI_RATING_MAPPING_TO_CQS` t4 ON t1.ECAI_INSTITUTION_RATING_OBLIGOR = t4.ECAI_INSTITUTION AND
t1.ECAI_RATING_SCALE_OBLIGOR = t4.ECAI_RATING_SCALE AND t1.ECAI_RATING_GRADE_OBLIGOR = t4.ECAI_RATING_GRADE

JOIN `dmn01_rsk_mvs_b3p1.ECAI_RATING_MAPPING_TO_CQS` t5 ON t1.EXP_SPECIFIC_SHORT_TERM_ECAI_RATING = t5.ECAI_INSTITUTION AND
t1.ECAI_RATING_SCALE_SHORT_TERM_EXPOSURE_SPECIFIC = t5.ECAI_RATING_SCALE AND t1.EXP_SPECIFIC_SHORT_TERM_ECAI_RATING = t5.ECAI_RATING_GRADE

JOIN `dmn01_rsk_mvs_b3p1.ECAI_RATING_MAPPING_TO_CQS` t6 ON t1.ECAI_INSTITUTION_RATING_SHORT_TERM_FROM_ANY_OTHER_EXPOSURE = t6.ECAI_INSTITUTION AND
t1.ECAI_RATING_SCALE_SHORT_TERM_FROM_ANY_OTHER_EXPOSURE = t6.ECAI_RATING_SCALE AND t1.WORST_SHORT_TERM_ECAI_RATING_FROM_ANY_OTHER_EXPOSURE = t6.ECAI_RATING_GRADE

JOIN `dmn01_rsk_mvs_b3p1.ECAI_RATING_MAPPING_TO_CQS` t7 ON t1.ECAI_INSTITUTION_RATING_SOVEREIGN = t7.ECAI_INSTITUTION AND
t1.ECAI_RATING_SCALE_SOVEREIGN = t7.ECAI_RATING_SCALE AND t1.SOVEREIGN_ECAI_RATING = t7.ECAI_RATING_GRADE

JOIN `dmn01_rsk_mvs_b3p1.STANDARDISED_EXPOSURE_CLASSES` t8 ON t1.ST_EXPOSURE_CLASS = t8.ST_EXPOSURE_CLASS_NAME

  
  
  PRODUCT = 'GC (exc CRE)'
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

  ,COUNTRY_OF_DOMICLE =
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
  WHEN (RAND_UNIF_1 > 0.84159519) AND (RAND_UNIF_2 > 0.4252669) THEN 'UNITED STATES OF AMERICA' END

  --,RISK_GRADE = CMS_RATING
  --,BU_ID int
  --,BUS_UNIT varchar(50)
  --,SUB_BUSINESS_UNIT_NAME varchar(50)
  --,PRA_ASSET_CLASS varchar(50)
  --,SECTOR varchar(50)

  ,LEGAL_ENTITY_DETAILED =
  CASE WHEN RAND_UNIF_1 <= 0.09580038 THEN 'Bank of Scotland'
  WHEN (RAND_UNIF_1 > 0.09580038) AND (RAND_UNIF_1 <= 0.84159519) THEN 'Lloyds Bank'
  WHEN (RAND_UNIF_1 > 0.84159519) AND (RAND_UNIF_1 <= 1) THEN 'Lloyds Bank Capital Markets'
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