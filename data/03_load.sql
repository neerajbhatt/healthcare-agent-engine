-- ============================================================
-- Load CSV data into Snowflake
-- Run AFTER 01_schema.sql and uploading CSVs to a stage
-- ============================================================

USE DATABASE HEALTHCARE_DB;
USE SCHEMA ANALYTICS;

-- ── Option A: Load from internal stage ──
-- First upload CSVs:
--   PUT file:///path/to/csv/providers.csv @~;
--   PUT file:///path/to/csv/members.csv @~;
--   PUT file:///path/to/csv/eligibility.csv @~;
--   PUT file:///path/to/csv/claims.csv @~;
--   PUT file:///path/to/csv/specialty_procedure_map.csv @~;

-- Create a file format for the CSVs
CREATE OR REPLACE FILE FORMAT healthcare_csv
    TYPE = 'CSV'
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    SKIP_HEADER = 1
    NULL_IF = ('', 'NULL')
    EMPTY_FIELD_AS_NULL = TRUE;

-- Load providers
COPY INTO providers (
    npi, first_name, last_name, specialty, practice_state,
    practice_city, practice_zip, tax_id, practice_type,
    enrollment_date, is_active
)
FROM @~/providers.csv.gz
FILE_FORMAT = healthcare_csv
ON_ERROR = 'CONTINUE';

-- Load members
COPY INTO members (
    member_id, date_of_birth, gender, state, city,
    zip, plan_type, employer_group, risk_score
)
FROM @~/members.csv.gz
FILE_FORMAT = healthcare_csv
ON_ERROR = 'CONTINUE';

-- Load eligibility
COPY INTO eligibility (
    eligibility_id, member_id, coverage_start, coverage_end,
    plan_code, coverage_type, status, created_date
)
FROM @~/eligibility.csv.gz
FILE_FORMAT = healthcare_csv
ON_ERROR = 'CONTINUE';

-- Load claims
COPY INTO claims (
    claim_id, member_id, provider_npi, referring_npi,
    service_date, paid_date, procedure_code, diagnosis_code,
    diagnosis_desc, place_of_service, allowed_amount, paid_amount,
    member_liability, claim_type, service_state,
    modifier_1, modifier_2, units, claim_status
)
FROM @~/claims.csv.gz
FILE_FORMAT = healthcare_csv
ON_ERROR = 'CONTINUE';

-- Load specialty map
COPY INTO specialty_procedure_map (
    procedure_code, expected_specialty, category
)
FROM @~/specialty_procedure_map.csv.gz
FILE_FORMAT = healthcare_csv
ON_ERROR = 'CONTINUE';


-- ── Option B: Load from S3 (if you prefer external stage) ──
-- CREATE OR REPLACE STAGE healthcare_stage
--     URL = 's3://your-bucket/healthcare-data/'
--     CREDENTIALS = (AWS_KEY_ID='...' AWS_SECRET_KEY='...');
--
-- COPY INTO providers FROM @healthcare_stage/providers.csv ...


-- ── Verify row counts ──
SELECT 'providers' AS tbl, COUNT(*) AS rows FROM providers
UNION ALL
SELECT 'members', COUNT(*) FROM members
UNION ALL
SELECT 'eligibility', COUNT(*) FROM eligibility
UNION ALL
SELECT 'claims', COUNT(*) FROM claims
UNION ALL
SELECT 'specialty_procedure_map', COUNT(*) FROM specialty_procedure_map;
