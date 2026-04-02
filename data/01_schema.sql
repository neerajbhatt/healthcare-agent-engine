-- ============================================================
-- Healthcare Analytics Schema - Snowflake DDL
-- Run this first to create all tables before loading data
-- ============================================================

CREATE DATABASE IF NOT EXISTS HEALTHCARE_DB;
USE DATABASE HEALTHCARE_DB;

CREATE SCHEMA IF NOT EXISTS ANALYTICS;
USE SCHEMA ANALYTICS;

-- ── PROVIDERS ──
CREATE OR REPLACE TABLE providers (
    npi                 VARCHAR(10)     PRIMARY KEY,
    first_name          VARCHAR(100),
    last_name           VARCHAR(100),
    specialty           VARCHAR(100),
    practice_state      VARCHAR(2),
    practice_city       VARCHAR(100),
    practice_zip        VARCHAR(10),
    tax_id              VARCHAR(20),
    practice_type       VARCHAR(50),    -- solo | group | facility
    enrollment_date     DATE,
    is_active           BOOLEAN DEFAULT TRUE,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- ── MEMBERS ──
CREATE OR REPLACE TABLE members (
    member_id           VARCHAR(20)     PRIMARY KEY,
    date_of_birth       DATE,
    gender              VARCHAR(1),
    state               VARCHAR(2),
    city                VARCHAR(100),
    zip                 VARCHAR(10),
    plan_type           VARCHAR(50),    -- HMO | PPO | EPO | Medicaid
    employer_group      VARCHAR(100),
    risk_score          DECIMAL(5,2),
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- ── ELIGIBILITY ──
CREATE OR REPLACE TABLE eligibility (
    eligibility_id      VARCHAR(20)     PRIMARY KEY,
    member_id           VARCHAR(20)     REFERENCES members(member_id),
    coverage_start      DATE,
    coverage_end        DATE,
    plan_code           VARCHAR(20),
    coverage_type       VARCHAR(50),    -- medical | dental | vision | pharmacy
    status              VARCHAR(20),    -- active | terminated | suspended
    created_date        DATE,           -- when the record was created (for retroactive detection)
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- ── CLAIMS ──
CREATE OR REPLACE TABLE claims (
    claim_id            VARCHAR(20)     PRIMARY KEY,
    member_id           VARCHAR(20)     REFERENCES members(member_id),
    provider_npi        VARCHAR(10)     REFERENCES providers(npi),
    referring_npi       VARCHAR(10),
    service_date        DATE,
    paid_date           DATE,
    procedure_code      VARCHAR(10),
    diagnosis_code      VARCHAR(10),
    diagnosis_desc      VARCHAR(200),
    place_of_service    VARCHAR(5),
    allowed_amount      DECIMAL(12,2),
    paid_amount         DECIMAL(12,2),
    member_liability    DECIMAL(12,2),
    claim_type          VARCHAR(20),    -- professional | institutional | pharmacy
    service_state       VARCHAR(2),
    modifier_1          VARCHAR(5),
    modifier_2          VARCHAR(5),
    units               INT DEFAULT 1,
    claim_status        VARCHAR(20),    -- paid | denied | pending | adjusted
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- ── SPECIALTY TO PROCEDURE MAPPING ──
CREATE OR REPLACE TABLE specialty_procedure_map (
    procedure_code      VARCHAR(10),
    expected_specialty  VARCHAR(100),
    category            VARCHAR(100),
    PRIMARY KEY (procedure_code, expected_specialty)
);

-- ── INDEXES FOR PERFORMANCE ──
-- Snowflake uses micro-partitions, but clustering keys help:
ALTER TABLE claims CLUSTER BY (provider_npi, service_date);
ALTER TABLE eligibility CLUSTER BY (member_id, coverage_start);
