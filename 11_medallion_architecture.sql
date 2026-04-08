-- =============================================
-- 11_MEDALLION_ARCHITECTURE.SQL
-- Healthcare RxDecision Analytics Platform
-- Data Source: Snowflake Marketplace (Definitive Healthcare)
-- Layers: Bronze -> Silver -> Gold -> Platinum
-- All tables under 5K records for minimal credit usage
-- =============================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;

-- =============================================
-- BRONZE LAYER (RAW_DB)
-- Raw data ingestion from Marketplace + synthetic
-- =============================================
USE DATABASE RAW_DB;
USE SCHEMA RAW_SCHEMA;

-- ICD-10 Reference Table (30 codes across 6 diagnosis categories)
CREATE OR REPLACE TABLE ICD10_REFERENCE (
    ICD_CODE VARCHAR(10),
    ICD_DESCRIPTION VARCHAR(200),
    DIAGNOSIS_CATEGORY VARCHAR(50)
) AS
SELECT * FROM VALUES
    ('I25.10', 'Atherosclerotic heart disease of native coronary artery', 'Cardiology'),
    ('I50.9',  'Heart failure, unspecified', 'Cardiology'),
    ('I10',    'Essential (primary) hypertension', 'Cardiology'),
    ('I48.91', 'Unspecified atrial fibrillation', 'Cardiology'),
    ('I21.9',  'Acute myocardial infarction, unspecified', 'Cardiology'),
    ('G43.909','Migraine, unspecified, not intractable', 'Neurology'),
    ('G40.909','Epilepsy, unspecified, not intractable', 'Neurology'),
    ('G20',    'Parkinson disease', 'Neurology'),
    ('G35',    'Multiple sclerosis', 'Neurology'),
    ('G30.9',  'Alzheimer disease, unspecified', 'Neurology'),
    ('M54.5',  'Low back pain', 'Orthopedics'),
    ('M17.11', 'Primary osteoarthritis, right knee', 'Orthopedics'),
    ('S72.001A','Fracture of unspecified part of neck of right femur', 'Orthopedics'),
    ('M79.3',  'Panniculitis, unspecified', 'Orthopedics'),
    ('M25.511','Pain in right shoulder', 'Orthopedics'),
    ('F32.1',  'Major depressive disorder, single episode, moderate', 'Psychiatry'),
    ('F41.1',  'Generalized anxiety disorder', 'Psychiatry'),
    ('F31.9',  'Bipolar disorder, unspecified', 'Psychiatry'),
    ('F20.9',  'Schizophrenia, unspecified', 'Psychiatry'),
    ('F43.10', 'Post-traumatic stress disorder, unspecified', 'Psychiatry'),
    ('Z00.00', 'Encounter for general adult medical examination without abnormal findings', 'Family Medicine'),
    ('J06.9',  'Acute upper respiratory infection, unspecified', 'Family Medicine'),
    ('E11.9',  'Type 2 diabetes mellitus without complications', 'Family Medicine'),
    ('J45.20', 'Mild intermittent asthma, uncomplicated', 'Family Medicine'),
    ('E78.5',  'Hyperlipidemia, unspecified', 'Family Medicine'),
    ('R51.9',  'Headache, unspecified', 'General'),
    ('R10.9',  'Unspecified abdominal pain', 'General'),
    ('R50.9',  'Fever, unspecified', 'General'),
    ('R05.9',  'Cough, unspecified', 'General'),
    ('R53.83', 'Other fatigue', 'General');

-- Patient Raw Table (2,000 records with ICD-10 codes)
CREATE OR REPLACE TABLE PATIENT_RAW AS
WITH FIRST_NAMES AS (
    SELECT COLUMN1 AS FIRST_NAME, ROW_NUMBER() OVER (ORDER BY COLUMN1) AS RN FROM VALUES
    ('James'),('Mary'),('Robert'),('Patricia'),('John'),('Jennifer'),('Michael'),('Linda'),('David'),('Elizabeth'),
    ('William'),('Barbara'),('Richard'),('Susan'),('Joseph'),('Jessica'),('Thomas'),('Sarah'),('Charles'),('Karen')
),
LAST_NAMES AS (
    SELECT COLUMN1 AS LAST_NAME, ROW_NUMBER() OVER (ORDER BY COLUMN1) AS RN FROM VALUES
    ('Smith'),('Johnson'),('Williams'),('Brown'),('Jones'),('Garcia'),('Miller'),('Davis'),('Rodriguez'),('Martinez'),
    ('Hernandez'),('Lopez'),('Gonzalez'),('Wilson'),('Anderson'),('Thomas'),('Taylor'),('Moore'),('Jackson'),('Martin')
),
GENDERS AS (SELECT COLUMN1 AS GENDER, ROW_NUMBER() OVER (ORDER BY COLUMN1) AS RN FROM VALUES ('M'),('F')),
GENERATED AS (SELECT SEQ4() + 1 AS ID FROM TABLE(GENERATOR(ROWCOUNT => 2000))),
BASE_PATIENTS AS (
    SELECT
        'P' || LPAD(g.ID::STRING, 5, '0') AS PATIENT_ID,
        fn.FIRST_NAME || ' ' || ln.LAST_NAME AS NAME,
        UNIFORM(18, 90, RANDOM()) AS AGE,
        gd.GENDER,
        DATEADD(DAY, -UNIFORM(1, 365, RANDOM()), CURRENT_DATE()) AS ADMISSION_DATE,
        CASE UNIFORM(1, 6, RANDOM()) WHEN 1 THEN 'Cardiology' WHEN 2 THEN 'Neurology'
            WHEN 3 THEN 'Orthopedics' WHEN 4 THEN 'Psychiatry' WHEN 5 THEN 'Family Medicine' ELSE 'General' END AS DIAGNOSIS
    FROM GENERATED g
    JOIN FIRST_NAMES fn ON fn.RN = MOD(g.ID - 1, 20) + 1
    JOIN LAST_NAMES ln ON ln.RN = MOD(g.ID - 1, 20) + 1
    JOIN GENDERS gd ON gd.RN = MOD(g.ID - 1, 2) + 1
)
SELECT p.PATIENT_ID, p.NAME, p.AGE, p.GENDER, p.ADMISSION_DATE, p.DIAGNOSIS,
    icd.ICD_CODE, icd.ICD_DESCRIPTION
FROM BASE_PATIENTS p
JOIN (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY DIAGNOSIS_CATEGORY ORDER BY ICD_CODE) AS RN,
        COUNT(*) OVER (PARTITION BY DIAGNOSIS_CATEGORY) AS CAT_COUNT
    FROM ICD10_REFERENCE
) icd ON p.DIAGNOSIS = icd.DIAGNOSIS_CATEGORY
    AND icd.RN = MOD(ABS(HASH(p.PATIENT_ID)), icd.CAT_COUNT) + 1;

-- Hospitals (synthetic HCO data)
CREATE OR REPLACE TABLE HOSPITALS AS
WITH HOSPITAL_NAMES AS (
    SELECT COLUMN1 AS HOSPITAL_NAME, COLUMN2 AS CITY, COLUMN3 AS STATE, ROW_NUMBER() OVER (ORDER BY COLUMN1) AS RN FROM VALUES
    ('General Memorial Hospital','New York','NY'),('St. Mary Medical Center','Los Angeles','CA'),
    ('University Health System','Chicago','IL'),('Regional Medical Center','Houston','TX'),
    ('Community Hospital','Phoenix','AZ'),('Metro Health Center','Philadelphia','PA'),
    ('Riverside Medical','San Antonio','TX'),('Lakeside Hospital','San Diego','CA'),
    ('Valley Health System','Dallas','TX'),('Summit Medical Center','San Jose','CA'),
    ('Harbor General Hospital','Austin','TX'),('Cedar Park Medical','Jacksonville','FL'),
    ('Northside Hospital','Fort Worth','TX'),('Pine Ridge Medical','Columbus','OH'),
    ('Bayview Hospital','Charlotte','NC'),('Heritage Health Center','Indianapolis','IN'),
    ('Suncoast Medical','San Francisco','CA'),('Mountain View Hospital','Seattle','WA'),
    ('Coastal Medical Center','Denver','CO'),('Prairie Health System','Nashville','TN')
)
SELECT
    1000000000 + (RN * 13) AS FACILITY_NPI,
    HOSPITAL_NAME, CITY, STATE,
    LPAD((10000 + RN * 431)::STRING, 5, '0') AS ZIP,
    '555-' || LPAD((1000 + RN * 37)::STRING, 4, '0') AS PHONE,
    RN AS DEFINITIVE_ID,
    UNIFORM(50, 800, RANDOM()) AS NUMBER_BEDS,
    ROUND(UNIFORM(10000000, 500000000, RANDOM()), 2) AS NET_PATIENT_REVENUE,
    ROUND(UNIFORM(15000000, 600000000, RANDOM()), 2) AS TOTAL_PATIENT_REVENUE,
    ROUND(UNIFORM(-5000000, 50000000, RANDOM()), 2) AS NET_INCOME,
    2024 AS FINANCIAL_YEAR
FROM HOSPITAL_NAMES;

-- HCO Locations (synthetic)
CREATE OR REPLACE TABLE HCO_LOCATIONS AS
SELECT
    h.DEFINITIVE_ID,
    h.HOSPITAL_NAME AS ORG_NAME,
    h.CITY, h.STATE, h.ZIP,
    h.PHONE,
    ROUND(UNIFORM(25.0, 48.0, RANDOM()), 6) AS LATITUDE,
    ROUND(UNIFORM(-122.0, -73.0, RANDOM()), 6) AS LONGITUDE,
    CASE MOD(ABS(RANDOM()), 3) WHEN 0 THEN 'Primary' WHEN 1 THEN 'Branch' ELSE 'Satellite' END AS LOCATION_TYPE
FROM RAW_DB.RAW_SCHEMA.HOSPITALS h;

-- Provider Affiliations (synthetic)
CREATE OR REPLACE TABLE PROVIDER_AFFILIATIONS AS
SELECT
    1000000000 + SEQ4() AS HCP_NPI,
    h.FACILITY_NPI AS HCO_NPI,
    h.HOSPITAL_NAME AS ORG_NAME,
    CASE MOD(ABS(RANDOM()), 6) WHEN 0 THEN 'Cardiology' WHEN 1 THEN 'Neurology' WHEN 2 THEN 'Orthopedics'
        WHEN 3 THEN 'Psychiatry' WHEN 4 THEN 'Family Medicine' ELSE 'Internal Medicine' END AS PRIMARY_SPECIALTY,
    CASE MOD(ABS(RANDOM()), 3) WHEN 0 THEN 'Active' WHEN 1 THEN 'Active' ELSE 'Active' END AS AFFILIATION_STATUS,
    UNIFORM(1, 100, RANDOM()) AS AFFILIATION_SCORE
FROM TABLE(GENERATOR(ROWCOUNT => 500)) g
JOIN RAW_DB.RAW_SCHEMA.HOSPITALS h ON MOD(SEQ4(), 20) + 1 = h.DEFINITIVE_ID;

-- Prescription Data (synthetic Rx)
CREATE OR REPLACE TABLE PRESCRIPTION_DATA AS
SELECT
    2020 + MOD(ABS(RANDOM()), 5) AS CLAIM_YEAR,
    CASE MOD(ABS(RANDOM()), 10) WHEN 0 THEN 'Aspirin' WHEN 1 THEN 'Metformin' WHEN 2 THEN 'Lisinopril'
        WHEN 3 THEN 'Atorvastatin' WHEN 4 THEN 'Metoprolol' WHEN 5 THEN 'Amlodipine'
        WHEN 6 THEN 'Omeprazole' WHEN 7 THEN 'Levothyroxine' WHEN 8 THEN 'Gabapentin' ELSE 'Lamotrigine' END AS MEDICATION_NAME,
    1000000000 + MOD(ABS(RANDOM()), 500) AS PRESCRIBER_NPI,
    CASE MOD(ABS(RANDOM()), 4) WHEN 0 THEN 'New' WHEN 1 THEN 'Switch' WHEN 2 THEN 'Restart' ELSE 'Continuation' END AS RX_EVENT_TYPE,
    UNIFORM(1, 500, RANDOM()) AS RX_EVENT_CLAIMS,
    UNIFORM(100, 5000, RANDOM()) AS RX_TOTAL_CLAIMS,
    UNIFORM(1, 10, RANDOM()) AS RX_EVENT_SCORE_DECILE,
    UNIFORM(0, 200, RANDOM()) AS NEW_RX_COUNT,
    UNIFORM(50, 1000, RANDOM()) AS TOTAL_RX_COUNT,
    DATEADD(DAY, -UNIFORM(1, 365, RANDOM()), CURRENT_DATE()) AS DATE_UPDATED
FROM TABLE(GENERATOR(ROWCOUNT => 3000));

-- ICU Events (4,000 records)
CREATE OR REPLACE TABLE ICU_EVENTS AS
SELECT 'E' || LPAD(SEQ4()::STRING, 6, '0') AS EVENT_ID,
    'P' || LPAD((MOD(ABS(RANDOM()), 2000) + 1)::STRING, 5, '0') AS PATIENT_ID,
    CASE MOD(ABS(RANDOM()), 5) WHEN 0 THEN 'Vitals Check' WHEN 1 THEN 'Medication' WHEN 2 THEN 'Procedure' WHEN 3 THEN 'Alert' ELSE 'Transfer' END AS EVENT_TYPE,
    DATEADD(HOUR, -MOD(ABS(RANDOM()), 720), CURRENT_TIMESTAMP()) AS EVENT_TIMESTAMP,
    50 + MOD(ABS(RANDOM()), 101) AS HEART_RATE,
    80 + MOD(ABS(RANDOM()), 21) AS OXYGEN_LEVEL
FROM TABLE(GENERATOR(ROWCOUNT => 4000));

-- Billing Data (3,000 records)
CREATE OR REPLACE TABLE BILLING_DATA AS
SELECT 'B' || LPAD(SEQ4()::STRING, 6, '0') AS BILL_ID,
    'P' || LPAD((MOD(ABS(RANDOM()), 2000) + 1)::STRING, 5, '0') AS PATIENT_ID,
    ROUND(1000 + UNIFORM(0, 99000, RANDOM()), 2) AS AMOUNT,
    DATEADD(DAY, -MOD(ABS(RANDOM()), 365), CURRENT_DATE()) AS BILL_DATE,
    CASE MOD(ABS(RANDOM()), 3) WHEN 0 THEN 'PAID' WHEN 1 THEN 'PENDING' ELSE 'OVERDUE' END AS STATUS
FROM TABLE(GENERATOR(ROWCOUNT => 3000));

-- Device Alerts (1,500 records)
CREATE OR REPLACE TABLE DEVICE_ALERTS AS
SELECT 'A' || LPAD(SEQ4()::STRING, 6, '0') AS ALERT_ID,
    'DEV' || LPAD(MOD(ABS(RANDOM()), 100)::STRING, 3, '0') AS DEVICE_ID,
    'P' || LPAD((MOD(ABS(RANDOM()), 2000) + 1)::STRING, 5, '0') AS PATIENT_ID,
    CASE MOD(ABS(RANDOM()), 5) WHEN 0 THEN 'Low Battery' WHEN 1 THEN 'Connection Lost' WHEN 2 THEN 'Abnormal Reading' WHEN 3 THEN 'Calibration Needed' ELSE 'Maintenance Due' END AS ALERT_TYPE,
    CASE MOD(ABS(RANDOM()), 3) WHEN 0 THEN 'LOW' WHEN 1 THEN 'MEDIUM' ELSE 'HIGH' END AS SEVERITY,
    DATEADD(HOUR, -MOD(ABS(RANDOM()), 168), CURRENT_TIMESTAMP()) AS ALERT_TIMESTAMP,
    CASE WHEN MOD(ABS(RANDOM()), 2) = 0 THEN TRUE ELSE FALSE END AS ACKNOWLEDGED
FROM TABLE(GENERATOR(ROWCOUNT => 1500));

-- Medication Records (3,000 records)
CREATE OR REPLACE TABLE MEDICATION_RECORDS AS
SELECT 'M' || LPAD(SEQ4()::STRING, 6, '0') AS RECORD_ID,
    'P' || LPAD((MOD(ABS(RANDOM()), 2000) + 1)::STRING, 5, '0') AS PATIENT_ID,
    CASE MOD(ABS(RANDOM()), 10) WHEN 0 THEN 'Aspirin' WHEN 1 THEN 'Metformin' WHEN 2 THEN 'Lisinopril' WHEN 3 THEN 'Atorvastatin' WHEN 4 THEN 'Metoprolol' WHEN 5 THEN 'Amlodipine' WHEN 6 THEN 'Omeprazole' WHEN 7 THEN 'Levothyroxine' WHEN 8 THEN 'Gabapentin' ELSE 'Lamotrigine' END AS MEDICATION_NAME,
    CASE MOD(ABS(RANDOM()), 4) WHEN 0 THEN '10mg' WHEN 1 THEN '25mg' WHEN 2 THEN '50mg' ELSE '100mg' END AS DOSAGE,
    CASE MOD(ABS(RANDOM()), 4) WHEN 0 THEN 'Once daily' WHEN 1 THEN 'Twice daily' WHEN 2 THEN 'Three times daily' ELSE 'As needed' END AS FREQUENCY,
    DATEADD(DAY, -MOD(ABS(RANDOM()), 180), CURRENT_DATE()) AS PRESCRIBED_DATE,
    'Dr. ' || CASE MOD(ABS(RANDOM()), 5) WHEN 0 THEN 'Smith' WHEN 1 THEN 'Johnson' WHEN 2 THEN 'Williams' WHEN 3 THEN 'Brown' ELSE 'Davis' END AS PRESCRIBING_DOCTOR
FROM TABLE(GENERATOR(ROWCOUNT => 3000));

-- =============================================
-- SILVER LAYER (TRANSFORM_DB)
-- Cleansed, validated, enriched data
-- =============================================
USE DATABASE TRANSFORM_DB;
USE SCHEMA TRANSFORM_SCHEMA;

CREATE OR REPLACE TABLE CLEAN_PATIENT AS
SELECT PATIENT_ID, NAME, AGE, GENDER, ADMISSION_DATE, DIAGNOSIS,
    ICD_CODE, ICD_DESCRIPTION,
    LEFT(PATIENT_ID, 2) || '***' AS PATIENT_ID_MASKED, CURRENT_TIMESTAMP() AS ETL_TIMESTAMP
FROM RAW_DB.RAW_SCHEMA.PATIENT_RAW;

CREATE OR REPLACE TABLE CLEAN_ICU_EVENTS AS
SELECT EVENT_ID, PATIENT_ID, EVENT_TYPE, EVENT_TIMESTAMP, HEART_RATE, OXYGEN_LEVEL,
    CASE WHEN HEART_RATE < 60 OR HEART_RATE > 100 OR OXYGEN_LEVEL < 92 THEN TRUE ELSE FALSE END AS IS_CRITICAL,
    CURRENT_TIMESTAMP() AS ETL_TIMESTAMP
FROM RAW_DB.RAW_SCHEMA.ICU_EVENTS;

-- =============================================
-- GOLD LAYER (ANALYTICS_DB)
-- Aggregated, business-ready analytics
-- =============================================
USE DATABASE ANALYTICS_DB;
USE SCHEMA ANALYTICS_SCHEMA;

CREATE OR REPLACE TABLE PATIENT_ANALYTICS AS
SELECT p.PATIENT_ID, p.NAME, p.AGE, p.GENDER, p.ADMISSION_DATE, p.DIAGNOSIS,
    p.ICD_CODE, p.ICD_DESCRIPTION,
    COUNT(e.EVENT_ID) AS ICU_EVENT_COUNT, AVG(e.HEART_RATE) AS AVG_HEART_RATE,
    AVG(e.OXYGEN_LEVEL) AS AVG_OXYGEN_LEVEL,
    SUM(CASE WHEN e.IS_CRITICAL THEN 1 ELSE 0 END) AS CRITICAL_EVENT_COUNT
FROM TRANSFORM_DB.TRANSFORM_SCHEMA.CLEAN_PATIENT p
LEFT JOIN TRANSFORM_DB.TRANSFORM_SCHEMA.CLEAN_ICU_EVENTS e ON p.PATIENT_ID = e.PATIENT_ID
GROUP BY p.PATIENT_ID, p.NAME, p.AGE, p.GENDER, p.ADMISSION_DATE, p.DIAGNOSIS, p.ICD_CODE, p.ICD_DESCRIPTION;

CREATE OR REPLACE TABLE BILLING_ANALYTICS AS
SELECT PATIENT_ID, COUNT(*) AS TOTAL_BILLS, SUM(AMOUNT) AS TOTAL_AMOUNT, AVG(AMOUNT) AS AVG_BILL_AMOUNT,
    SUM(CASE WHEN STATUS = 'PAID' THEN AMOUNT ELSE 0 END) AS PAID_AMOUNT,
    SUM(CASE WHEN STATUS = 'PENDING' THEN AMOUNT ELSE 0 END) AS PENDING_AMOUNT,
    SUM(CASE WHEN STATUS = 'OVERDUE' THEN AMOUNT ELSE 0 END) AS OVERDUE_AMOUNT
FROM RAW_DB.RAW_SCHEMA.BILLING_DATA GROUP BY PATIENT_ID;

-- =============================================
-- PLATINUM LAYER (AI_READY_DB)
-- ML feature store, NLP-ready notes, embeddings
-- =============================================
USE DATABASE AI_READY_DB;
USE SCHEMA FEATURE_STORE;

CREATE OR REPLACE TABLE ICU_FEATURE_STORE AS
SELECT e.PATIENT_ID, p.AGE, p.GENDER, p.DIAGNOSIS, p.ICD_CODE, p.ICD_DESCRIPTION,
    COUNT(*) AS TOTAL_EVENTS,
    SUM(CASE WHEN e.IS_CRITICAL THEN 1 ELSE 0 END) AS CRITICAL_EVENTS,
    AVG(e.HEART_RATE) AS AVG_HEART_RATE, MIN(e.HEART_RATE) AS MIN_HEART_RATE,
    MAX(e.HEART_RATE) AS MAX_HEART_RATE, STDDEV(e.HEART_RATE) AS STD_HEART_RATE,
    AVG(e.OXYGEN_LEVEL) AS AVG_OXYGEN, MIN(e.OXYGEN_LEVEL) AS MIN_OXYGEN,
    MAX(e.OXYGEN_LEVEL) AS MAX_OXYGEN, STDDEV(e.OXYGEN_LEVEL) AS STD_OXYGEN,
    DATEDIFF(DAY, MIN(e.EVENT_TIMESTAMP), MAX(e.EVENT_TIMESTAMP)) AS ICU_STAY_DAYS,
    ROUND(SUM(CASE WHEN e.IS_CRITICAL THEN 1 ELSE 0 END)::FLOAT / NULLIF(COUNT(*), 0) * 100, 2) AS CRITICAL_EVENT_RATE,
    CASE WHEN AVG(e.OXYGEN_LEVEL) < 90 OR AVG(e.HEART_RATE) > 110 THEN 'HIGH'
         WHEN AVG(e.OXYGEN_LEVEL) < 94 OR AVG(e.HEART_RATE) > 100 THEN 'MEDIUM'
         ELSE 'LOW' END AS RISK_SCORE,
    CURRENT_TIMESTAMP() AS FEATURE_TIMESTAMP
FROM TRANSFORM_DB.TRANSFORM_SCHEMA.CLEAN_ICU_EVENTS e
JOIN TRANSFORM_DB.TRANSFORM_SCHEMA.CLEAN_PATIENT p ON e.PATIENT_ID = p.PATIENT_ID
GROUP BY e.PATIENT_ID, p.AGE, p.GENDER, p.DIAGNOSIS, p.ICD_CODE, p.ICD_DESCRIPTION;

CREATE OR REPLACE TABLE PATIENT_NOTES AS
SELECT 'N' || LPAD(SEQ4()::STRING, 6, '0') AS NOTE_ID,
    'P' || LPAD((MOD(ABS(RANDOM()), 2000) + 1)::STRING, 5, '0') AS PATIENT_ID,
    CASE MOD(ABS(RANDOM()), 5)
        WHEN 0 THEN 'Patient presents with chest pain and shortness of breath. ECG shows normal sinus rhythm.'
        WHEN 1 THEN 'Follow-up visit for diabetes management. Blood sugar levels improving with current medication.'
        WHEN 2 THEN 'Post-operative day 2. Wound healing well. No signs of infection.'
        WHEN 3 THEN 'Patient reports dizziness and fatigue. Ordered blood work to check for anemia.'
        ELSE 'Routine checkup. All vitals within normal range. Continue current treatment plan.' END AS NOTE_TEXT,
    DATEADD(DAY, -MOD(ABS(RANDOM()), 90), CURRENT_DATE()) AS NOTE_DATE,
    CASE MOD(ABS(RANDOM()), 4) WHEN 0 THEN 'Progress Note' WHEN 1 THEN 'Discharge Summary' WHEN 2 THEN 'Consultation' ELSE 'Lab Results' END AS NOTE_TYPE
FROM TABLE(GENERATOR(ROWCOUNT => 2000));

CREATE OR REPLACE TABLE PATIENT_EMBEDDINGS AS
SELECT PATIENT_ID,
    ARRAY_CONSTRUCT(UNIFORM(0::FLOAT,1::FLOAT,RANDOM()),UNIFORM(0::FLOAT,1::FLOAT,RANDOM()),
        UNIFORM(0::FLOAT,1::FLOAT,RANDOM()),UNIFORM(0::FLOAT,1::FLOAT,RANDOM()),
        UNIFORM(0::FLOAT,1::FLOAT,RANDOM()),UNIFORM(0::FLOAT,1::FLOAT,RANDOM()),
        UNIFORM(0::FLOAT,1::FLOAT,RANDOM()),UNIFORM(0::FLOAT,1::FLOAT,RANDOM())) AS EMBEDDING_VECTOR,
    CURRENT_TIMESTAMP() AS CREATED_AT
FROM TRANSFORM_DB.TRANSFORM_SCHEMA.CLEAN_PATIENT;

USE SCHEMA SEMANTIC_MODELS;

CREATE OR REPLACE VIEW V_PATIENT_SEMANTIC AS
SELECT p.PATIENT_ID AS "Patient ID", p.AGE AS "Age", p.GENDER AS "Gender", p.DIAGNOSIS AS "Diagnosis",
    p.ICD_CODE AS "ICD Code", p.ICD_DESCRIPTION AS "ICD Description",
    a.ICU_EVENT_COUNT AS "Total ICU Events",
    a.CRITICAL_EVENT_COUNT AS "Critical Events",
    ROUND(a.AVG_HEART_RATE, 1) AS "Average Heart Rate",
    ROUND(a.AVG_OXYGEN_LEVEL, 1) AS "Average Oxygen Level",
    (CASE WHEN p.AGE >= 65 THEN 1 ELSE 0 END)
        + (CASE WHEN a.AVG_OXYGEN_LEVEL < 92 THEN 1 ELSE 0 END)
        + (CASE WHEN a.AVG_HEART_RATE > 100 THEN 1 ELSE 0 END)
        + (CASE WHEN a.CRITICAL_EVENT_COUNT > 5 THEN 1 ELSE 0 END) AS "Risk Score",
    COALESCE(b.TOTAL_AMOUNT, 0) AS "Total Billing Amount",
    CASE WHEN p.AGE >= 65 THEN 1 ELSE 0 END AS "Age Risk Flag",
    CASE WHEN a.AVG_OXYGEN_LEVEL < 92 THEN 1 ELSE 0 END AS "Low Oxygen Risk Flag",
    CASE WHEN a.AVG_HEART_RATE > 100 THEN 1 ELSE 0 END AS "High Heart Rate Risk Flag"
FROM ANALYTICS_DB.ANALYTICS_SCHEMA.PATIENT_ANALYTICS a
JOIN TRANSFORM_DB.TRANSFORM_SCHEMA.CLEAN_PATIENT p ON a.PATIENT_ID = p.PATIENT_ID
LEFT JOIN ANALYTICS_DB.ANALYTICS_SCHEMA.BILLING_ANALYTICS b ON a.PATIENT_ID = b.PATIENT_ID;
