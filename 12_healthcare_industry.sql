-- =============================================
-- 12_HEALTHCARE_INDUSTRY.SQL
-- Healthcare RxDecision Analytics Platform
-- Healthcare-specific Bronze tables (standalone re-run)
-- Run this to rebuild healthcare industry tables independently
-- Depends on: RAW_DB.RAW_SCHEMA exists
-- Under 5K records per table
-- =============================================

USE ROLE ACCOUNTADMIN;
USE DATABASE RAW_DB;
USE SCHEMA RAW_SCHEMA;
USE WAREHOUSE COMPUTE_WH;

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
