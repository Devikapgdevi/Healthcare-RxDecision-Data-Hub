-- =============================================
-- 07_ALERTS.SQL
-- Healthcare RxDecision Analytics Platform
-- Automated Alerts
-- =============================================

USE ROLE ACCOUNTADMIN;
USE DATABASE MONITORING_DB;
USE SCHEMA MONITORING_SCHEMA;
USE WAREHOUSE COMPUTE_WH;

CREATE OR REPLACE ALERT HC_LONG_QUERY_ALERT
    WAREHOUSE = COMPUTE_WH SCHEDULE = '5 MINUTE'
    IF (EXISTS (
        SELECT 1 FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE TOTAL_ELAPSED_TIME > 600000
        AND START_TIME >= DATEADD(MINUTE, -5, CURRENT_TIMESTAMP())
    ))
    THEN
        CALL SYSTEM$SEND_EMAIL('healthcare_alerts','admin@healthcare.com',
            'RxDecision Platform - Long Running Query Detected',
            'A query has exceeded 10 minutes execution time');

CREATE OR REPLACE ALERT HC_FAILED_LOGIN_ALERT
    WAREHOUSE = COMPUTE_WH SCHEDULE = '15 MINUTE'
    IF (EXISTS (
        SELECT 1 FROM SNOWFLAKE.ACCOUNT_USAGE.LOGIN_HISTORY
        WHERE IS_SUCCESS = 'NO'
        AND EVENT_TIMESTAMP >= DATEADD(MINUTE, -15, CURRENT_TIMESTAMP())
        HAVING COUNT(*) > 3
    ))
    THEN
        CALL SYSTEM$SEND_EMAIL('healthcare_alerts','security@healthcare.com',
            'RxDecision Platform - Multiple Failed Login Attempts',
            'Multiple failed login attempts detected');

CREATE OR REPLACE ALERT HC_CREDIT_ALERT
    WAREHOUSE = COMPUTE_WH SCHEDULE = '60 MINUTE'
    IF (EXISTS (
        SELECT 1 FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
        WHERE START_TIME >= DATEADD(HOUR, -1, CURRENT_TIMESTAMP())
        GROUP BY WAREHOUSE_NAME HAVING SUM(CREDITS_USED) > 10
    ))
    THEN
        CALL SYSTEM$SEND_EMAIL('healthcare_alerts','admin@healthcare.com',
            'RxDecision Platform - High Credit Usage Alert',
            'Warehouse credit usage exceeded threshold');

SHOW ALERTS LIKE 'HC_%';
