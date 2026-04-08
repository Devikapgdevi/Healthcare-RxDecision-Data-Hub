-- =============================================
-- 01_ACCOUNT_ADMINISTRATION.SQL
-- Healthcare RxDecision Analytics Platform
-- Data Source: Snowflake Marketplace (Definitive Healthcare)
-- =============================================

USE ROLE ACCOUNTADMIN;
USE DATABASE SECURITY_DB;
USE SCHEMA PUBLIC;

-- Network Policy
CREATE OR REPLACE NETWORK POLICY HC_NETWORK_POLICY
    ALLOWED_IP_LIST = ('0.0.0.0/0')
    COMMENT = 'Healthcare RxDecision Analytics - network access policy';

-- Password Policy
CREATE OR REPLACE PASSWORD POLICY HC_PASSWORD_POLICY
    PASSWORD_MIN_LENGTH = 12
    PASSWORD_MAX_LENGTH = 24
    PASSWORD_MIN_UPPER_CASE_CHARS = 1
    PASSWORD_MIN_LOWER_CASE_CHARS = 1
    PASSWORD_MIN_NUMERIC_CHARS = 1
    PASSWORD_MIN_SPECIAL_CHARS = 1
    PASSWORD_MAX_AGE_DAYS = 90
    PASSWORD_MAX_RETRIES = 3
    PASSWORD_LOCKOUT_TIME_MINS = 30
    COMMENT = 'Healthcare RxDecision Analytics - HIPAA compliant password policy';

-- Session Policy
CREATE OR REPLACE SESSION POLICY HC_SESSION_POLICY
    SESSION_IDLE_TIMEOUT_MINS = 30
    SESSION_UI_IDLE_TIMEOUT_MINS = 30
    COMMENT = 'Healthcare RxDecision Analytics - session timeout policy';

-- Verify
SHOW NETWORK POLICIES;
SHOW PASSWORD POLICIES;
SHOW SESSION POLICIES;
