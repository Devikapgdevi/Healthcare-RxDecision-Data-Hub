/*=============================================================================
  GITHUB_INTEGRATION.SQL
  Healthcare RxDecision Analytics Platform
  Snowflake <-> GitHub Connection (PAT Authentication)

  Account:      UUB65990
  Account URL:  UUB65990.snowflakecomputing.com
  Username:     DEVIKAPG
  GitHub User:  Devikapgdevi
  Repo:         Healthcare-RxDecision-Data-Hub
=============================================================================*/

USE ROLE ACCOUNTADMIN;

-- ============================================
-- STEP 1: DATABASE & SCHEMA
-- ============================================
CREATE DATABASE IF NOT EXISTS INTEGRATIONS_DB;
CREATE SCHEMA IF NOT EXISTS INTEGRATIONS_DB.GIT;
USE DATABASE INTEGRATIONS_DB;
USE SCHEMA GIT;

-- ============================================
-- STEP 2: CREATE GITHUB PAT SECRET
-- ============================================
-- IMPORTANT: Replace <YOUR_GITHUB_PAT> with your actual PAT.
-- Generate one at: https://github.com/settings/tokens
-- Required scopes: repo (full control)
-- NEVER commit the actual PAT value to version control.

CREATE SECRET IF NOT EXISTS INTEGRATIONS_DB.GIT.GIT_SECRET
  TYPE = PASSWORD
  USERNAME = 'Devikapgdevi'
  PASSWORD = '<YOUR_GITHUB_PAT>'
  COMMENT = 'GitHub PAT for Healthcare-RxDecision-Data-Hub repo';

-- To rotate/update the PAT later:
-- ALTER SECRET INTEGRATIONS_DB.GIT.GIT_SECRET SET PASSWORD = '<YOUR_NEW_PAT>';

-- Verify:
DESCRIBE SECRET INTEGRATIONS_DB.GIT.GIT_SECRET;

-- ============================================
-- STEP 3: API INTEGRATION
-- ============================================
CREATE OR REPLACE API INTEGRATION GIT_API_INTEGRATION
  API_PROVIDER = git_https_api
  API_ALLOWED_PREFIXES = ('https://github.com/Devikapgdevi')
  ALLOWED_AUTHENTICATION_SECRETS = (INTEGRATIONS_DB.GIT.GIT_SECRET)
  ENABLED = TRUE;

DESCRIBE INTEGRATION GIT_API_INTEGRATION;

-- ============================================
-- STEP 4: CONNECT GIT REPOSITORY
-- ============================================
CREATE OR REPLACE GIT REPOSITORY INTEGRATIONS_DB.GIT.HEALTHCARE_RXDECISION_DATA_HUB
  API_INTEGRATION = GIT_API_INTEGRATION
  GIT_CREDENTIALS = INTEGRATIONS_DB.GIT.GIT_SECRET
  ORIGIN = 'https://github.com/Devikapgdevi/Healthcare-RxDecision-Data-Hub.git';

-- ============================================
-- STEP 5: FETCH & VERIFY
-- ============================================
ALTER GIT REPOSITORY INTEGRATIONS_DB.GIT.HEALTHCARE_RXDECISION_DATA_HUB FETCH;
SHOW GIT BRANCHES IN INTEGRATIONS_DB.GIT.HEALTHCARE_RXDECISION_DATA_HUB;
DESCRIBE GIT REPOSITORY INTEGRATIONS_DB.GIT.HEALTHCARE_RXDECISION_DATA_HUB;
LIST @INTEGRATIONS_DB.GIT.HEALTHCARE_RXDECISION_DATA_HUB/branches/main/;

-- ============================================
-- STEP 6: GRANT PERMISSIONS
-- ============================================
GRANT USAGE ON DATABASE INTEGRATIONS_DB TO ROLE HC_ACCOUNT_ADMIN;
GRANT USAGE ON SCHEMA INTEGRATIONS_DB.GIT TO ROLE HC_ACCOUNT_ADMIN;
GRANT USAGE ON INTEGRATION GIT_API_INTEGRATION TO ROLE HC_ACCOUNT_ADMIN;
GRANT READ, WRITE ON GIT REPOSITORY INTEGRATIONS_DB.GIT.HEALTHCARE_RXDECISION_DATA_HUB TO ROLE HC_ACCOUNT_ADMIN;

GRANT USAGE ON DATABASE INTEGRATIONS_DB TO ROLE HC_DATA_ENGINEER;
GRANT USAGE ON SCHEMA INTEGRATIONS_DB.GIT TO ROLE HC_DATA_ENGINEER;
GRANT READ, WRITE ON GIT REPOSITORY INTEGRATIONS_DB.GIT.HEALTHCARE_RXDECISION_DATA_HUB TO ROLE HC_DATA_ENGINEER;

GRANT USAGE ON DATABASE INTEGRATIONS_DB TO ROLE HC_ANALYST;
GRANT USAGE ON SCHEMA INTEGRATIONS_DB.GIT TO ROLE HC_ANALYST;
GRANT READ ON GIT REPOSITORY INTEGRATIONS_DB.GIT.HEALTHCARE_RXDECISION_DATA_HUB TO ROLE HC_ANALYST;

-- ============================================
-- TROUBLESHOOTING
-- ============================================
-- If connection fails:
--   1. Verify PAT is valid:       DESCRIBE SECRET INTEGRATIONS_DB.GIT.GIT_SECRET;
--   2. Verify integration:        DESCRIBE INTEGRATION GIT_API_INTEGRATION;
--   3. Verify repo exists:        SHOW GIT REPOSITORIES IN INTEGRATIONS_DB.GIT;
--   4. Re-fetch:                  ALTER GIT REPOSITORY INTEGRATIONS_DB.GIT.HEALTHCARE_RXDECISION_DATA_HUB FETCH;
--   5. Rotate expired PAT:        ALTER SECRET INTEGRATIONS_DB.GIT.GIT_SECRET SET PASSWORD = '<NEW_PAT>';
