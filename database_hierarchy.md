        # Healthcare RxDecision Data Hub - Project Structure

**Account:** `tyb42779` | **User:** `DEVIKAPG` | **Role:** `ACCOUNTADMIN`

---

## Project Overview

An end-to-end healthcare analytics platform on Snowflake using the **4-layer Medallion Architecture**. Combines real-world Snowflake Marketplace data (Definitive Healthcare) with synthetic patient/ICU/billing data. Includes RBAC, data governance, monitoring, CI/CD, and a 7-tab Streamlit dashboard.

**Total Records:** ~29,000 across 17 tables + 1 semantic view ---

## Architecture Diagram

```
  SNOWFLAKE MARKETPLACE                      SYNTHETIC DATA
  (Definitive Healthcare)                    (GENERATOR + RANDOM)
         |                                         |
         v                                         v
 +=================== BRONZE LAYER ====================+
 |                   RAW_DB.RAW_SCHEMA                  |
 |                                                      |
 |  Marketplace-sourced:          Synthetic:            |
 |  - HOSPITALS (100)             - PATIENT_RAW (2,000) |
 |  - HCO_LOCATIONS (100)         - ICU_EVENTS (4,000)  |
 |  - PROVIDER_AFFILIATIONS (100) - BILLING_DATA (3,000)|
 |  - PRESCRIPTION_DATA (100)     - MEDICATION_RECORDS  |
 |                                  (3,000)             |
 |  Reference:                    - DEVICE_ALERTS       |
 |  - ICD10_REFERENCE (30)          (1,500)             |
 +==========================|===========================+
                            |
            +PII masking    |    +IS_CRITICAL flag
            +ETL_TIMESTAMP  |    +ETL_TIMESTAMP
                            v
 +=================== SILVER LAYER ====================+
 |             TRANSFORM_DB.TRANSFORM_SCHEMA            |
 |                                                      |
 |  CLEAN_PATIENT (2,000)      CLEAN_ICU_EVENTS (4,000)|
 +==========================|===========================+
                            |
            +ICU aggregates |    +Billing pivot by status
            +Billing totals |
                            v
 +==================== GOLD LAYER =====================+
 |             ANALYTICS_DB.ANALYTICS_SCHEMA            |
 |                                                      |
 |  PATIENT_ANALYTICS (2,000)  BILLING_ANALYTICS(1,551)|
 +==========================|===========================+
                            |
            +Statistical features   +Risk scoring (HIGH/MED/LOW)
            +Embeddings (8-dim)     +Clinical notes (NLP-ready)
                            v
 +================== PLATINUM LAYER ===================+
 |                    AI_READY_DB                      |
 |                                                     |
 |  FEATURE_STORE schema:                              |
 |    ICU_FEATURE_STORE (1,739)                        |
 |    PATIENT_NOTES (2,000)                            |
 |    PATIENT_EMBEDDINGS (2,000)                       |
 |                                                     |
 |  SEMANTIC_MODELS schema:                            |
 |    V_PATIENT_SEMANTIC (view)                        |
 +======================================================+
```

---

## Databases & Schemas

### Medallion Layer Databases (4)

| Database | Schema | Layer | Purpose | Tables |
|----------|--------|-------|---------|--------|
| `RAW_DB` | `RAW_SCHEMA` | Bronze | Raw ingestion from marketplace + synthetic generators | 10 tables, 13,930 rows |
| `TRANSFORM_DB` | `TRANSFORM_SCHEMA` | Silver | Cleaned, validated data with derived fields | 2 tables, 6,000 rows |
| `ANALYTICS_DB` | `ANALYTICS_SCHEMA` | Gold | Pre-aggregated analytics-ready tables | 2 tables, 3,551 rows |
| `AI_READY_DB` | `FEATURE_STORE` | Platinum | ML features, embeddings, clinical notes | 3 tables, 5,739 rows |
| `AI_READY_DB` | `SEMANTIC_MODELS` | Platinum | Cortex Analyst semantic view | 1 view |

### Support Databases (4)

| Database | Schema | Purpose | Objects |
|----------|--------|---------|---------|
| `MONITORING_DB` | `MONITORING_SCHEMA` | Query performance, credit usage, login tracking | 11 views, 2 tables |
| `AUDIT_DB` | `AUDIT_SCHEMA` | HIPAA compliance, access history, data changes | 6 views |
| `GOVERNANCE_DB` | `TAGS` | Data classification tags (PII, PHI, sensitivity) | 3 tags |
| `GOVERNANCE_DB` | `POLICIES` | Masking policies (SSN, phone, insurance) + row access | 4 policies |
| `INTEGRATIONS_DB` | `GIT` | GitHub repo connection for CI/CD | API integration + Git repo |

---

## Tables by Layer

### BRONZE - RAW_DB.RAW_SCHEMA (10 tables)

| Table | Rows | Source | Description |
|-------|------|--------|-------------|
| `ICD10_REFERENCE` | 30 | Static | ICD-10 code lookup (5 codes x 6 diagnosis categories) |
| `PATIENT_RAW` | 2,000 | Synthetic + Marketplace seed | Patient demographics with ICD-10 mapping |
| `ICU_EVENTS` | 4,000 | Synthetic | ICU vital sign readings (heart rate, oxygen level) |
| `BILLING_DATA` | 3,000 | Synthetic | Bills with PAID/PENDING/OVERDUE status |
| `MEDICATION_RECORDS` | 3,000 | Synthetic | Prescriptions (10 medications, dosages, frequency) |
| `DEVICE_ALERTS` | 1,500 | Synthetic | IoT medical device alerts (5 types, 3 severities) |
| `HOSPITALS` | 100 | Marketplace | Hospital profiles from Definitive Healthcare |
| `HCO_LOCATIONS` | 100 | Marketplace | Organization locations with geo coordinates |
| `PROVIDER_AFFILIATIONS` | 100 | Marketplace | Provider-to-organization affiliation scores |
| `PRESCRIPTION_DATA` | 100 | Marketplace | Rx therapy decisions (8 event types) |

### SILVER - TRANSFORM_DB.TRANSFORM_SCHEMA (2 tables)

| Table | Rows | Source Table | What Changed |
|-------|------|-------------|--------------|
| `CLEAN_PATIENT` | 2,000 | PATIENT_RAW | Added `PATIENT_ID_MASKED`, `ETL_TIMESTAMP` |
| `CLEAN_ICU_EVENTS` | 4,000 | ICU_EVENTS | Added `IS_CRITICAL` flag (HR<60 or HR>100 or O2<92), `ETL_TIMESTAMP` |

### GOLD - ANALYTICS_DB.ANALYTICS_SCHEMA (2 tables)

| Table | Rows | Source Tables | What Changed |
|-------|------|--------------|--------------|
| `PATIENT_ANALYTICS` | 2,000 | CLEAN_PATIENT + CLEAN_ICU_EVENTS | One row per patient with ICU_EVENT_COUNT, AVG_HEART_RATE, AVG_OXYGEN_LEVEL, CRITICAL_EVENT_COUNT |
| `BILLING_ANALYTICS` | 1,551 | BILLING_DATA | One row per patient with TOTAL_AMOUNT, PAID_AMOUNT, PENDING_AMOUNT, OVERDUE_AMOUNT |

### PLATINUM - AI_READY_DB (3 tables + 1 view)

| Table | Schema | Rows | Purpose |
|-------|--------|------|---------|
| `ICU_FEATURE_STORE` | FEATURE_STORE | 1,739 | ML features: statistical aggregates (min/max/avg/stddev), ICU stay days, critical event rate, risk score (HIGH/MEDIUM/LOW) |
| `PATIENT_NOTES` | FEATURE_STORE | 2,000 | Synthetic clinical notes for NLP (5 templates, 4 note types) |
| `PATIENT_EMBEDDINGS` | FEATURE_STORE | 2,000 | 8-dimensional float vectors for similarity search |
| `V_PATIENT_SEMANTIC` | SEMANTIC_MODELS | view | Cortex Analyst semantic view with human-readable column names and risk flags |

---

## Key Relationships

```
PATIENT_RAW.PATIENT_ID (PK, P00001-P02000)
    |
    +--> ICU_EVENTS.PATIENT_ID -----------> CLEAN_ICU_EVENTS --> PATIENT_ANALYTICS
    |                                                        --> ICU_FEATURE_STORE
    +--> BILLING_DATA.PATIENT_ID ---------> BILLING_ANALYTICS
    +--> MEDICATION_RECORDS.PATIENT_ID
    +--> DEVICE_ALERTS.PATIENT_ID
    +--> PATIENT_NOTES.PATIENT_ID
    |
    +--> CLEAN_PATIENT -------------------> PATIENT_ANALYTICS
                                        --> PATIENT_EMBEDDINGS
                                        --> V_PATIENT_SEMANTIC

ICD10_REFERENCE.DIAGNOSIS_CATEGORY = PATIENT_RAW.DIAGNOSIS

HOSPITALS.DEFINITIVE_ID = HCO_LOCATIONS.DEFINITIVE_ID
HOSPITALS.FACILITY_NPI = PROVIDER_AFFILIATIONS.HCO_NPI
```

---

## Streamlit Dashboard (7 Tabs)

File: `streamlit/healthcare_dashboard.py`

| Tab | Name | Data Sources | Visualizations |
|-----|------|-------------|----------------|
| 1 | **Overview** | PATIENT_RAW, ICU_EVENTS, BILLING_DATA, PRESCRIPTION_DATA | KPI metrics, diagnosis bar chart, admission trend area chart |
| 2 | **Patient Explorer** | PATIENT_RAW | Filterable data table (diagnosis + gender), age distribution bar chart |
| 3 | **ICU Vitals** | CLEAN_ICU_EVENTS | Avg HR/O2 metrics, critical event count, event type breakdown, daily trend lines |
| 4 | **Billing** | BILLING_ANALYTICS, BILLING_DATA | Revenue/paid/overdue KPIs, status donut chart, monthly revenue trend |
| 5 | **Rx Decisions** | PRESCRIPTION_DATA, MEDICATION_RECORDS | Marketplace Rx data table, event type chart, top medications chart |
| 6 | **Hospitals & Providers** | HOSPITALS, HCO_LOCATIONS, PROVIDER_AFFILIATIONS | Hospital count metrics, details table, locations by region chart |
| 7 | **AI Risk Scoring** | ICU_FEATURE_STORE | HIGH/MEDIUM/LOW risk KPIs, risk by diagnosis stacked bar, top high-risk patients table |

---

## Warehouses (5)

| Warehouse | Size | Auto-Suspend | Assigned Role | Resource Monitor |
|-----------|------|-------------|---------------|-----------------|
| `COMPUTE_WH` | XSMALL | 600s | ACCOUNTADMIN | - |
| `HC_ETL_WH` | XSMALL | 60s | HC_DATA_ENGINEER | HC_ETL_WH_MONITOR (10 credits/mo) |
| `HC_TRANSFORM_WH` | XSMALL | 60s | HC_DATA_ENGINEER | HC_TRANSFORM_WH_MONITOR (10 credits/mo) |
| `HC_ANALYTICS_WH` | XSMALL | 60s | HC_ANALYST | HC_ANALYTICS_WH_MONITOR (10 credits/mo) |
| `HC_AI_WH` | XSMALL | 60s | HC_DATA_SCIENTIST | HC_AI_WH_MONITOR (10 credits/mo) |

Account monitor: `HC_ACCOUNT_MONITOR` (50 credits/mo)

---

## RBAC Hierarchy (6 Roles)

```
ACCOUNTADMIN
    |
    +-- HC_ACCOUNT_ADMIN
            |
            +-- HC_SECURITY_ADMIN      (security policies, governance)
            |
            +-- HC_DATA_SCIENTIST      (AI_READY_DB, HC_AI_WH)
            |
            +-- HC_DATA_ENGINEER       (RAW_DB, TRANSFORM_DB, HC_ETL_WH, HC_TRANSFORM_WH)
                    |
                    +-- HC_ANALYST     (ANALYTICS_DB, HC_ANALYTICS_WH)
                            |
                            +-- HC_VIEWER  (read-only access)
```

---

## Data Governance

### Tags (GOVERNANCE_DB.TAGS)

| Tag | Values | Applied To|
|-----|--------|-----------|
| `DATA_CLASSIFICATION` | PUBLIC, INTERNAL, CONFIDENTIAL, RESTRICTED | Tables/columns by sensitivity |
| `PII_TYPE` | SSN, PHONE, EMAIL, ADDRESS, DOB, NAME | Patient PII columns |
| `PHI_TYPE` | DIAGNOSIS, TREATMENT, LAB_RESULTS, VITALS, MEDICATIONS | HIPAA PHI columns |

### Policies (GOVERNANCE_DB.POLICIES)

| Policy | Type | Behavior |
|--------|------|----------|
| `SSN_MASK` | Masking | Shows `***-**-1234` except for ACCOUNTADMIN/HC_ACCOUNT_ADMIN/HC_DATA_ENGINEER |
| `PHONE_MASK` | Masking | Shows `***-***-1234` except for admin roles |
| `INSURANCE_MASK` | Masking | Partially masks insurance IDs |
| `REGION_ACCESS` | Row Access | Restricts rows by region based on role |

---

## Monitoring & Audit

### MONITORING_DB.MONITORING_SCHEMA

| View | Tracks |
|------|--------|
| `QUERY_HISTORY_VIEW` | All queries (30 days) |
| `LOGIN_HISTORY_VIEW` | All logins (30 days) |
| `FAILED_LOGINS_VIEW` | Failed logins (7 days) |
| `LONG_RUNNING_QUERIES_VIEW` | Queries > 5 min (7 days) |
| `CREDIT_USAGE_VIEW` | Daily credits by warehouse (30 days) |
| `DAILY_CREDIT_USAGE` | Aggregated daily credit usage |
| `CREDIT_SAVINGS_TRACKER` | Cost optimization tracking |
| `WAREHOUSE_LOAD_HISTORY_VIEW` | Warehouse load patterns |
| `WAREHOUSE_PERFORMANCE_SUMMARY` | Performance metrics |
| `BLOCKED_QUERIES_VIEW` | Queries with errors |
| `STORAGE_USAGE_VIEW` | Storage bytes (30 days) |

### AUDIT_DB.AUDIT_SCHEMA

| View | Tracks |
|------|--------|
| `ACCESS_HISTORY_VIEW` | Who accessed what objects (30 days) |
| `DATA_CHANGES_AUDIT` | INSERT/UPDATE/DELETE operations (30 days) |
| `ROLE_GRANTS_AUDIT` | Role privilege changes (90 days) |
| `USER_LOGIN_AUDIT` | Login attempts with auth details (30 days) |
| `SENSITIVE_DATA_ACCESS` | Queries touching patient/SSN data (7 days) |
| `COMPLIANCE_SUMMARY` | Aggregate: total queries, failed logins, data mods |

### Alerts (MONITORING_DB)

| Alert | Schedule | Trigger |
|-------|----------|---------|
| `HC_LONG_QUERY_ALERT` | 5 min | Query > 10 min execution |
| `HC_FAILED_LOGIN_ALERT` | 15 min | > 3 failed logins |
| `HC_CREDIT_ALERT` | 60 min | Warehouse > 10 credits/hour |

---

## CI/CD (GitHub Integration)

| Component | Location | Description |
|-----------|----------|-------------|
| API Integration | `GITHUB_API_INTEGRATION` | Connects Snowflake to github.com/Devikapgdevi |
| PAT Secret | `INTEGRATIONS_DB.GIT.GITHUB_PAT_SECRET` | GitHub authentication |
| Git Repository | `INTEGRATIONS_DB.GIT.HEALTHCARE_ENTERPRISE_REPO` | Linked repo |
| GitHub Actions | `.github/workflows/snowflake-deploy.yml` | Auto-deploy SQL on push to main |

---

## SQL Scripts (Deployment Order)

| # | File | Purpose |
|---|------|---------|
| 01 | `01_account_administration.sql` | Network + password + session policies |
| 02 | `02_rbac_setup.sql` | 6 custom roles + hierarchy |
| 03 | `03_warehouse_management.sql` | 4 XSMALL warehouses |
| 04 | `04_database_structure.sql` | 8 databases + schemas + grants |
| 05 | `05_resource_monitors.sql` | 5 cost monitors |
| 06 | `06_monitoring_views.sql` | 11 monitoring views |
| 07 | `07_alerts.sql` | Email notification integration + 3 alerts |
| 08 | `08_data_governance.sql` | Tags + masking + row access policies |
| 09 | `09_audit_layer.sql` | 6 audit/compliance views |
| 10 | `10_verification.sql` | Deployment validation checks |
| 11 | `11_medallion_architecture.sql` | Full 4-layer pipeline (Bronze -> Silver -> Gold -> Platinum) |
| 12 | `12_healthcare_industry.sql` | Standalone re-run: DEVICE_ALERTS + MEDICATION_RECORDS |
| 13 | `13_ai_ready_layer.sql` | Standalone re-run: Platinum layer |

---

## File Structure

```
Healthcare-RxDecision-Data-Hub/
|
+-- .github/workflows/
|   +-- snowflake-deploy.yml          # GitHub Actions CI/CD pipeline
|   +-- github_integration.sql        # Snowflake Git repo setup
|
+-- sql/
|   +-- 01_account_administration.sql
|   +-- 02_rbac_setup.sql
|   +-- 03_warehouse_management.sql
|   +-- 04_database_structure.sql
|   +-- 05_resource_monitors.sql
|   +-- 06_monitoring_views.sql
|   +-- 07_alerts.sql
|   +-- 08_data_governance.sql
|   +-- 09_audit_layer.sql
|   +-- 10_verification.sql
|   +-- 11_medallion_architecture.sql  # Main pipeline
|   +-- 12_healthcare_industry.sql     # Standalone Bronze re-run
|   +-- 13_ai_ready_layer.sql          # Standalone Platinum re-run
|   +-- master_deployment.sql          # Deployment guide
|
+-- streamlit/
|   +-- healthcare_dashboard.py        # 7-tab Streamlit in Snowflake app
|
+-- docs/
|   +-- database_hierarchy.md          # This file
|   +-- structure.md                   # Detailed table/column reference
|   +-- phase_documentation.md
|   +-- test_cases.md
|
+-- README.md
```



Database	 Tables Views TOTAL 	
RAW_DB	        9	0	   9
TRANSFORM_DB	2	0	   2
ANALYTICS_DB	2	0	   2
AI_READY_DB  	3	1	   4
AUDIT_DB	    7	7	  14
MONITORING_DB 	2	11	  13
GOVERNANCE_DB	0	0	   0
SECURITY_DB 	0	0	   0
DEVOPS_DB	    0	0	   0
INTEGRATIONS_DB	0	0	   0


