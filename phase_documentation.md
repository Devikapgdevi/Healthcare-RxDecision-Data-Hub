# Healthcare RxDecision Analytics Platform — Complete Phase Documentation

**Account:** `tyb42779` | **Author:** `DEVIKAPG` | **Role:** `ACCOUNTADMIN` | **Date:** March 2026
**Data Sources:** Snowflake Marketplace (Definitive Healthcare) + Synthetic Data Generators
**Total Records:** ~29,000 across 17 tables + 1 semantic view

---

## Technology Stack

| Layer | Technology | Role in Project |
|-------|-----------|----------------|
| **Cloud Platform** | Snowflake Data Cloud | Core data warehouse — 10 databases, compute, storage, governance |
| **Data Marketplace** | Snowflake Marketplace | Definitive Healthcare RxDecision Insights (hospitals, HCO, Rx therapy data) |
| **Data Architecture** | Medallion Architecture (4-layer) | Bronze → Silver → Gold → Platinum data flow |
| **Python Runtime** | Snowpark Python | Server-side Python execution, session management for Streamlit |
| **Dashboard** | Streamlit in Snowflake | 7-tab interactive healthcare analytics dashboard |
| **Visualizations** | Altair | Bar, area, arc/donut, and line charts across all dashboard tabs |
| **Semantic Layer** | Cortex Analyst | Semantic view (`V_PATIENT_SEMANTIC`) for natural-language SQL generation |
| **Access Control** | Snowflake RBAC | 6-role hierarchy with least-privilege access |
| **Data Governance** | Tags + Masking Policies | HIPAA-compliant PII/PHI classification and column masking |
| **Monitoring** | Snowflake Alerts + Views | 11 monitoring views, 3 automated alerts, resource monitors |
| **Audit & Compliance** | Snowflake Account Usage | 6 compliance views + 8 snapshot tables for HIPAA audit trail |
| **CI/CD** | GitHub Actions | Automated SQL deployment on push to main branch |
| **Version Control** | Git Integration (Snowflake) | `INTEGRATIONS_DB.GIT.HEALTHCARE_ENTERPRISE_REPO` linked to GitHub |

---

## Table of Contents

1. [Phase 1: Account Administration](#phase-1-account-administration)
2. [Phase 2: RBAC Setup](#phase-2-rbac-setup)
3. [Phase 3: Warehouse Management](#phase-3-warehouse-management)
4. [Phase 4: Database Structure](#phase-4-database-structure)
5. [Phase 5: Resource Monitors](#phase-5-resource-monitors)
6. [Phase 6: Monitoring Views](#phase-6-monitoring-views)
7. [Phase 7: Alerts](#phase-7-alerts)
8. [Phase 8: Data Governance](#phase-8-data-governance)
9. [Phase 9: Audit Layer](#phase-9-audit-layer)
10. [Phase 10: Verification](#phase-10-verification)
11. [Phase 11: Medallion Architecture Pipeline](#phase-11-medallion-architecture-pipeline)
12. [Phase 12: Healthcare Industry Data](#phase-12-healthcare-industry-data)
13. [Phase 13: AI-Ready Layer](#phase-13-ai-ready-layer)
14. [Streamlit Dashboard — 7 Tabs](#streamlit-dashboard--7-tabs)

---

## Phase 1: Account Administration

**Script:** `sql/01_account_administration.sql`
**Purpose:** Establish HIPAA-compliant security policies at the account level.

| Policy | Type | Configuration |
|--------|------|---------------|
| `HC_NETWORK_POLICY` | Network Policy | Controls allowed/blocked IP ranges for account access |
| `HC_PASSWORD_POLICY` | Password Policy | HIPAA-compliant password requirements (length, complexity, expiry, lockout) |
| `HC_SESSION_POLICY` | Session Policy | Auto-timeout for idle sessions to prevent unauthorized access |

**What It Displays:** Enforces security at the account perimeter — ensures only authorized networks, strong passwords, and time-limited sessions protect all healthcare data.

---

## Phase 2: RBAC Setup

**Script:** `sql/02_rbac_setup.sql`
**Purpose:** Create a 6-role hierarchy implementing least-privilege access for healthcare workflows.

```
ACCOUNTADMIN
    |
    +-- HC_ACCOUNT_ADMIN          (full project admin)
            |
            +-- HC_SECURITY_ADMIN (security policies, governance, audit)
            |
            +-- HC_DATA_SCIENTIST (AI_READY_DB, HC_AI_WH)
            |
            +-- HC_DATA_ENGINEER  (RAW_DB, TRANSFORM_DB, HC_ETL_WH, HC_TRANSFORM_WH)
                    |
                    +-- HC_ANALYST    (ANALYTICS_DB, HC_ANALYTICS_WH, read-only upstream)
                            |
                            +-- HC_VIEWER (read-only access to analytics outputs)
```

| Role | Access Scope | Warehouse |
|------|-------------|-----------|
| `HC_ACCOUNT_ADMIN` | All databases, all warehouses | All |
| `HC_SECURITY_ADMIN` | GOVERNANCE_DB, AUDIT_DB, SECURITY_DB | HC_ANALYTICS_WH |
| `HC_DATA_SCIENTIST` | AI_READY_DB (read/write), ANALYTICS_DB (read) | HC_AI_WH |
| `HC_DATA_ENGINEER` | RAW_DB, TRANSFORM_DB (read/write) | HC_ETL_WH, HC_TRANSFORM_WH |
| `HC_ANALYST` | ANALYTICS_DB (read/write) | HC_ANALYTICS_WH |
| `HC_VIEWER` | ANALYTICS_DB (read-only) | HC_ANALYTICS_WH |

**What It Displays:** A complete role-based access control framework ensuring data scientists cannot modify raw data, analysts cannot access PII, and viewers have read-only access to final analytics outputs.

---

## Phase 3: Warehouse Management

**Script:** `sql/03_warehouse_management.sql`
**Purpose:** Create purpose-specific compute warehouses with aggressive auto-suspend for cost optimization.

| Warehouse | Size | Auto-Suspend | Auto-Resume | Purpose |
|-----------|------|-------------|-------------|---------|
| `COMPUTE_WH` | XSMALL | 600s | Yes | General compute (ACCOUNTADMIN) |
| `HC_ETL_WH` | XSMALL | 60s | Yes | ETL/ingestion workloads |
| `HC_TRANSFORM_WH` | XSMALL | 60s | Yes | Data transformation (Silver layer) |
| `HC_ANALYTICS_WH` | XSMALL | 60s | Yes | Analytics queries + dashboard |
| `HC_AI_WH` | XSMALL | 60s | Yes | AI/ML feature engineering |

**What It Displays:** Workload isolation — each pipeline stage has its own warehouse, preventing ETL jobs from competing with analyst queries. 60-second auto-suspend minimizes credit consumption.

---

## Phase 4: Database Structure

**Script:** `sql/04_database_structure.sql`
**Purpose:** Create the 10-database architecture spanning data processing, governance, monitoring, and DevOps.

### Medallion Layer Databases (4)

| Database | Schema | Layer | Purpose |
|----------|--------|-------|---------|
| `RAW_DB` | `RAW_SCHEMA` | Bronze | Raw data ingestion — marketplace + synthetic |
| `TRANSFORM_DB` | `TRANSFORM_SCHEMA` | Silver | Cleansed data with PII masking and derived flags |
| `ANALYTICS_DB` | `ANALYTICS_SCHEMA` | Gold | Pre-aggregated, analytics-ready tables |
| `AI_READY_DB` | `FEATURE_STORE`, `SEMANTIC_MODELS` | Platinum | ML features, embeddings, semantic view |

### Support Databases (6)

| Database | Purpose |
|----------|---------|
| `MONITORING_DB` | Query performance, credit usage, login tracking |
| `AUDIT_DB` | HIPAA compliance, access history, data change tracking |
| `GOVERNANCE_DB` | Data classification tags, masking/row-access policies |
| `SECURITY_DB` | Security policies and configurations |
| `DEVOPS_DB` | CI/CD and DevOps workflows |
| `INTEGRATIONS_DB` | GitHub API integration, Git repository connection |

**What It Displays:** A clear separation of concerns — data flows through Bronze → Silver → Gold → Platinum while governance, auditing, and monitoring operate independently.

---

## Phase 5: Resource Monitors

**Script:** `sql/05_resource_monitors.sql`
**Purpose:** Set credit budgets on every warehouse and the account to prevent cost overruns.

| Monitor | Scope | Credit Limit | Actions |
|---------|-------|-------------|---------|
| `HC_ACCOUNT_MONITOR` | Account-level | 50 credits/month | Notify at 75%, suspend at 90%, hard suspend at 100% |
| `HC_ETL_WH_MONITOR` | HC_ETL_WH | 10 credits/month | Notify at 75%, suspend at 90% |
| `HC_TRANSFORM_WH_MONITOR` | HC_TRANSFORM_WH | 10 credits/month | Notify at 75%, suspend at 90% |
| `HC_ANALYTICS_WH_MONITOR` | HC_ANALYTICS_WH | 10 credits/month | Notify at 75%, suspend at 90% |
| `HC_AI_WH_MONITOR` | HC_AI_WH | 10 credits/month | Notify at 75%, suspend at 90% |

**What It Displays:** Full cost governance — each warehouse is capped at 10 credits/month, the entire account at 50 credits/month, with progressive alerting before suspension.

---

## Phase 6: Monitoring Views

**Script:** `sql/06_monitoring_views.sql`
**Purpose:** Create operational observability views in `MONITORING_DB.MONITORING_SCHEMA`.

| View | Data Source | What It Tracks | Lookback |
|------|-----------|----------------|----------|
| `QUERY_HISTORY_VIEW` | ACCOUNT_USAGE | All executed queries | 30 days |
| `LOGIN_HISTORY_VIEW` | ACCOUNT_USAGE | Login attempts (success + failure) | 30 days |
| `FAILED_LOGINS_VIEW` | ACCOUNT_USAGE | Failed authentication attempts | 7 days |
| `LONG_RUNNING_QUERIES_VIEW` | ACCOUNT_USAGE | Queries exceeding 5-minute execution | 7 days |
| `CREDIT_USAGE_VIEW` | ACCOUNT_USAGE | Daily credit consumption by warehouse | 30 days |
| `DAILY_CREDIT_USAGE` | ACCOUNT_USAGE | Aggregated daily credit totals | 30 days |
| `CREDIT_SAVINGS_TRACKER` | ACCOUNT_USAGE | Cost optimization tracking | 30 days |
| `WAREHOUSE_LOAD_HISTORY_VIEW` | ACCOUNT_USAGE | Warehouse load patterns | 30 days |
| `WAREHOUSE_PERFORMANCE_SUMMARY` | ACCOUNT_USAGE | Performance metrics per warehouse | 30 days |
| `BLOCKED_QUERIES_VIEW` | ACCOUNT_USAGE | Queries with errors | 7 days |
| `STORAGE_USAGE_VIEW` | ACCOUNT_USAGE | Storage bytes consumed | 30 days |

**What It Displays:** A complete operational dashboard — track slow queries, failed logins, credit burn rate, warehouse performance, and storage growth from a single monitoring database.

---

## Phase 7: Alerts

**Script:** `sql/07_alerts.sql`
**Purpose:** Automated alerting for critical operational events.

| Alert | Schedule | Trigger Condition | Notification |
|-------|----------|-------------------|-------------|
| `HC_LONG_QUERY_ALERT` | Every 5 min | Any query running > 10 minutes | Email |
| `HC_FAILED_LOGIN_ALERT` | Every 15 min | > 3 failed login attempts | Email |
| `HC_CREDIT_ALERT` | Every 60 min | Any warehouse consuming > 10 credits/hour | Email |

**Alert Log Table:** `MONITORING_DB.MONITORING_SCHEMA.ALERT_LOG` — stores triggered alert records with `ALERT_NAME`, `ALERT_TIME`, `MESSAGE`.

**What It Displays:** Proactive incident detection — security threats (brute-force logins), performance issues (long queries), and cost anomalies (credit spikes) are caught automatically.

---

## Phase 8: Data Governance

**Script:** `sql/08_data_governance.sql`
**Purpose:** Implement HIPAA-compliant data classification, masking, and row-level access control.

### Tags (`GOVERNANCE_DB.TAGS`)

| Tag | Allowed Values | Applied To |
|-----|---------------|-----------|
| `DATA_CLASSIFICATION` | PUBLIC, INTERNAL, CONFIDENTIAL, RESTRICTED | Tables and columns by sensitivity level |
| `PII_TYPE` | SSN, PHONE, EMAIL, ADDRESS, DOB, NAME | Patient personally identifiable information columns |
| `PHI_TYPE` | DIAGNOSIS, TREATMENT, LAB_RESULTS, VITALS, MEDICATIONS | HIPAA protected health information columns |

### Masking Policies (`GOVERNANCE_DB.POLICIES`)

| Policy | Type | Behavior | Exempt Roles |
|--------|------|----------|-------------|
| `SSN_MASK` | Column Masking | Full SSN → `***-**-1234` | ACCOUNTADMIN, HC_ACCOUNT_ADMIN, HC_DATA_ENGINEER |
| `PHONE_MASK` | Column Masking | Full phone → `***-***-1234` | Admin roles |
| `INSURANCE_MASK` | Column Masking | Partially masks insurance IDs | Admin roles |
| `REGION_ACCESS` | Row Access Policy | Restricts visible rows by geographic region based on user role | ACCOUNTADMIN |

**What It Displays:** HIPAA compliance at the column and row level — sensitive data is automatically masked for unauthorized roles, and row access is restricted by region.

---

## Phase 9: Audit Layer

**Script:** `sql/09_audit_layer.sql`
**Purpose:** Create compliance and audit views in `AUDIT_DB.AUDIT_SCHEMA` plus snapshot tables of all data layers.

### Compliance Views

| View | What It Tracks | Lookback |
|------|----------------|----------|
| `ACCESS_HISTORY_VIEW` | Who accessed what objects and when | 30 days |
| `DATA_CHANGES_AUDIT` | INSERT/UPDATE/DELETE operations on tables | 30 days |
| `ROLE_GRANTS_AUDIT` | Privilege and role grant changes | 90 days |
| `USER_LOGIN_AUDIT` | Login attempts with authentication details | 30 days |
| `SENSITIVE_DATA_ACCESS` | Queries touching patient/SSN/PHI data specifically | 7 days |
| `COMPLIANCE_SUMMARY` | Aggregated: total queries, failed logins, data modifications | Rolling |

### Audit Snapshot Tables

| Table | Rows | Source | Purpose |
|-------|------|--------|---------|
| `CLEAN_PATIENT` | 2,000 | TRANSFORM_DB | Point-in-time snapshot of transformed patient data |
| `CLEAN_ICU_EVENTS` | 4,000 | TRANSFORM_DB | Snapshot of cleansed ICU events |
| `PATIENT_ANALYTICS` | 2,000 | ANALYTICS_DB | Snapshot of analytics output |
| `ICU_FEATURE_STORE` | 1,739 | AI_READY_DB | Snapshot of ML feature data |
| `BILLING_ANALYTICS` | 1,551 | ANALYTICS_DB | Snapshot of billing summaries |
| `PATIENT_NOTES` | 2,000 | AI_READY_DB | Snapshot of clinical notes |
| `PATIENT_EMBEDDINGS` | 2,000 | AI_READY_DB | Snapshot of vector embeddings |
| `V_PATIENT_SEMANTIC` | view | AI_READY_DB | Audit copy of the semantic view |

**What It Displays:** Full HIPAA audit trail — every data access, modification, and privilege change is logged. Snapshot tables preserve data state for compliance investigations.

---

## Phase 10: Verification

**Script:** `sql/10_verification.sql`
**Purpose:** Validate that all preceding phases deployed successfully.

**Checks performed:**
- All 10 databases exist and are accessible
- All 6 custom roles are created with correct hierarchy
- All 4+1 warehouses are active
- All 5 resource monitors are attached
- All monitoring views return data
- All alerts are scheduled and active
- All governance tags and policies are applied
- All audit views are functional
- Row counts on all data tables match expected values

**What It Displays:** A deployment validation report confirming every object across all phases is correctly created and operational.

---

## Phase 11: Medallion Architecture Pipeline

**Script:** `sql/11_medallion_architecture.sql`
**Purpose:** Build the complete 4-layer data pipeline (Bronze → Silver → Gold → Platinum).

### Bronze Layer — `RAW_DB.RAW_SCHEMA`

| Table | Rows | Source | Columns |
|-------|------|--------|---------|
| `ICD10_REFERENCE` | 30 | Static | DIAGNOSIS_CATEGORY, ICD_CODE, ICD_DESCRIPTION |
| `PATIENT_RAW` | 2,000 | Synthetic + Marketplace seed | PATIENT_ID, NAME, AGE, GENDER, ADMISSION_DATE, DIAGNOSIS, ICD_CODE, ICD_DESCRIPTION |
| `ICU_EVENTS` | 4,000 | Synthetic | EVENT_ID, PATIENT_ID, EVENT_TYPE, EVENT_TIMESTAMP, HEART_RATE, OXYGEN_LEVEL |
| `BILLING_DATA` | 3,000 | Synthetic | BILL_ID, PATIENT_ID, AMOUNT, BILL_DATE, STATUS (PAID/PENDING/OVERDUE) |
| `HOSPITALS` | 100 | Marketplace (Definitive Healthcare) | FACILITY_NPI, HOSPITAL_NAME, CITY, STATE, ZIP, PHONE, DEFINITIVE_ID, NUMBER_BEDS, NET_PATIENT_REVENUE, TOTAL_PATIENT_REVENUE, NET_INCOME, FINANCIAL_YEAR |

### Silver Layer — `TRANSFORM_DB.TRANSFORM_SCHEMA`

| Table | Rows | Source | Transformations Applied |
|-------|------|--------|------------------------|
| `CLEAN_PATIENT` | 2,000 | PATIENT_RAW | + `PATIENT_ID_MASKED` (PII protection via SHA2 hashing), + `ETL_TIMESTAMP` (lineage tracking) |
| `CLEAN_ICU_EVENTS` | 4,000 | ICU_EVENTS | + `IS_CRITICAL` (boolean: HR<60 or HR>100 or O2<92), + `ETL_TIMESTAMP` |

### Gold Layer — `ANALYTICS_DB.ANALYTICS_SCHEMA`

| Table | Rows | Source Tables | Aggregations |
|-------|------|--------------|-------------|
| `PATIENT_ANALYTICS` | 2,000 | CLEAN_PATIENT + CLEAN_ICU_EVENTS | One row per patient: ICU_EVENT_COUNT, CRITICAL_EVENT_COUNT, AVG_HEART_RATE, AVG_OXYGEN_LEVEL + demographics + ICD codes |
| `BILLING_ANALYTICS` | 1,551 | BILLING_DATA | One row per patient: TOTAL_BILLS, TOTAL_AMOUNT, AVG_BILL_AMOUNT, PAID_AMOUNT, PENDING_AMOUNT, OVERDUE_AMOUNT |

### Platinum Layer — `AI_READY_DB`

| Table | Schema | Rows | Purpose |
|-------|--------|------|---------|
| `ICU_FEATURE_STORE` | FEATURE_STORE | 1,739 | 20-column ML feature set: statistical vitals (MIN/MAX/AVG/STDDEV for HR and O2), ICU_STAY_DAYS, CRITICAL_EVENT_RATE, RISK_SCORE (HIGH/MEDIUM/LOW) |
| `PATIENT_NOTES` | FEATURE_STORE | 2,000 | Synthetic clinical notes: NOTE_ID, PATIENT_ID, NOTE_TEXT, NOTE_DATE, NOTE_TYPE (5 templates, 4 note types) — NLP-ready |
| `PATIENT_EMBEDDINGS` | FEATURE_STORE | 2,000 | 8-dimensional float vectors (EMBEDDING_VECTOR as ARRAY) for patient similarity search, with CREATED_AT timestamp |
| `V_PATIENT_SEMANTIC` | SEMANTIC_MODELS | view | Cortex Analyst semantic view with human-readable column names: Patient ID, Age, Gender, Diagnosis, ICD Code, ICD Description, Total ICU Events, Critical Events, Average Heart Rate, Average Oxygen Level, Risk Score, Total Billing Amount, Age Risk Flag, Low Oxygen Risk Flag, High Heart Rate Risk Flag |

**What It Displays:** The complete data transformation journey — raw ingestion → cleansing/masking → aggregation → ML-ready features, with full lineage via ETL timestamps.

---

## Phase 12: Healthcare Industry Data

**Script:** `sql/12_healthcare_industry.sql`
**Purpose:** Standalone re-run of additional Bronze-layer healthcare tables.

| Table | Rows | Source | Columns |
|-------|------|--------|---------|
| `MEDICATION_RECORDS` | 3,000 | Synthetic | PATIENT_ID, MEDICATION_NAME (10 drugs), DOSAGE, FREQUENCY |
| `DEVICE_ALERTS` | 1,500 | Synthetic | PATIENT_ID, DEVICE_TYPE (5 types), ALERT_SEVERITY (3 levels), ALERT_TIMESTAMP |
| `HCO_LOCATIONS` | 100 | Marketplace | DEFINITIVE_ID, REGION, geographic coordinates |
| `PROVIDER_AFFILIATIONS` | 100 | Marketplace | HCO_NPI, affiliation scores |
| `PRESCRIPTION_DATA` | 100 | Marketplace (RxDecision) | CLAIM_YEAR, MEDICATION_NAME, RX_EVENT_TYPE (8 types), RX_EVENT_CLAIMS, RX_TOTAL_CLAIMS, RX_EVENT_SCORE_DECILE |

**What It Displays:** Extended healthcare domain data — medication records for pharmacy analytics, IoT device alerts for remote patient monitoring, and Marketplace-sourced prescription therapy decisions.

---

## Phase 13: AI-Ready Layer

**Script:** `sql/13_ai_ready_layer.sql`
**Purpose:** Standalone re-run of the Platinum layer (Feature Store + Semantic Model).

**Objects rebuilt:**
- `ICU_FEATURE_STORE` — Statistical features with risk scoring logic:
  - **HIGH risk:** Critical event rate > 50% OR avg O2 < 90 OR avg HR > 110
  - **MEDIUM risk:** Critical event rate > 25% OR avg O2 < 94 OR avg HR > 100
  - **LOW risk:** All other patients
- `PATIENT_NOTES` — NLP-ready clinical notes
- `PATIENT_EMBEDDINGS` — Vector embeddings for similarity search
- `V_PATIENT_SEMANTIC` — Business-user semantic view with 3 derived risk flags:
  - `Age Risk Flag` — 1 if age ≥ 65
  - `Low Oxygen Risk Flag` — 1 if avg O2 < 92
  - `High Heart Rate Risk Flag` — 1 if avg HR > 100

**What It Displays:** Production-ready ML features — a data scientist can directly consume the feature store for model training, use embeddings for patient clustering, and leverage the semantic view for natural-language analytics via Cortex Analyst.

---

## Streamlit Dashboard — 7 Tabs

**App File:** `streamlit/healthcare_dashboard.py`
**Title:** Healthcare RxDecision Data Hub

---

### Tab 1: Overview

**Purpose:** Executive summary of the entire platform at a glance.

**KPI Metrics (4 cards):**

| Metric | Source Table | Description |
|--------|------------|-------------|
| Total Patients | `RAW_DB.RAW_SCHEMA.PATIENT_RAW` | Count of all patients in the system |
| ICU Events | `RAW_DB.RAW_SCHEMA.ICU_EVENTS` | Total ICU telemetry events recorded |
| Billing Records | `RAW_DB.RAW_SCHEMA.BILLING_DATA` | Total billing transactions |
| Rx Decisions (Marketplace) | `RAW_DB.RAW_SCHEMA.PRESCRIPTION_DATA` | Marketplace prescription therapy records |

**Charts:**
- **Admissions by Diagnosis** — Horizontal bar chart showing patient count per diagnosis category (e.g., Diabetes, Hypertension, Asthma, etc.), sorted descending. Color: `#4A90D9`.
- **Admission Trend (Last 12 Months)** — Area chart showing monthly admission volume over time. Reveals seasonality and growth patterns. Color: `#4A90D9`, opacity 0.6.

---

### Tab 2: Patient Explorer

**Purpose:** Interactive patient lookup with filtering and demographic analysis.

**Filters (2 columns):**
- **Diagnosis** — Dropdown: All + distinct diagnosis values from PATIENT_RAW
- **Gender** — Dropdown: All, M, F

**Display:**
- **Patient Data Table** — Scrollable table (up to 500 rows) showing: PATIENT_ID, NAME, AGE, GENDER, DIAGNOSIS, ADMISSION_DATE. Sorted by most recent admission. Dynamically filtered by selected diagnosis and gender.

**Chart:**
- **Age Distribution** — Bar chart with 4 age groups: 18-29, 30-49, 50-69, 70+. Responds to active filters. Color: `#6C5CE7`.

---

### Tab 3: ICU Vitals Monitor

**Purpose:** Real-time ICU telemetry analytics with critical event tracking.

**KPI Metrics (4 cards):**

| Metric | Value | Source |
|--------|-------|--------|
| Avg Heart Rate | bpm | `TRANSFORM_DB.TRANSFORM_SCHEMA.CLEAN_ICU_EVENTS` |
| Avg O2 Level | % | CLEAN_ICU_EVENTS |
| Critical Events | count | CLEAN_ICU_EVENTS (WHERE IS_CRITICAL = TRUE) |
| Total Events | count | CLEAN_ICU_EVENTS |

**Charts:**
- **Event Type Breakdown** — Horizontal bar chart showing count per EVENT_TYPE with critical count overlay. Color: `#00B894`.
- **ICU Events Over Time** — Dual-line chart:
  - Green solid line (`#00B894`): Total daily events
  - Red dashed line (`#E74C3C`): Critical events per day
  - Allows visual identification of critical event spikes.

---

### Tab 4: Billing Analytics

**Purpose:** Financial overview of patient billing with revenue tracking.

**KPI Metrics (3 cards):**

| Metric | Source |
|--------|--------|
| Total Revenue | `ANALYTICS_DB.ANALYTICS_SCHEMA.BILLING_ANALYTICS` — SUM(TOTAL_AMOUNT) |
| Paid | SUM(PAID_AMOUNT) |
| Overdue | SUM(OVERDUE_AMOUNT) |

**Charts:**
- **Billing Status Distribution** — Donut chart (arc with inner radius) showing count by STATUS:
  - PAID: `#00B894` (green)
  - PENDING: `#FDCB6E` (yellow)
  - OVERDUE: `#E74C3C` (red)
  - Tooltip shows STATUS, count, and total amount.
- **Monthly Revenue Trend** — Bar chart of monthly revenue from `RAW_DB.RAW_SCHEMA.BILLING_DATA`, aggregated by BILL_DATE month. Color: `#FDCB6E`.

---

### Tab 5: Rx Decisions (Marketplace Data)

**Purpose:** Explore prescription therapy decision data from Snowflake Marketplace (Definitive Healthcare - RxDecision Insights).

**Display:**
- **Rx Decisions Table** — Full data view: CLAIM_YEAR, MEDICATION_NAME, RX_EVENT_TYPE, RX_EVENT_CLAIMS, RX_TOTAL_CLAIMS, RX_EVENT_SCORE_DECILE. Sorted by year (desc) then total claims (desc).

**Charts:**
- **Rx Event Type Distribution** — Horizontal bar chart counting records per RX_EVENT_TYPE (8 types: New Start, Restart, Switch To, Switch From, Add-on, Discontinuation, etc.). Color: `#A29BFE`.
- **Top Medications by Prescription Volume** — Horizontal bar chart from `RAW_DB.RAW_SCHEMA.MEDICATION_RECORDS` showing prescription count per MEDICATION_NAME, sorted descending. Color: `#FD79A8`.

---

### Tab 6: Hospitals & Providers (Marketplace Data)

**Purpose:** Explore hospital and healthcare organization data sourced from Snowflake Marketplace.

**KPI Metrics (3 cards):**

| Metric | Source |
|--------|--------|
| Hospitals | `RAW_DB.RAW_SCHEMA.HOSPITALS` — COUNT(*) |
| HCO Locations | `RAW_DB.RAW_SCHEMA.HCO_LOCATIONS` — COUNT(*) |
| Provider Affiliations | `RAW_DB.RAW_SCHEMA.PROVIDER_AFFILIATIONS` — COUNT(*) |

**Display:**
- **Hospital Details Table** — Columns: HOSPITAL_NAME, CITY, STATE, NUMBER_BEDS, NET_PATIENT_REVENUE, NET_INCOME, FINANCIAL_YEAR. Sorted by NET_PATIENT_REVENUE descending.

**Chart:**
- **HCO Locations by Region** — Horizontal bar chart showing location count per REGION. Color: `#00CEC9`. Only renders if data is non-empty.

---

### Tab 7: AI Risk Scoring (Feature Store)

**Purpose:** ML-derived patient risk classification from the Platinum layer.

**KPI Metrics (3 cards):**

| Metric | Source | Description |
|--------|--------|-------------|
| High Risk | `AI_READY_DB.FEATURE_STORE.ICU_FEATURE_STORE` | Patients with RISK_SCORE = 'HIGH' |
| Medium Risk | ICU_FEATURE_STORE | Patients with RISK_SCORE = 'MEDIUM' |
| Low Risk | ICU_FEATURE_STORE | Patients with RISK_SCORE = 'LOW' |

**Risk Scoring Logic:**
- **HIGH:** Critical event rate > 50% OR avg O2 < 90 OR avg HR > 110
- **MEDIUM:** Critical event rate > 25% OR avg O2 < 94 OR avg HR > 100
- **LOW:** All remaining patients

**Charts:**
- **Risk Distribution by Diagnosis** — Stacked bar chart: X-axis = Diagnosis, Y-axis = patient count, Color = RISK_SCORE:
  - HIGH: `#E74C3C` (red)
  - MEDIUM: `#FDCB6E` (yellow)
  - LOW: `#00B894` (green)
- **Top High-Risk Patients** — Data table (top 20) showing: PATIENT_ID, AGE, GENDER, DIAGNOSIS, AVG_HR, AVG_O2, CRITICAL_EVENTS, TOTAL_EVENTS, CRITICAL_EVENT_RATE, RISK_SCORE. Sorted by CRITICAL_EVENT_RATE descending.

---

## Data Flow Summary

```
SNOWFLAKE MARKETPLACE                          SYNTHETIC DATA GENERATORS
(Definitive Healthcare)                        (GENERATOR + RANDOM)
        |                                               |
        v                                               v
+======================== PHASE 11-12: BRONZE ========================+
|                        RAW_DB.RAW_SCHEMA                            |
|  HOSPITALS (100)              PATIENT_RAW (2,000)                   |
|  HCO_LOCATIONS (100)          ICU_EVENTS (4,000)                    |
|  PROVIDER_AFFILIATIONS (100)  BILLING_DATA (3,000)                  |
|  PRESCRIPTION_DATA (100)      MEDICATION_RECORDS (3,000)            |
|  ICD10_REFERENCE (30)         DEVICE_ALERTS (1,500)                 |
+================================|====================================+
                                 |
              + PII masking      |     + IS_CRITICAL flag
              + ETL_TIMESTAMP    |     + ETL_TIMESTAMP
                                 v
+======================== PHASE 11: SILVER ===========================+
|                  TRANSFORM_DB.TRANSFORM_SCHEMA                      |
|  CLEAN_PATIENT (2,000)            CLEAN_ICU_EVENTS (4,000)         |
+================================|====================================+
                                 |
              + ICU aggregates   |     + Billing pivot by status
              + Billing totals   |
                                 v
+========================= PHASE 11: GOLD ===========================+
|                  ANALYTICS_DB.ANALYTICS_SCHEMA                      |
|  PATIENT_ANALYTICS (2,000)        BILLING_ANALYTICS (1,551)        |
+================================|====================================+
                                 |
              + Statistical features    + Risk scoring (HIGH/MED/LOW)
              + Embeddings (8-dim)      + Clinical notes (NLP-ready)
                                 v
+====================== PHASE 13: PLATINUM ==========================+
|                          AI_READY_DB                                |
|  FEATURE_STORE:                    SEMANTIC_MODELS:                 |
|    ICU_FEATURE_STORE (1,739)         V_PATIENT_SEMANTIC (view)     |
|    PATIENT_NOTES (2,000)                                           |
|    PATIENT_EMBEDDINGS (2,000)                                      |
+=========================|===========================================+
                          |
         +----------------+----------------+
         v                                 v
+--- PHASE 9: AUDIT ---+    +--- PHASE 6-7: MONITORING ---+
|     AUDIT_DB          |    |     MONITORING_DB            |
| 8 snapshot tables     |    | 11 views + 3 alerts         |
| 6 compliance views    |    | ALERT_LOG table             |
+-----------------------+    +-----------------------------+
```

---

## Total Objects Created

| Category | Count |
|----------|-------|
| Databases | 10 |
| Custom Roles | 6 |
| Warehouses | 5 |
| Resource Monitors | 5 |
| Monitoring Views | 11 |
| Alerts | 3 |
| Governance Tags | 3 |
| Masking/Access Policies | 4 |
| Audit Compliance Views | 6 |
| Audit Snapshot Tables | 8 |
| Data Tables (Medallion) | 17 |
| Semantic Views | 1 |
| Streamlit Dashboard Tabs | 7 |
| SQL Deployment Scripts | 14 |
