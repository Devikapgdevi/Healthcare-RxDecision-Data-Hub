# Test Cases - Healthcare RxDecision Analytics Platform

## Data Validation Tests

### Bronze Layer
| Test ID | Test Name | Query | Expected |
|---------|-----------|-------|----------|
| TC001 | Patient Count | `SELECT COUNT(*) FROM RAW_DB.RAW_SCHEMA.PATIENT_RAW` | = 2,000 |
| TC002 | ICU Events | `SELECT COUNT(*) FROM RAW_DB.RAW_SCHEMA.ICU_EVENTS` | = 4,000 |
| TC003 | Billing | `SELECT COUNT(*) FROM RAW_DB.RAW_SCHEMA.BILLING_DATA` | = 3,000 |
| TC004 | Hospitals | `SELECT COUNT(*) FROM RAW_DB.RAW_SCHEMA.HOSPITALS` | = 100 |
| TC005 | HCO Locations | `SELECT COUNT(*) FROM RAW_DB.RAW_SCHEMA.HCO_LOCATIONS` | = 100 |
| TC006 | Rx Data | `SELECT COUNT(*) FROM RAW_DB.RAW_SCHEMA.PRESCRIPTION_DATA` | = 100 |
| TC007 | Affiliations | `SELECT COUNT(*) FROM RAW_DB.RAW_SCHEMA.PROVIDER_AFFILIATIONS` | = 100 |
| TC008 | Device Alerts | `SELECT COUNT(*) FROM RAW_DB.RAW_SCHEMA.DEVICE_ALERTS` | = 1,500 |
| TC009 | Medications | `SELECT COUNT(*) FROM RAW_DB.RAW_SCHEMA.MEDICATION_RECORDS` | = 3,000 |

### Silver Layer
| Test ID | Test Name | Query | Expected |
|---------|-----------|-------|----------|
| TC010 | Clean Patient | `SELECT COUNT(*) FROM TRANSFORM_DB.TRANSFORM_SCHEMA.CLEAN_PATIENT` | = 2,000 |
| TC011 | Clean ICU | `SELECT COUNT(*) FROM TRANSFORM_DB.TRANSFORM_SCHEMA.CLEAN_ICU_EVENTS` | = 4,000 |

### Gold Layer
| Test ID | Test Name | Query | Expected |
|---------|-----------|-------|----------|
| TC012 | Patient Analytics | `SELECT COUNT(*) FROM ANALYTICS_DB.ANALYTICS_SCHEMA.PATIENT_ANALYTICS` | = 2,000 |
| TC013 | Billing Analytics | `SELECT COUNT(*) FROM ANALYTICS_DB.ANALYTICS_SCHEMA.BILLING_ANALYTICS` | > 0 |

### Platinum Layer
| Test ID | Test Name | Query | Expected |
|---------|-----------|-------|----------|
| TC014 | Feature Store | `SELECT COUNT(*) FROM AI_READY_DB.FEATURE_STORE.ICU_FEATURE_STORE` | > 0 |
| TC015 | Patient Notes | `SELECT COUNT(*) FROM AI_READY_DB.FEATURE_STORE.PATIENT_NOTES` | = 2,000 |
| TC016 | Embeddings | `SELECT COUNT(*) FROM AI_READY_DB.FEATURE_STORE.PATIENT_EMBEDDINGS` | = 2,000 |

### Infrastructure
| Test ID | Test Name | Expected |
|---------|-----------|----------|
| TC017 | HC Roles | 6 |
| TC018 | Warehouses | 4 |
| TC019 | Monitoring Views | >= 6 |
| TC020 | Masking Policies | >= 3 |

## Run Tests
```sql
-- Execute sql/10_verification.sql
```
