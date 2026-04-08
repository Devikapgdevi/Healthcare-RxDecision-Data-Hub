-- =============================================
-- MASTER_DEPLOYMENT.SQL
-- Healthcare RxDecision Analytics Platform
-- Data Source: Snowflake Marketplace (Definitive Healthcare)
-- Architecture: Bronze -> Silver -> Gold -> Platinum (ICD-10)
-- =============================================

-- PREREQUISITE: Install the free marketplace dataset first:
--   Snowflake Marketplace > "RxDecision Insights Prescription Therapy Decisions (Sample)"
--   by Definitive Healthcare
--   URL: https://app.snowflake.com/marketplace/listing/GZT1Z13RCC1

-- Run scripts in sequence:
-- 1.  01_account_administration.sql  - Network, Password, Session policies
-- 2.  02_rbac_setup.sql              - 6-role hierarchy
-- 3.  03_warehouse_management.sql    - 4 XSMALL warehouses (cost-optimized)
-- 4.  04_database_structure.sql      - 10 databases (Medallion + Support)
-- 5.  05_resource_monitors.sql       - 5 cost monitors
-- 6.  06_monitoring_views.sql        - 8 monitoring views
-- 7.  07_alerts.sql                  - 3 automated alerts
-- 8.  08_data_governance.sql         - Tags, masking, row access policies
-- 9.  09_audit_layer.sql             - 6 audit/compliance views
-- 10. 10_verification.sql            - Deployment validation
-- 11. 11_medallion_architecture.sql  - All 4 layers: Bronze/Silver/Gold/Platinum (ICD-10)
-- 12. 12_healthcare_industry.sql     - Device alerts, medication records (standalone re-run)
-- 13. 13_ai_ready_layer.sql          - Platinum layer (standalone re-run)

-- NOTE: Scripts 12 and 13 are standalone re-run scripts for their respective layers.
-- Script 11 is the master data pipeline that builds all 4 medallion layers end-to-end.
