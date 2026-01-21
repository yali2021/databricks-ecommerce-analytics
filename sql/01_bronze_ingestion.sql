-- =====================================================
-- Bronze Layer: Environment Setup & Raw Data Namespace
-- Description:
--   - Create schema for raw and downstream tables
--   - Raw CSV ingestion is handled via Databricks UI
--     due to Community Edition limitations
-- =====================================================

CREATE SCHEMA IF NOT EXISTS workspace.ecommerce;
