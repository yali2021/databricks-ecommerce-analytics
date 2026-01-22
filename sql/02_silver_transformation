-- =====================================================
-- Silver Layer: Data Cleaning & Data Modeling
-- Description:
--   - Transform Bronze tables into analysis-ready Silver tables
--   - Apply schema standardization (types, timestamps, numeric fields)
--   - Create a star-schema-friendly layout:
--       * Fact tables: orders, order_items
--       * Dimension tables: customers, products
--   - Add curated/enriched columns used downstream in Gold KPIs
--
-- Key Outputs (example names):
--   - silver_orders_clean
--   - silver_order_items_enriched
--   - silver_customers_dim
--   - silver_products_dim
--
-- Notes:
--   - This layer focuses on correct grain, consistent data types,
--     and reusable tables for analytics.
-- =====================================================

-- (Optional) sanity check: show current tables
-- SHOW TABLES IN workspace.ecommerce;

-- -----------------------------------------------------
-- 1) Clean Orders Fact
--    - Standardize timestamps
--    - Filter/retain relevant order statuses if needed
-- -----------------------------------------------------
CREATE OR REPLACE TABLE workspace.ecommerce.silver_orders_clean AS
SELECT
  order_id,
  customer_id,
  order_status,
  CAST(order_purchase_timestamp AS TIMESTAMP) AS order_purchase_ts,
  CAST(order_approved_at AS TIMESTAMP)        AS order_approved_ts,
  CAST(order_delivered_carrier_date AS TIMESTAMP) AS order_delivered_carrier_ts,
  CAST(order_delivered_customer_date AS TIMESTAMP) AS order_delivered_customer_ts,
  CAST(order_estimated_delivery_date AS TIMESTAMP) AS order_estimated_delivery_ts
FROM workspace.ecommerce.bronze_orders
WHERE order_id IS NOT NULL;

-- -----------------------------------------------------
-- 2) Enrich Order Items Fact
--    - Cast price/freight to numeric
--    - Create item_total_value (price + freight)
-- -----------------------------------------------------
CREATE OR REPLACE TABLE workspace.ecommerce.silver_order_items_enriched AS
SELECT
  oi.order_id,
  oi.order_item_id,
  oi.product_id,
  CAST(oi.shipping_limit_date AS TIMESTAMP) AS shipping_limit_ts,
  CAST(oi.price AS DOUBLE)    AS item_price,
  CAST(oi.freight_value AS DOUBLE) AS freight_value,
  CAST(oi.price AS DOUBLE) + CAST(oi.freight_value AS DOUBLE) AS item_total_value,

  p.product_category_name,
  p.product_weight_g,
  p.product_length_cm,
  p.product_height_cm,
  p.product_width_cm
FROM workspace.ecommerce.bronze_order_items oi
LEFT JOIN workspace.ecommerce.bronze_products p
  ON oi.product_id = p.product_id
WHERE oi.order_id IS NOT NULL;

-- -----------------------------------------------------
-- 3) Customer Dimension
--    - Keep customer_id + customer_unique_id
--    - Use customer_unique_id for user-level analytics (retention)
-- -----------------------------------------------------
CREATE OR REPLACE TABLE workspace.ecommerce.silver_customers_dim AS
SELECT
  customer_id,
  customer_unique_id,
  customer_zip_code_prefix,
  LOWER(TRIM(customer_city))  AS customer_city,
  UPPER(TRIM(customer_state)) AS customer_state
FROM workspace.ecommerce.bronze_customers
WHERE customer_id IS NOT NULL;
-- -----------------------------------------------------
-- 4) Product Dimension
--    - One row per product
--    - Clean and standardize product attributes
--    - Used for category-level and product-level analysis
-- -----------------------------------------------------
CREATE OR REPLACE TABLE workspace.ecommerce.silver_products_dim AS
SELECT
  product_id,
  LOWER(TRIM(product_category_name)) AS product_category_name,
  product_weight_g,
  product_length_cm,
  product_height_cm,
  product_width_cm
FROM workspace.ecommerce.bronze_products
WHERE product_id IS NOT NULL;
