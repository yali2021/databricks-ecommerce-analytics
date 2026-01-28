-- =====================================================
-- Gold Layer: Business KPI Tables
-- Purpose:
--   Build daily and categorical business KPIs derived
--   from Silver-layer fact and dimension tables.
--
-- KPIs included:
--   - Daily sales (orders & GMV)
--   - Daily average order value (AOV)
--   - Top product categories by GMV
--
-- Notes:
--   - Only completed orders (delivered / shipped) are included
--   - All aggregations are optimized for reporting and BI use
-- =====================================================


-- -----------------------------------------------------
-- 1) Daily Sales KPI (Orders & GMV)
-- -----------------------------------------------------
CREATE OR REPLACE TABLE workspace.ecommerce.gold_daily_sales_kpi AS
SELECT
  DATE(o.order_purchase_ts)      AS order_date,
  COUNT(DISTINCT o.order_id)     AS order_count,
  SUM(oi.item_total_value)       AS gmv
FROM workspace.ecommerce.silver_orders_clean o
JOIN workspace.ecommerce.silver_order_items_enriched oi
  ON o.order_id = oi.order_id
WHERE o.order_status IN ('delivered', 'shipped')
GROUP BY DATE(o.order_purchase_ts)
ORDER BY order_date;


-- -----------------------------------------------------
-- 2) Daily Average Order Value (AOV)
-- -----------------------------------------------------
CREATE OR REPLACE TABLE workspace.ecommerce.gold_daily_aov AS
WITH order_level_value AS (
  SELECT
    o.order_id,
    DATE(o.order_purchase_ts) AS order_date,
    SUM(oi.item_total_value)  AS order_total_value
  FROM workspace.ecommerce.silver_orders_clean o
  JOIN workspace.ecommerce.silver_order_items_enriched oi
    ON o.order_id = oi.order_id
  WHERE o.order_status IN ('delivered', 'shipped')
  GROUP BY o.order_id, DATE(o.order_purchase_ts)
)
SELECT
  order_date,
  AVG(order_total_value) AS aov
FROM order_level_value
GROUP BY order_date
ORDER BY order_date;


-- -----------------------------------------------------
-- 3) Top Product Categories by GMV
-- -----------------------------------------------------
CREATE OR REPLACE TABLE workspace.ecommerce.gold_top_categories_by_gmv AS
SELECT
  p.product_category_name,
  SUM(oi.item_total_value) AS category_gmv
FROM workspace.ecommerce.silver_order_items_enriched oi
JOIN workspace.ecommerce.silver_orders_clean o
  ON oi.order_id = o.order_id
JOIN workspace.ecommerce.silver_products_dim p
  ON oi.product_id = p.product_id
WHERE o.order_status IN ('delivered', 'shipped')
GROUP BY p.product_category_name
ORDER BY category_gmv DESC;
