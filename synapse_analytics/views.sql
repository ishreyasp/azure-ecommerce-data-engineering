CREATE SCHEMA gold;

CREATE VIEW gold.consolidated_data
AS
SELECT
    *
FROM
    OPENROWSET(
        BULK 'https://ecommdatastorageacc.dfs.core.windows.net/ecommdata/silver/',
        FORMAT = 'PARQUET'
    ) AS result;   

CREATE VIEW gold.delivered_orders
AS
SELECT
    *
FROM
    OPENROWSET(
        BULK 'https://ecommdatastorageacc.dfs.core.windows.net/ecommdata/silver/',
        FORMAT = 'PARQUET'
    ) AS result1
    WHERE order_status = 'delivered';       

CREATE VIEW gold.revenue
AS
SELECT 
    FORMAT(order_purchase_timestamp, 'yyyy-MM') AS Month, 
    SUM(CONVERT(decimal(10, 2), payment_value)) AS Total_Revenue
FROM gold.finaltable
GROUP BY FORMAT(order_purchase_timestamp, 'yyyy-MM');

CREATE VIEW gold.top_performing_categories
AS
SELECT 
    product_category_name, 
    product_category_name_english,
    SUM(CAST(payment_value AS decimal(18, 2))) AS Total_Revenue
FROM gold.finaltable
GROUP BY product_category_name, product_category_name_english;

CREATE VIEW gold.sale_by_region
AS
SELECT 
    customer_state, 
    SUM(CAST(payment_value AS decimal(18, 2))) AS Total_Revenue
FROM gold.finaltable
WHERE order_status = 'delivered'
GROUP BY customer_state;

CREATE VIEW gold.order_status
AS
SELECT order_status, COUNT(*) AS Order_Count
FROM gold.finaltable
GROUP BY order_status;

CREATE VIEW gold.shipping_time
AS
SELECT 
    AVG(DATEDIFF(day, order_purchase_timestamp, order_delivered_customer_date)) AS Average_Shipping_Time
FROM gold.finaltable
WHERE order_status = 'delivered';

CREATE VIEW gold.profit_margin
AS
SELECT product_category_name, 
       product_category_name_english, 
       SUM(CAST(payment_value AS decimal(18, 2))) AS Revenue, 
       SUM(CAST(price AS decimal(18, 2))) AS Total_Cost,
       (SUM(CAST(payment_value AS decimal(18, 2))) - SUM(CAST(price AS decimal(18, 2)))) / SUM(CAST(payment_value AS decimal(18, 2))) * 100 AS Profit_Margin
FROM gold.finaltable
GROUP BY product_category_name, product_category_name_english;

CREATE VIEW gold.seasonal_sales_trends
AS
SELECT FORMAT(order_purchase_timestamp, 'MM-yyyy') AS Month, SUM(CAST(payment_value AS decimal(18, 2))) AS Revenue
FROM gold.finaltable
GROUP BY FORMAT(order_purchase_timestamp, 'MM-yyyy');