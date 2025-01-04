SELECT 
    TOP 100 *
FROM 
    OPENROWSET(
        BULK 'https://ecommdatastorageacc.dfs.core.windows.net/ecommdata/silver/',
        FORMAT = 'PARQUET'
    ) AS result;

CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'master#enc@1';

CREATE DATABASE SCOPED CREDENTIAL ecommadmin WITH IDENTITY = 'Managed Identity';

CREATE EXTERNAL FILE FORMAT extfileformat WITH (
    FORMAT_TYPE = PARQUET,
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
);

CREATE EXTERNAL DATA SOURCE goldlayer
WITH (
    LOCATION = 'https://ecommdatastorageacc.dfs.core.windows.net/ecommdata/gold/',
    CREDENTIAL = ecommadmin
);

CREATE EXTERNAL TABLE gold.finaltable
WITH (
    LOCATION = 'finalServing/',
    DATA_SOURCE = goldlayer,
    FILE_FORMAT = extfileformat
) AS
SELECT * FROM gold.consolidated_data;

SELECT * FROM gold.finaltable;    

SELECT * FROM gold.revenue;

SELECT * FROM gold.top_performing_categories
ORDER BY Total_Revenue DESC;

SELECT * FROM gold.sale_by_region;

SELECT * FROM gold.order_status;

SELECT * FROM gold.shipping_time;

SELECT * FROM gold.profit_margin;

SELECT * from gold.seasonal_sales_trends
ORDER BY Month;