-- Sample DDL file for testing DDL Splitter
-- This file contains various Snowflake object types

CREATE OR REPLACE DATABASE test_database;

CREATE OR REPLACE SCHEMA test_schema;

CREATE OR REPLACE WAREHOUSE test_warehouse
  WAREHOUSE_SIZE = 'SMALL'
  AUTO_SUSPEND = 300
  AUTO_RESUME = TRUE;

CREATE OR REPLACE TABLE customers (
  customer_id NUMBER(38,0) PRIMARY KEY,
  first_name VARCHAR(50),
  last_name VARCHAR(50),
  email VARCHAR(100) UNIQUE,
  created_date TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE orders (
  order_id NUMBER(38,0) PRIMARY KEY,
  customer_id NUMBER(38,0) REFERENCES customers(customer_id),
  order_date TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  total_amount DECIMAL(10,2),
  status VARCHAR(20) DEFAULT 'PENDING'
);

CREATE OR REPLACE VIEW customer_orders AS
SELECT 
  c.customer_id,
  c.first_name,
  c.last_name,
  COUNT(o.order_id) as total_orders,
  SUM(o.total_amount) as total_spent
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.first_name, c.last_name;

CREATE OR REPLACE MATERIALIZED VIEW customer_summary AS
SELECT 
  customer_id,
  COUNT(*) as order_count,
  AVG(total_amount) as avg_order_value
FROM orders
GROUP BY customer_id;

CREATE OR REPLACE FUNCTION calculate_tax(amount NUMBER, tax_rate NUMBER)
RETURNS NUMBER
AS
$$
  amount * tax_rate
$$;

CREATE OR REPLACE PROCEDURE process_orders()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
  var result = "Orders processed successfully";
  return result;
$$;

CREATE OR REPLACE SEQUENCE order_id_seq
  START = 1
  INCREMENT = 1;

CREATE OR REPLACE STREAM customer_changes
ON TABLE customers;

CREATE OR REPLACE TASK process_customer_updates
  WAREHOUSE = test_warehouse
  SCHEDULE = '1 minute'
AS
  INSERT INTO customer_audit SELECT * FROM customer_changes;

CREATE OR REPLACE PIPE customer_pipe
  AUTO_INGEST = TRUE
AS
  COPY INTO customers FROM @customer_stage;

CREATE OR REPLACE FILE FORMAT csv_format
  TYPE = 'CSV'
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1;

CREATE OR REPLACE MASKING POLICY email_mask AS (val STRING) RETURNS STRING ->
  CASE 
    WHEN CURRENT_ROLE() IN ('ADMIN', 'HR') THEN val
    ELSE CONCAT(LEFT(val, 3), '***@***.com')
  END;

CREATE OR REPLACE ROW ACCESS POLICY customer_access AS (customer_id NUMBER) RETURNS BOOLEAN ->
  CURRENT_ROLE() IN ('ADMIN', 'SALES') OR 
  customer_id IN (SELECT customer_id FROM user_customers WHERE user_id = CURRENT_USER());

CREATE OR REPLACE TAG cost_center
  ALLOWED_VALUES = ('IT', 'SALES', 'MARKETING', 'FINANCE');

CREATE OR REPLACE STORAGE INTEGRATION s3_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE;

CREATE OR REPLACE EXTERNAL TABLE external_customers (
  customer_id NUMBER(38,0),
  name VARCHAR(100),
  email VARCHAR(100)
)
LOCATION = (@s3_stage/customers/)
FILE_FORMAT = csv_format;

CREATE OR REPLACE HYBRID TABLE hybrid_events (
  event_id NUMBER(38,0) PRIMARY KEY,
  event_type VARCHAR(50),
  event_data VARIANT,
  created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE DYNAMIC TABLE sales_summary
  TARGET_LAG = '1 hour'
  WAREHOUSE = test_warehouse
AS
  SELECT 
    DATE_TRUNC('day', order_date) as order_day,
    COUNT(*) as daily_orders,
    SUM(total_amount) as daily_revenue
  FROM orders
  GROUP BY DATE_TRUNC('day', order_date);

CREATE OR REPLACE EVENT TABLE audit_events (
  event_id NUMBER(38,0) AUTOINCREMENT,
  event_type VARCHAR(50),
  user_id VARCHAR(50),
  event_data VARIANT,
  event_time TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE SEMANTIC VIEW customer_analytics AS
SELECT 
  c.customer_id,
  c.first_name,
  c.last_name,
  COUNT(o.order_id) as order_count,
  SUM(o.total_amount) as total_spent,
  AVG(o.total_amount) as avg_order_value
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.first_name, c.last_name;

CREATE OR REPLACE ALERT high_value_orders
  WAREHOUSE = test_warehouse
  SCHEDULE = '5 minutes'
  IF (EXISTS (
    SELECT 1 FROM orders 
    WHERE total_amount > 1000 
    AND order_date > DATEADD(hour, -1, CURRENT_TIMESTAMP())
  ))
  THEN
    CALL send_notification('High value orders detected');

CREATE OR REPLACE DBT PROJECT my_dbt_project
  REPOSITORY = 'https://github.com/company/dbt-project'
  BRANCH = 'main';

CREATE OR REPLACE DATA METRIC FUNCTION customer_lifetime_value(customer_id NUMBER)
RETURNS NUMBER
AS
$$
  SELECT SUM(total_amount) 
  FROM orders 
  WHERE customer_id = $1
$$; 