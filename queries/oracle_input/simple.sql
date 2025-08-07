-- Simple Oracle SQL Query for Lakebridge Complexity Scoring Demo
-- This query is designed to meet LOW classification criteria:
-- - No loops
-- - Conventional Statement count <= 10
-- - Uses basic Oracle native functions

SELECT
    c.c_custkey,
    c.c_name,
    c.c_nationkey,
    c.c_acctbal,
    NVL(COUNT(o.o_orderkey), 0) AS total_orders,
    NVL(SUM(o.o_totalprice), 0) AS total_spent,
    NVL(AVG(o.o_totalprice), 0) AS avg_order_value,
    CASE 
        WHEN COUNT(o.o_orderkey) > 5 THEN 'Frequent'
        ELSE 'Occasional'
    END AS customer_status,
    TO_NUMBER(c.c_acctbal) AS validated_balance,
    CASE 
        WHEN MIN(o.o_orderkey) IS NULL THEN 1
        ELSE 0
    END AS has_orders
FROM
    customer c
LEFT JOIN
    orders o ON c.c_custkey = o.o_custkey
GROUP BY
    c.c_custkey,
    c.c_name,
    c.c_nationkey,
    c.c_acctbal
ORDER BY
    total_spent DESC
FETCH FIRST 100 ROWS ONLY; 