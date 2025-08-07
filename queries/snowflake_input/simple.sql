SELECT
    c.c_custkey,
    c.c_name,
    c.c_nationkey,
    c.c_acctbal,
    COALESCE(COUNT(o.o_orderkey), 0) AS total_orders,
    COALESCE(SUM(o.o_totalprice), 0) AS total_spent,
    COALESCE(AVG(o.o_totalprice), 0) AS avg_order_value,
    IFF(COUNT(o.o_orderkey) > 5, 'Frequent', 'Occasional') AS customer_status,
    TRY_CAST(c.c_acctbal AS DECIMAL(15,2)) AS validated_balance,
    IFF(MIN(o.o_orderkey) IS NULL, TRUE, FALSE) AS has_orders
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
LIMIT 100;
