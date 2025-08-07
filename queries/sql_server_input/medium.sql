-- Statement 1: Customer Orders
WITH customer_orders AS (
    SELECT 
        c.c_custkey,
        c.c_name,
        c.c_nationkey,
        c.c_acctbal,
        COUNT(o.o_orderkey) AS total_orders,
        SUM(o.o_totalprice) AS total_spent,
        AVG(o.o_totalprice) AS avg_order_value,
        STRING_AGG(DISTINCT o.o_orderstatus, ',') AS order_statuses
    FROM customer c
    LEFT JOIN orders o ON c.c_custkey = o.o_custkey
    GROUP BY c.c_custkey, c.c_name, c.c_nationkey, c.c_acctbal
)
SELECT 
    c.c_custkey,
    c.c_name,
    c.c_nationkey,
    c.c_acctbal,
    c.total_orders,
    c.total_spent,
    c.avg_order_value,
    ROW_NUMBER() OVER (ORDER BY c.total_spent DESC) AS customer_rank,
    RANK() OVER (PARTITION BY c.c_nationkey ORDER BY c.total_spent DESC) AS nation_rank,
    DENSE_RANK() OVER (ORDER BY c.total_orders DESC) AS order_count_rank,
    NTILE(4) OVER (ORDER BY c.total_spent DESC) AS spending_quartile,
    NTILE(10) OVER (ORDER BY c.avg_order_value DESC) AS avg_order_decile,
    LAG(c.total_spent) OVER (ORDER BY c.c_custkey) AS prev_customer_spent,
    LEAD(c.total_spent) OVER (ORDER BY c.c_custkey) AS next_customer_spent,
    FIRST_VALUE(c.total_spent) OVER (ORDER BY c.c_custkey) AS first_customer_spent,
    LAST_VALUE(c.total_spent) OVER (ORDER BY c.c_custkey ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_customer_spent,
    PERCENT_RANK() OVER (ORDER BY c.total_orders) AS order_percentile,
    PERCENT_RANK() OVER (ORDER BY c.total_spent) AS spending_percentile,
    CASE 
        WHEN c.total_spent > 50000 THEN 'Premium'
        WHEN c.total_spent > 20000 THEN 'Gold'
        WHEN c.total_spent > 5000 THEN 'Silver'
        ELSE 'Bronze'
    END AS customer_segment,
    TRY_CAST(c.total_spent AS DECIMAL(15,2)) AS validated_spending,
    CASE WHEN c.avg_order_value IS NULL THEN 1 ELSE 0 END AS avg_value_null_check,
    CASE WHEN c.c_name LIKE 'Customer#[0-9]%' THEN 1 ELSE 0 END AS name_format_check,
    c.order_statuses,
    LEN(c.order_statuses) - LEN(REPLACE(c.order_statuses, ',', '')) + 1 AS status_count,
    CASE WHEN c.order_statuses LIKE '%O%' THEN 1 ELSE 0 END AS profile_validation,
    JSON_OBJECT('customer_id', c.c_custkey, 'total_spent', c.total_spent) AS customer_profile
FROM customer_orders c
WHERE c.total_spent > 1000;

-- Statement 2: Nation Customer Analysis
WITH customer_data AS (
    SELECT 
        c.c_custkey,
        c.c_nationkey,
        SUM(o.o_totalprice) AS total_spent,
        AVG(o.o_totalprice) AS avg_order_value,
        CASE 
            WHEN SUM(o.o_totalprice) > 50000 THEN 'Premium'
            WHEN SUM(o.o_totalprice) > 20000 THEN 'Gold'
            WHEN SUM(o.o_totalprice) > 5000 THEN 'Silver'
            ELSE 'Bronze'
        END AS customer_segment
    FROM customer c
    LEFT JOIN orders o ON c.c_custkey = o.o_custkey
    GROUP BY c.c_custkey, c.c_nationkey
)
SELECT 
    n.n_nationkey,
    n.n_name AS nation_name,
    COUNT(DISTINCT c.c_custkey) AS customer_count,
    AVG(c.c_acctbal) AS avg_customer_balance,
    SUM(cd.total_spent) AS total_nation_spent,
    AVG(cd.avg_order_value) AS avg_order_value_by_nation,
    RANK() OVER (ORDER BY SUM(cd.total_spent) DESC) AS nation_spending_rank,
    DENSE_RANK() OVER (ORDER BY COUNT(DISTINCT c.c_custkey) DESC) AS nation_customer_rank,
    PERCENT_RANK() OVER (ORDER BY AVG(cd.avg_order_value)) AS nation_order_percentile,
    NTILE(5) OVER (ORDER BY COUNT(DISTINCT c.c_custkey)) AS nation_customer_quintile,
    LAG(SUM(cd.total_spent)) OVER (ORDER BY n.n_nationkey) AS prev_nation_spent,
    LEAD(SUM(cd.total_spent)) OVER (ORDER BY n.n_nationkey) AS next_nation_spent,
    FIRST_VALUE(SUM(cd.total_spent)) OVER (ORDER BY n.n_nationkey) AS first_nation_spent,
    LAST_VALUE(SUM(cd.total_spent)) OVER (ORDER BY n.n_nationkey ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_nation_spent,
    CASE 
        WHEN SUM(cd.total_spent) > 100000 THEN 'High Revenue'
        WHEN SUM(cd.total_spent) > 50000 THEN 'Medium Revenue'
        ELSE 'Low Revenue'
    END AS revenue_category,
    STRING_AGG(DISTINCT cd.customer_segment, ',') AS nation_segments,
    COUNT(DISTINCT cd.customer_segment) AS segment_count,
    JSON_OBJECT('nation_id', n.n_nationkey, 'total_revenue', SUM(cd.total_spent)) AS nation_profile
FROM nation n
LEFT JOIN customer c ON n.n_nationkey = c.c_nationkey
LEFT JOIN customer_data cd ON c.c_custkey = cd.c_custkey
GROUP BY n.n_nationkey, n.n_name
HAVING SUM(cd.total_spent) > 5000;

-- Statement 3: Supplier Line Items
WITH supplier_li AS (
    SELECT 
        s.s_suppkey,
        s.s_name,
        s.s_nationkey,
        COUNT(l.l_orderkey) AS total_line_items,
        SUM(l.l_extendedprice) AS total_revenue,
        AVG(l.l_quantity) AS avg_quantity,
        STRING_AGG(DISTINCT l.l_shipmode, ',') AS shipping_modes
    FROM supplier s
    LEFT JOIN lineitem l ON s.s_suppkey = l.l_suppkey
    GROUP BY s.s_suppkey, s.s_name, s.s_nationkey
)
SELECT 
    s.s_suppkey,
    s.s_name,
    s.s_nationkey,
    s.total_line_items,
    s.total_revenue,
    s.avg_quantity,
    RANK() OVER (ORDER BY s.total_revenue DESC) AS supplier_revenue_rank,
    DENSE_RANK() OVER (ORDER BY s.total_line_items DESC) AS supplier_volume_rank,
    PERCENT_RANK() OVER (ORDER BY s.total_line_items) AS supplier_volume_percentile,
    NTILE(4) OVER (ORDER BY s.avg_quantity) AS quantity_quartile,
    LAG(s.total_revenue) OVER (ORDER BY s.s_suppkey) AS prev_supplier_revenue,
    LEAD(s.total_revenue) OVER (ORDER BY s.s_suppkey) AS next_supplier_revenue,
    FIRST_VALUE(s.total_revenue) OVER (ORDER BY s.s_suppkey) AS first_supplier_revenue,
    LAST_VALUE(s.total_revenue) OVER (ORDER BY s.s_suppkey ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_supplier_revenue,
    CASE 
        WHEN s.total_revenue > 50000 THEN 'High Revenue'
        WHEN s.total_revenue > 20000 THEN 'Medium Revenue'
        ELSE 'Low Revenue'
    END AS supplier_category,
    TRY_CAST(s.total_revenue AS DECIMAL(15,2)) AS validated_revenue,
    CASE WHEN s.avg_quantity IS NULL THEN 1 ELSE 0 END AS quantity_null_check,
    CASE WHEN s.s_name LIKE 'Supplier#[0-9]%' THEN 1 ELSE 0 END AS supplier_name_check,
    s.shipping_modes,
    LEN(s.shipping_modes) - LEN(REPLACE(s.shipping_modes, ',', '')) + 1 AS mode_count,
    JSON_OBJECT('supplier_id', s.s_suppkey, 'total_revenue', s.total_revenue) AS supplier_profile
FROM supplier_li s
WHERE s.total_revenue > 1000;

-- Statement 4: Part Line Items
WITH part_li AS (
    SELECT
        p.p_partkey,
        p.p_name,
        p.p_brand,
        p.p_type,
        p.p_size,
        p.p_container,
        p.p_retailprice,
        COUNT(l.l_orderkey) AS times_ordered,
        SUM(l.l_quantity) AS total_quantity_ordered,
        SUM(l.l_extendedprice) AS total_revenue_generated,
        AVG(l.l_discount) AS avg_discount_applied,
        STRING_AGG(DISTINCT l.l_shipmode, ',') AS shipping_modes
    FROM part p
    LEFT JOIN lineitem l ON p.p_partkey = l.l_partkey
    GROUP BY p.p_partkey, p.p_name, p.p_brand, p.p_type, p.p_size, p.p_container, p.p_retailprice
)
SELECT
    p.p_partkey,
    p.p_name,
    p.p_brand,
    p.p_type,
    p.p_size,
    p.p_container,
    p.p_retailprice,
    p.times_ordered,
    p.total_quantity_ordered,
    p.total_revenue_generated,
    p.avg_discount_applied,
    RANK() OVER (PARTITION BY p.p_brand ORDER BY p.times_ordered DESC) AS brand_popularity_rank,
    DENSE_RANK() OVER (ORDER BY p.total_revenue_generated DESC) AS revenue_rank,
    NTILE(5) OVER (ORDER BY p.total_revenue_generated DESC) AS revenue_quintile,
    PERCENT_RANK() OVER (ORDER BY p.times_ordered) AS order_percentile,
    LAG(p.total_revenue_generated) OVER (ORDER BY p.p_partkey) AS prev_part_revenue,
    LEAD(p.total_revenue_generated) OVER (ORDER BY p.p_partkey) AS next_part_revenue,
    FIRST_VALUE(p.total_revenue_generated) OVER (ORDER BY p.p_partkey) AS first_part_revenue,
    LAST_VALUE(p.total_revenue_generated) OVER (ORDER BY p.p_partkey ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_part_revenue,
    CASE 
        WHEN p.total_revenue_generated > 10000 THEN 'High Revenue'
        WHEN p.total_revenue_generated > 5000 THEN 'Medium Revenue'
        ELSE 'Low Revenue'
    END AS part_category,
    TRY_CAST(p.total_revenue_generated AS DECIMAL(15,2)) AS validated_part_revenue,
    CASE WHEN p.total_quantity_ordered IS NULL THEN 1 ELSE 0 END AS quantity_null_check,
    CASE WHEN p.p_name LIKE 'Part#[0-9]%' THEN 1 ELSE 0 END AS part_name_check,
    p.shipping_modes,
    LEN(p.shipping_modes) - LEN(REPLACE(p.shipping_modes, ',', '')) + 1 AS mode_count,
    JSON_OBJECT('part_id', p.p_partkey, 'total_revenue', p.total_revenue_generated) AS part_profile
FROM part_li p
WHERE p.total_revenue_generated > 500;

-- Statement 5: Customer Segmented Analysis
WITH customer_segmented AS (
    SELECT 
        c.c_custkey,
        c.c_name,
        c.c_nationkey,
        SUM(o.o_totalprice) AS total_spent,
        AVG(o.o_totalprice) AS avg_order_value,
        COUNT(o.o_orderkey) AS total_orders,
        CASE 
            WHEN SUM(o.o_totalprice) > 50000 THEN 'Premium'
            WHEN SUM(o.o_totalprice) > 20000 THEN 'Gold'
            WHEN SUM(o.o_totalprice) > 5000 THEN 'Silver'
            ELSE 'Bronze'
        END AS customer_segment,
        ROW_NUMBER() OVER (PARTITION BY 
            CASE 
                WHEN SUM(o.o_totalprice) > 50000 THEN 'Premium'
                WHEN SUM(o.o_totalprice) > 20000 THEN 'Gold'
                WHEN SUM(o.o_totalprice) > 5000 THEN 'Silver'
                ELSE 'Bronze'
            END
             ORDER BY SUM(o.o_totalprice) DESC) AS segment_rank,
        LAG(SUM(o.o_totalprice)) OVER (ORDER BY c.c_custkey) AS prev_customer_spent,
        CASE 
            WHEN SUM(o.o_totalprice) > LAG(SUM(o.o_totalprice)) OVER (ORDER BY c.c_custkey) THEN 'Increasing'
            ELSE 'Decreasing'
        END AS spending_trend,
        TRY_CAST(SUM(o.o_totalprice) AS DECIMAL(15,2)) AS validated_spending,
        CASE WHEN AVG(o.o_totalprice) IS NULL THEN 1 ELSE 0 END AS avg_value_null_check,
        CASE WHEN c.c_name LIKE 'Customer#[0-9]%' THEN 1 ELSE 0 END AS name_format_check,
        STRING_AGG(DISTINCT o.o_orderstatus, ',') AS order_statuses,
        PERCENT_RANK() OVER (ORDER BY COUNT(o.o_orderkey)) AS order_percentile,
        CASE WHEN SUM(o.o_totalprice) > 30000 THEN 'High Value' ELSE 'Standard' END AS value_category,
        CASE WHEN c.c_custkey = c.c_custkey THEN 1 ELSE 0 END AS profile_validation
    FROM customer c
    LEFT JOIN orders o ON c.c_custkey = o.o_custkey
    GROUP BY c.c_custkey, c.c_name, c.c_nationkey
    HAVING SUM(o.o_totalprice) > 1000
),
nation_segmented AS (
    SELECT 
        n.n_nationkey,
        n.n_name AS nation_name,
        RANK() OVER (ORDER BY SUM(cs.total_spent) DESC) AS nation_spending_rank,
        PERCENT_RANK() OVER (ORDER BY AVG(cs.avg_order_value)) AS nation_order_percentile,
        CASE 
            WHEN SUM(cs.total_spent) > 100000 THEN 'High Revenue'
            WHEN SUM(cs.total_spent) > 50000 THEN 'Medium Revenue'
            ELSE 'Low Revenue'
        END AS revenue_category,
        COUNT(DISTINCT cs.customer_segment) AS segment_count,
        JSON_OBJECT('nation_id', n.n_nationkey, 'total_revenue', SUM(cs.total_spent)) AS nation_profile
    FROM nation n
    LEFT JOIN customer_segmented cs ON n.n_nationkey = cs.c_nationkey
    GROUP BY n.n_nationkey, n.n_name
    HAVING SUM(cs.total_spent) > 5000
),
supplier_agg AS (
    SELECT 
        s.s_nationkey,
        RANK() OVER (ORDER BY SUM(l.l_extendedprice) DESC) AS supplier_revenue_rank,
        CASE 
            WHEN SUM(l.l_extendedprice) > 50000 THEN 'High Revenue'
            WHEN SUM(l.l_extendedprice) > 20000 THEN 'Medium Revenue'
            ELSE 'Low Revenue'
        END AS supplier_category
    FROM supplier s
    LEFT JOIN lineitem l ON s.s_suppkey = l.l_suppkey
    GROUP BY s.s_nationkey
    HAVING SUM(l.l_extendedprice) > 1000
),
part_agg AS (
    SELECT 
        p.p_brand,
        RANK() OVER (PARTITION BY p.p_brand ORDER BY COUNT(l.l_orderkey) DESC) AS brand_popularity_rank,
        CASE 
            WHEN SUM(l.l_extendedprice) > 10000 THEN 'High Revenue'
            WHEN SUM(l.l_extendedprice) > 5000 THEN 'Medium Revenue'
            ELSE 'Low Revenue'
        END AS part_category
    FROM part p
    LEFT JOIN lineitem l ON p.p_partkey = l.l_partkey
    GROUP BY p.p_brand
    HAVING SUM(l.l_extendedprice) > 500
)
SELECT TOP 25
    cs.c_custkey,
    cs.c_name,
    cs.customer_segment,
    ns.nation_name,
    cs.total_spent,
    cs.avg_order_value,
    cs.total_orders,
    cs.spending_trend,
    cs.segment_rank,
    ns.nation_spending_rank,
    ROUND(cs.order_percentile * 100, 2) AS order_percentile,
    ROUND(cs.total_spent / NULLIF(cs.prev_customer_spent, 0), 2) AS spending_ratio,
    cs.value_category,
    CASE WHEN cs.total_orders > 15 THEN 'Loyal' ELSE 'New' END AS loyalty_status,
    cs.validated_spending,
    cs.avg_value_null_check,
    cs.name_format_check,
    cs.profile_validation,
    LEN(cs.order_statuses) - LEN(REPLACE(cs.order_statuses, ',', '')) + 1 AS status_count,
    ns.nation_order_percentile,
    ns.revenue_category,
    ns.segment_count,
    ns.nation_profile,
    sup.supplier_revenue_rank,
    sup.supplier_category,
    prt.part_category,
    prt.brand_popularity_rank,
    ROW_NUMBER() OVER (ORDER BY cs.total_spent DESC) AS final_rank,
    RANK() OVER (PARTITION BY cs.customer_segment ORDER BY cs.total_spent DESC) AS segment_final_rank,
    NTILE(4) OVER (ORDER BY cs.total_spent DESC) AS spending_quartile,
    PERCENT_RANK() OVER (ORDER BY cs.total_orders) AS order_percentile_final
FROM customer_segmented cs
JOIN nation_segmented ns ON cs.c_nationkey = ns.n_nationkey
LEFT JOIN supplier_agg sup ON cs.c_nationkey = sup.s_nationkey
LEFT JOIN part_agg prt ON cs.c_custkey % 10 = prt.p_brand % 10
WHERE cs.total_spent IS NOT NULL
  AND cs.avg_value_null_check = 0
  AND cs.name_format_check = 1
  AND cs.profile_validation = 1
ORDER BY cs.total_spent DESC, cs.customer_segment, ns.nation_name; 