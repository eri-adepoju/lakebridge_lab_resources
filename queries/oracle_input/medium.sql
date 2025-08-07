-- Medium Complexity Oracle SQL Query for Lakebridge Complexity Scoring Demo
-- This query is designed to meet MEDIUM classification criteria:
-- - Conventional Statement count > 10
-- - Medium/High category breaks > 0
-- - Uses complex SQL constructs that trigger category breaks

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
        LISTAGG(DISTINCT o.o_orderstatus, ',') WITHIN GROUP (ORDER BY o.o_orderstatus) AS order_statuses
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
    TO_NUMBER(c.total_spent) AS validated_spending,
    CASE 
        WHEN c.avg_order_value IS NULL THEN 1
        ELSE 0
    END AS avg_value_null_check,
    CASE 
        WHEN REGEXP_LIKE(c.c_name, 'Customer#[0-9]+') THEN 1
        ELSE 0
    END AS name_format_check,
    c.order_statuses,
    LENGTH(c.order_statuses) - LENGTH(REPLACE(c.order_statuses, ',', '')) + 1 AS status_count,
    CASE 
        WHEN c.order_statuses LIKE '%O%' THEN 1
        ELSE 0
    END AS profile_validation,
    '{"customer_id":' || c.c_custkey || ',"total_spent":' || c.total_spent || '}' AS customer_profile
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
    LISTAGG(DISTINCT cd.customer_segment, ',') WITHIN GROUP (ORDER BY cd.customer_segment) AS nation_segments,
    COUNT(DISTINCT cd.customer_segment) AS segment_count,
    '{"nation_id":' || n.n_nationkey || ',"total_revenue":' || SUM(cd.total_spent) || '}' AS nation_profile
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
        LISTAGG(DISTINCT l.l_shipmode, ',') WITHIN GROUP (ORDER BY l.l_shipmode) AS shipping_modes
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
    TO_NUMBER(s.total_revenue) AS validated_revenue,
    CASE 
        WHEN s.avg_quantity IS NULL THEN 1
        ELSE 0
    END AS quantity_null_check,
    CASE 
        WHEN REGEXP_LIKE(s.s_name, 'Supplier#[0-9]+') THEN 1
        ELSE 0
    END AS supplier_name_check,
    s.shipping_modes,
    LENGTH(s.shipping_modes) - LENGTH(REPLACE(s.shipping_modes, ',', '')) + 1 AS mode_count,
    '{"supplier_id":' || s.s_suppkey || ',"total_revenue":' || s.total_revenue || '}' AS supplier_profile
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
        LISTAGG(DISTINCT l.l_shipmode, ',') WITHIN GROUP (ORDER BY l.l_shipmode) AS shipping_modes
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
    TO_NUMBER(p.total_revenue_generated) AS validated_part_revenue,
    CASE 
        WHEN p.total_quantity_ordered IS NULL THEN 1
        ELSE 0
    END AS quantity_null_check,
    CASE 
        WHEN REGEXP_LIKE(p.p_name, 'Part#[0-9]+') THEN 1
        ELSE 0
    END AS part_name_check,
    p.shipping_modes,
    LENGTH(p.shipping_modes) - LENGTH(REPLACE(p.shipping_modes, ',', '')) + 1 AS mode_count,
    '{"part_id":' || p.p_partkey || ',"total_revenue":' || p.total_revenue_generated || '}' AS part_profile
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
        TO_NUMBER(SUM(o.o_totalprice)) AS validated_spending,
        CASE 
            WHEN AVG(o.o_totalprice) IS NULL THEN 1
            ELSE 0
        END AS avg_value_null_check,
        CASE 
            WHEN REGEXP_LIKE(c.c_name, 'Customer#[0-9]+') THEN 1
            ELSE 0
        END AS name_format_check,
        LISTAGG(DISTINCT o.o_orderstatus, ',') WITHIN GROUP (ORDER BY o.o_orderstatus) AS order_statuses,
        PERCENT_RANK() OVER (ORDER BY COUNT(o.o_orderkey)) AS order_percentile,
        CASE 
            WHEN SUM(o.o_totalprice) > 30000 THEN 'High Value'
            ELSE 'Standard'
        END AS value_category,
        CASE 
            WHEN c.c_custkey = c.c_custkey THEN 1
            ELSE 0
        END AS profile_validation
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
        '{"nation_id":' || n.n_nationkey || ',"total_revenue":' || SUM(cs.total_spent) || '}' AS nation_profile
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
SELECT 
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
    CASE 
        WHEN cs.total_orders > 15 THEN 'Loyal'
        ELSE 'New'
    END AS loyalty_status,
    cs.validated_spending,
    cs.avg_value_null_check,
    cs.name_format_check,
    cs.profile_validation,
    LENGTH(cs.order_statuses) - LENGTH(REPLACE(cs.order_statuses, ',', '')) + 1 AS status_count,
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
LEFT JOIN part_agg prt ON MOD(cs.c_custkey, 10) = MOD(prt.p_brand, 10)
WHERE cs.total_spent IS NOT NULL
  AND cs.avg_value_null_check = 0
  AND cs.name_format_check = 1
  AND cs.profile_validation = 1
ORDER BY cs.total_spent DESC, cs.customer_segment, ns.nation_name
FETCH FIRST 25 ROWS ONLY;

-- Statement 6: Additional customer ranking analysis with complex window functions
SELECT 
    c.c_custkey,
    c.c_name,
    ROW_NUMBER() OVER (ORDER BY SUM(o.o_totalprice) DESC) AS overall_rank,
    RANK() OVER (PARTITION BY c.c_nationkey ORDER BY SUM(o.o_totalprice) DESC) AS nation_rank,
    DENSE_RANK() OVER (ORDER BY COUNT(o.o_orderkey) DESC) AS order_count_rank,
    NTILE(10) OVER (ORDER BY AVG(o.o_totalprice) DESC) AS avg_order_decile,
    PERCENT_RANK() OVER (ORDER BY SUM(o.o_totalprice)) AS spending_percentile,
    LAG(SUM(o.o_totalprice)) OVER (ORDER BY c.c_custkey) AS prev_customer_spent,
    LEAD(SUM(o.o_totalprice)) OVER (ORDER BY c.c_custkey) AS next_customer_spent,
    FIRST_VALUE(SUM(o.o_totalprice)) OVER (ORDER BY c.c_custkey) AS first_customer_spent,
    LAST_VALUE(SUM(o.o_totalprice)) OVER (ORDER BY c.c_custkey) AS last_customer_spent,
    CASE 
        WHEN SUM(o.o_totalprice) > 50000 THEN 'Premium'
        WHEN SUM(o.o_totalprice) > 20000 THEN 'Gold'
        WHEN SUM(o.o_totalprice) > 5000 THEN 'Silver'
        ELSE 'Bronze'
    END AS customer_segment,
    TO_NUMBER(SUM(o.o_totalprice)) AS validated_spending,
    CASE 
        WHEN AVG(o.o_totalprice) IS NULL THEN 1
        ELSE 0
    END AS avg_value_null_check,
    CASE 
        WHEN REGEXP_LIKE(c.c_name, 'Customer#[0-9]+') THEN 1
        ELSE 0
    END AS name_format_check,
    LISTAGG(DISTINCT o.o_orderstatus, ',') WITHIN GROUP (ORDER BY o.o_orderstatus) AS order_statuses,
    COUNT(DISTINCT o.o_orderstatus) AS status_count,
    '{"customer_id":' || c.c_custkey || ',"total_spent":' || SUM(o.o_totalprice) || '}' AS customer_profile
FROM customer c
LEFT JOIN orders o ON c.c_custkey = o.o_custkey
GROUP BY c.c_custkey, c.c_name, c.c_nationkey
HAVING SUM(o.o_totalprice) > 1000;

-- Statement 7: Nation revenue analysis with complex aggregations
SELECT 
    n.n_nationkey,
    n.n_name AS nation_name,
    COUNT(DISTINCT c.c_custkey) AS customer_count,
    AVG(c.c_acctbal) AS avg_customer_balance,
    SUM(customer_spending.total_spent) AS total_nation_spent,
    AVG(customer_spending.avg_order_value) AS avg_order_value_by_nation,
    RANK() OVER (ORDER BY SUM(customer_spending.total_spent) DESC) AS nation_spending_rank,
    PERCENT_RANK() OVER (ORDER BY AVG(customer_spending.avg_order_value)) AS nation_order_percentile,
    NTILE(5) OVER (ORDER BY COUNT(DISTINCT c.c_custkey)) AS nation_customer_quintile,
    LAG(SUM(customer_spending.total_spent)) OVER (ORDER BY n.n_nationkey) AS prev_nation_spent,
    LEAD(SUM(customer_spending.total_spent)) OVER (ORDER BY n.n_nationkey) AS next_nation_spent,
    FIRST_VALUE(SUM(customer_spending.total_spent)) OVER (ORDER BY n.n_nationkey) AS first_nation_spent,
    LAST_VALUE(SUM(customer_spending.total_spent)) OVER (ORDER BY n.n_nationkey) AS last_nation_spent,
    CASE 
        WHEN SUM(customer_spending.total_spent) > 100000 THEN 'High Revenue'
        WHEN SUM(customer_spending.total_spent) > 50000 THEN 'Medium Revenue'
        ELSE 'Low Revenue'
    END AS revenue_category,
    LISTAGG(DISTINCT customer_spending.customer_segment, ',') WITHIN GROUP (ORDER BY customer_spending.customer_segment) AS nation_segments,
    COUNT(DISTINCT customer_spending.customer_segment) AS segment_count,
    '{"nation_id":' || n.n_nationkey || ',"total_revenue":' || SUM(customer_spending.total_spent) || '}' AS nation_profile
FROM nation n
LEFT JOIN customer c ON n.n_nationkey = c.c_nationkey
LEFT JOIN (
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
) customer_spending ON c.c_custkey = customer_spending.c_custkey
GROUP BY n.n_nationkey, n.n_name
HAVING SUM(customer_spending.total_spent) > 5000;

-- Statement 8: Supplier performance ranking with complex window functions
SELECT 
    s.s_suppkey,
    s.s_name,
    s.s_nationkey,
    COUNT(l.l_orderkey) AS total_line_items,
    SUM(l.l_extendedprice) AS total_revenue,
    AVG(l.l_quantity) AS avg_quantity,
    RANK() OVER (ORDER BY SUM(l.l_extendedprice) DESC) AS supplier_revenue_rank,
    DENSE_RANK() OVER (ORDER BY COUNT(l.l_orderkey) DESC) AS supplier_volume_rank,
    PERCENT_RANK() OVER (ORDER BY COUNT(l.l_orderkey)) AS supplier_volume_percentile,
    NTILE(4) OVER (ORDER BY AVG(l.l_quantity)) AS quantity_quartile,
    LAG(SUM(l.l_extendedprice)) OVER (ORDER BY s.s_suppkey) AS prev_supplier_revenue,
    LEAD(SUM(l.l_extendedprice)) OVER (ORDER BY s.s_suppkey) AS next_supplier_revenue,
    FIRST_VALUE(SUM(l.l_extendedprice)) OVER (ORDER BY s.s_suppkey) AS first_supplier_revenue,
    LAST_VALUE(SUM(l.l_extendedprice)) OVER (ORDER BY s.s_suppkey) AS last_supplier_revenue,
    CASE 
        WHEN SUM(l.l_extendedprice) > 50000 THEN 'High Revenue'
        WHEN SUM(l.l_extendedprice) > 20000 THEN 'Medium Revenue'
        ELSE 'Low Revenue'
    END AS supplier_category,
    TO_NUMBER(SUM(l.l_extendedprice)) AS validated_revenue,
    CASE 
        WHEN AVG(l.l_quantity) IS NULL THEN 1
        ELSE 0
    END AS quantity_null_check,
    CASE 
        WHEN REGEXP_LIKE(s.s_name, 'Supplier#[0-9]+') THEN 1
        ELSE 0
    END AS supplier_name_check,
    LISTAGG(DISTINCT l.l_shipmode, ',') WITHIN GROUP (ORDER BY l.l_shipmode) AS shipping_modes,
    COUNT(DISTINCT l.l_shipmode) AS mode_count,
    '{"supplier_id":' || s.s_suppkey || ',"total_revenue":' || SUM(l.l_extendedprice) || '}' AS supplier_profile
FROM supplier s
LEFT JOIN lineitem l ON s.s_suppkey = l.l_suppkey
GROUP BY s.s_suppkey, s.s_name, s.s_nationkey
HAVING SUM(l.l_extendedprice) > 1000;

-- Statement 9: Part revenue analysis with complex window functions
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
    RANK() OVER (PARTITION BY p.p_brand ORDER BY COUNT(l.l_orderkey) DESC) AS brand_popularity_rank,
    DENSE_RANK() OVER (ORDER BY SUM(l.l_extendedprice) DESC) AS revenue_rank,
    NTILE(5) OVER (ORDER BY SUM(l.l_extendedprice) DESC) AS revenue_quintile,
    PERCENT_RANK() OVER (ORDER BY COUNT(l.l_orderkey)) AS order_percentile,
    LAG(SUM(l.l_extendedprice)) OVER (ORDER BY p.p_partkey) AS prev_part_revenue,
    LEAD(SUM(l.l_extendedprice)) OVER (ORDER BY p.p_partkey) AS next_part_revenue,
    FIRST_VALUE(SUM(l.l_extendedprice)) OVER (ORDER BY p.p_partkey) AS first_part_revenue,
    LAST_VALUE(SUM(l.l_extendedprice)) OVER (ORDER BY p.p_partkey) AS last_part_revenue,
    CASE 
        WHEN SUM(l.l_extendedprice) > 10000 THEN 'High Revenue'
        WHEN SUM(l.l_extendedprice) > 5000 THEN 'Medium Revenue'
        ELSE 'Low Revenue'
    END AS part_category,
    TO_NUMBER(SUM(l.l_extendedprice)) AS validated_part_revenue,
    CASE 
        WHEN AVG(l.l_quantity) IS NULL THEN 1
        ELSE 0
    END AS quantity_null_check,
    CASE 
        WHEN REGEXP_LIKE(p.p_name, 'Part#[0-9]+') THEN 1
        ELSE 0
    END AS part_name_check,
    LISTAGG(DISTINCT l.l_shipmode, ',') WITHIN GROUP (ORDER BY l.l_shipmode) AS shipping_modes,
    COUNT(DISTINCT l.l_shipmode) AS mode_count,
    '{"part_id":' || p.p_partkey || ',"total_revenue":' || SUM(l.l_extendedprice) || '}' AS part_profile
FROM part p
LEFT JOIN lineitem l ON p.p_partkey = l.l_partkey
GROUP BY p.p_partkey, p.p_name, p.p_brand, p.p_type, p.p_size, p.p_container, p.p_retailprice
HAVING SUM(l.l_extendedprice) > 500;

-- Statement 10: Final comprehensive analysis with all complex features
SELECT 
    customer_data.c_custkey,
    customer_data.c_name,
    customer_data.customer_segment,
    nation_data.nation_name,
    customer_data.total_spent,
    customer_data.avg_order_value,
    customer_data.total_orders,
    customer_data.spending_trend,
    customer_data.segment_rank,
    nation_data.nation_spending_rank,
    ROUND(customer_data.order_percentile * 100, 2) AS order_percentile,
    ROUND(customer_data.total_spent / NULLIF(customer_data.prev_customer_spent, 0), 2) AS spending_ratio,
    customer_data.value_category,
    CASE 
        WHEN customer_data.total_orders > 15 THEN 'Loyal'
        ELSE 'New'
    END AS loyalty_status,
    customer_data.validated_spending,
    customer_data.avg_value_null_check,
    customer_data.name_format_check,
    customer_data.profile_validation,
    customer_data.status_count,
    nation_data.nation_order_percentile,
    nation_data.revenue_category,
    nation_data.segment_count,
    nation_data.nation_profile,
    supplier_data.supplier_revenue_rank,
    supplier_data.supplier_category,
    part_data.part_category,
    part_data.brand_popularity_rank,
    ROW_NUMBER() OVER (ORDER BY customer_data.total_spent DESC) AS final_rank,
    RANK() OVER (PARTITION BY customer_data.customer_segment ORDER BY customer_data.total_spent DESC) AS segment_final_rank,
    NTILE(4) OVER (ORDER BY customer_data.total_spent DESC) AS spending_quartile,
    PERCENT_RANK() OVER (ORDER BY customer_data.total_orders) AS order_percentile_final
FROM (
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
        TO_NUMBER(SUM(o.o_totalprice)) AS validated_spending,
        CASE 
            WHEN AVG(o.o_totalprice) IS NULL THEN 1
            ELSE 0
        END AS avg_value_null_check,
        CASE 
            WHEN REGEXP_LIKE(c.c_name, 'Customer#[0-9]+') THEN 1
            ELSE 0
        END AS name_format_check,
        LISTAGG(DISTINCT o.o_orderstatus, ',') WITHIN GROUP (ORDER BY o.o_orderstatus) AS order_statuses,
        PERCENT_RANK() OVER (ORDER BY COUNT(o.o_orderkey)) AS order_percentile,
        CASE 
            WHEN SUM(o.o_totalprice) > 30000 THEN 'High Value'
            ELSE 'Standard'
        END AS value_category,
        CASE 
            WHEN c.c_custkey = c.c_custkey THEN 1
            ELSE 0
        END AS profile_validation,
        COUNT(DISTINCT o.o_orderstatus) AS status_count
    FROM customer c
    LEFT JOIN orders o ON c.c_custkey = o.o_custkey
    GROUP BY c.c_custkey, c.c_name, c.c_nationkey
    HAVING SUM(o.o_totalprice) > 1000
) customer_data
JOIN (
    SELECT 
        n.n_nationkey,
        n.n_name AS nation_name,
        RANK() OVER (ORDER BY SUM(customer_spending.total_spent) DESC) AS nation_spending_rank,
        PERCENT_RANK() OVER (ORDER BY AVG(customer_spending.avg_order_value)) AS nation_order_percentile,
        CASE 
            WHEN SUM(customer_spending.total_spent) > 100000 THEN 'High Revenue'
            WHEN SUM(customer_spending.total_spent) > 50000 THEN 'Medium Revenue'
            ELSE 'Low Revenue'
        END AS revenue_category,
        COUNT(DISTINCT customer_spending.customer_segment) AS segment_count,
        '{"nation_id":' || n.n_nationkey || ',"total_revenue":' || SUM(customer_spending.total_spent) || '}' AS nation_profile
    FROM nation n
    LEFT JOIN (
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
    ) customer_spending ON n.n_nationkey = customer_spending.c_nationkey
    GROUP BY n.n_nationkey, n.n_name
    HAVING SUM(customer_spending.total_spent) > 5000
) nation_data ON customer_data.c_nationkey = nation_data.n_nationkey
LEFT JOIN (
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
) supplier_data ON customer_data.c_nationkey = supplier_data.s_nationkey
LEFT JOIN (
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
) part_data ON MOD(customer_data.c_custkey, 10) = MOD(part_data.p_brand, 10)
WHERE customer_data.total_spent IS NOT NULL
  AND customer_data.avg_value_null_check = 0
  AND customer_data.name_format_check = 1
  AND customer_data.profile_validation = 1
ORDER BY customer_data.total_spent DESC, customer_data.customer_segment, nation_data.nation_name
FETCH FIRST 25 ROWS ONLY;

-- Statement 11: Additional statement to ensure MEDIUM complexity classification
-- This statement adds one more conventional statement to push count above 10
SELECT 
    c.c_custkey,
    c.c_name,
    COUNT(o.o_orderkey) AS order_count,
    SUM(o.o_totalprice) AS total_spent,
    AVG(o.o_totalprice) AS avg_order_value,
    ROW_NUMBER() OVER (ORDER BY SUM(o.o_totalprice) DESC) AS spending_rank,
    RANK() OVER (PARTITION BY c.c_nationkey ORDER BY COUNT(o.o_orderkey) DESC) AS nation_order_rank,
    DENSE_RANK() OVER (ORDER BY AVG(o.o_totalprice) DESC) AS avg_value_rank,
    NTILE(5) OVER (ORDER BY SUM(o.o_totalprice) DESC) AS spending_quintile,
    PERCENT_RANK() OVER (ORDER BY COUNT(o.o_orderkey)) AS order_percentile,
    LAG(SUM(o.o_totalprice)) OVER (ORDER BY c.c_custkey) AS prev_customer_spent,
    LEAD(SUM(o.o_totalprice)) OVER (ORDER BY c.c_custkey) AS next_customer_spent,
    FIRST_VALUE(SUM(o.o_totalprice)) OVER (ORDER BY c.c_custkey) AS first_customer_spent,
    LAST_VALUE(SUM(o.o_totalprice)) OVER (ORDER BY c.c_custkey) AS last_customer_spent,
    CASE 
        WHEN SUM(o.o_totalprice) > 50000 THEN 'Premium'
        WHEN SUM(o.o_totalprice) > 20000 THEN 'Gold'
        WHEN SUM(o.o_totalprice) > 5000 THEN 'Silver'
        ELSE 'Bronze'
    END AS customer_segment,
    TO_NUMBER(SUM(o.o_totalprice)) AS validated_spending,
    CASE 
        WHEN AVG(o.o_totalprice) IS NULL THEN 1
        ELSE 0
    END AS avg_value_null_check,
    CASE 
        WHEN REGEXP_LIKE(c.c_name, 'Customer#[0-9]+') THEN 1
        ELSE 0
    END AS name_format_check,
    LISTAGG(DISTINCT o.o_orderstatus, ',') WITHIN GROUP (ORDER BY o.o_orderstatus) AS order_statuses,
    COUNT(DISTINCT o.o_orderstatus) AS status_count,
    '{"customer_id":' || c.c_custkey || ',"total_spent":' || SUM(o.o_totalprice) || '}' AS customer_profile
FROM customer c
LEFT JOIN orders o ON c.c_custkey = o.o_custkey
GROUP BY c.c_custkey, c.c_name, c.c_nationkey
HAVING SUM(o.o_totalprice) > 500; 