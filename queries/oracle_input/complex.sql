-- Complex Oracle PL/SQL Stored Procedure for Lakebridge Complexity Scoring Demo
-- This stored procedure is designed to meet COMPLEX classification criteria:
-- - Exactly 6 loops (COMPLEX threshold: >5 loops)
-- - Conventional Statement count between 30-50
-- - Complex business logic with multiple operations

CREATE OR REPLACE PROCEDURE COMPLEX_CUSTOMER_ANALYSIS (
    p_cursor OUT SYS_REFCURSOR,
    p_result_message OUT VARCHAR2
) AS
    v_customer_count NUMBER := 0;
    v_premium_count NUMBER := 0;
    v_gold_count NUMBER := 0;
    v_silver_count NUMBER := 0;
    v_bronze_count NUMBER := 0;
    v_total_revenue NUMBER := 0;
    v_avg_order_value NUMBER := 0;
    v_loop_run NUMBER := 0;
    v_cust_spent NUMBER := 0;
    v_cust_orders NUMBER := 0;
    v_segment VARCHAR2(50) := '';
    v_trend VARCHAR2(50) := '';
    v_nat_rev NUMBER := 0;
    v_cust_key NUMBER := 0;
    v_cust_name VARCHAR2(100) := '';
    v_cust_nationkey NUMBER := 0;
    v_nation_name VARCHAR2(100) := '';
    
    -- Cursor for customer records
    CURSOR customer_cursor IS
        SELECT DISTINCT c_custkey, c_name, c_nationkey 
        FROM customer 
        WHERE ROWNUM <= 30;
        
    -- Cursor for nation records
    CURSOR nation_cursor IS
        SELECT n_nationkey, n_name FROM nation;
        
BEGIN
    -- Temp Table for final results
    EXECUTE IMMEDIATE 'DROP TABLE TMP_CUSTOMER_ANALYSIS';
EXCEPTION
    WHEN OTHERS THEN
        NULL; -- Table doesn't exist, continue
        
    EXECUTE IMMEDIATE '
        CREATE GLOBAL TEMPORARY TABLE TMP_CUSTOMER_ANALYSIS (
            customer_id NUMBER,
            customer_name VARCHAR2(100),
            customer_segment VARCHAR2(50),
            nation_name VARCHAR2(100),
            total_spent NUMBER,
            avg_order_value NUMBER,
            total_orders NUMBER,
            spending_trend VARCHAR2(50),
            segment_rank NUMBER,
            nation_spending_rank NUMBER,
            value_category VARCHAR2(50),
            loyalty_status VARCHAR2(50)
        ) ON COMMIT PRESERVE ROWS';

    -- 1. Calculate initial aggregations
    SELECT COUNT(*) INTO v_customer_count FROM customer;
    SELECT NVL(SUM(o_totalprice), 0) INTO v_total_revenue FROM orders;
    SELECT NVL(AVG(o_totalprice), 0) INTO v_avg_order_value FROM orders;

    -- LOOP 1: Customer segmentation loop (5x for demo purpose)
    FOR v_loop_run IN 1..5 LOOP
        SELECT COUNT(DISTINCT c.c_custkey) INTO v_premium_count
          FROM customer c JOIN orders o ON c.c_custkey = o.o_custkey
         WHERE o.o_totalprice > 50000;
         
        SELECT COUNT(DISTINCT c.c_custkey) INTO v_gold_count
          FROM customer c JOIN orders o ON c.c_custkey = o.o_custkey
         WHERE o.o_totalprice BETWEEN 20000 AND 50000;
         
        SELECT COUNT(DISTINCT c.c_custkey) INTO v_silver_count
          FROM customer c JOIN orders o ON c.c_custkey = o.o_custkey
         WHERE o.o_totalprice BETWEEN 5000 AND 20000;
         
        SELECT COUNT(DISTINCT c.c_custkey) INTO v_bronze_count
          FROM customer c JOIN orders o ON c.c_custkey = o.o_custkey
         WHERE o.o_totalprice < 5000;
    END LOOP;

    -- LOOP 2: Customer detailed computation (simulate 30 customers)
    FOR customer_record IN customer_cursor LOOP
        v_cust_key := customer_record.c_custkey;
        v_cust_name := customer_record.c_name;
        v_cust_nationkey := customer_record.c_nationkey;

        SELECT NVL(SUM(o_totalprice), 0), COUNT(*) INTO v_cust_spent, v_cust_orders 
        FROM orders WHERE o_custkey = v_cust_key;

        -- Segmentation
        IF v_cust_spent > 50000 THEN
           v_segment := 'Premium';
        ELSIF v_cust_spent > 20000 THEN
           v_segment := 'Gold';
        ELSIF v_cust_spent > 5000 THEN
           v_segment := 'Silver';
        ELSE
           v_segment := 'Bronze';
        END IF;

        -- Spending trend
        IF v_cust_spent > v_avg_order_value * 10 THEN
           v_trend := 'High Spender';
        ELSIF v_cust_spent > v_avg_order_value * 5 THEN
           v_trend := 'Medium Spender';
        ELSE
           v_trend := 'Low Spender';
        END IF;

        -- Get nation name
        SELECT n_name INTO v_nation_name FROM nation WHERE n_nationkey = v_cust_nationkey;

        INSERT INTO TMP_CUSTOMER_ANALYSIS (
            customer_id, customer_name, customer_segment, nation_name,
            total_spent, avg_order_value, total_orders, spending_trend,
            segment_rank, nation_spending_rank, value_category, loyalty_status
        )
        VALUES (
           v_cust_key,
           v_cust_name,
           v_segment,
           v_nation_name,
           v_cust_spent,
           CASE WHEN v_cust_orders > 0 THEN v_cust_spent / v_cust_orders ELSE 0 END,
           v_cust_orders,
           v_trend,
           0, 0,
           CASE WHEN v_cust_spent > 30000 THEN 'High Value' ELSE 'Standard' END,
           CASE WHEN v_cust_orders > 15 THEN 'Loyal' ELSE 'New' END
        );
    END LOOP;

    -- LOOP 3: Nation analysis (iterate all nations)
    FOR nation_record IN nation_cursor LOOP
        SELECT NVL(SUM(o.o_totalprice), 0) INTO v_nat_rev
        FROM customer c
        JOIN orders o ON c.c_custkey = o.o_custkey
        WHERE c.c_nationkey = nation_record.n_nationkey;

        -- Example DML statement for more scoring points
        UPDATE nation SET n_comment = 'Total Revenue: ' || TO_CHAR(v_nat_rev)
        WHERE n_nationkey = nation_record.n_nationkey;
    END LOOP;

    -- LOOP 4: Supplier stats loop (3x)
    FOR v_loop_run IN 1..3 LOOP
        SELECT COUNT(DISTINCT s_suppkey) INTO v_customer_count
        FROM supplier s JOIN lineitem l ON s.s_suppkey = l.l_suppkey 
        WHERE l.l_quantity > 10;
        
        SELECT NVL(SUM(l.l_extendedprice), 0) INTO v_total_revenue
        FROM supplier s JOIN lineitem l ON s.s_suppkey = l.l_suppkey;
    END LOOP;

    -- LOOP 5: Part analysis loop (2x)
    FOR v_loop_run IN 1..2 LOOP
        SELECT COUNT(DISTINCT p_partkey) INTO v_customer_count
          FROM part p JOIN lineitem l ON p.p_partkey = l.l_partkey
          WHERE l.l_quantity > 5;
          
        SELECT NVL(SUM(l.l_extendedprice), 0) INTO v_total_revenue
          FROM part p JOIN lineitem l ON p.p_partkey = l.l_partkey;
    END LOOP;

    -- LOOP 6: Final metrics loop (1x)
    FOR v_loop_run IN 1..1 LOOP
        SELECT COUNT(*) INTO v_customer_count FROM customer WHERE c_acctbal > 5000;
        SELECT AVG(o_totalprice) INTO v_avg_order_value FROM orders;
    END LOOP;

    -- Post-Processing: Rank assignment
    MERGE INTO TMP_CUSTOMER_ANALYSIS t
    USING (
        SELECT customer_id,
               ROW_NUMBER() OVER (ORDER BY total_spent DESC) AS segment_rank,
               RANK() OVER (ORDER BY total_spent DESC) AS nation_spending_rank
        FROM TMP_CUSTOMER_ANALYSIS
    ) r
    ON (t.customer_id = r.customer_id)
    WHEN MATCHED THEN
       UPDATE SET t.segment_rank = r.segment_rank, t.nation_spending_rank = r.nation_spending_rank;

    p_result_message := 'Analysis completed successfully. Processed ' || TO_CHAR(v_customer_count) || ' customers.';
    
    -- Return results
    OPEN p_cursor FOR
        SELECT * FROM TMP_CUSTOMER_ANALYSIS ORDER BY total_spent DESC;
    
    -- Clean up
    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_CUSTOMER_ANALYSIS';
    
EXCEPTION
    WHEN OTHERS THEN
        p_result_message := 'Error: ' || SQLERRM;
        RAISE;
END;
/

-- Execute the stored procedure
DECLARE
    v_cursor SYS_REFCURSOR;
BEGIN
    COMPLEX_CUSTOMER_ANALYSIS(v_cursor);
    -- The cursor can be used to fetch results
    -- For demonstration, we'll just close it
    CLOSE v_cursor;
END;
/ 