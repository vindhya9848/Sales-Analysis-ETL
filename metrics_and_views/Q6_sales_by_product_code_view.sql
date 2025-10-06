CREATE OR REPLACE FUNCTION create_dynamic_pivot_view()
RETURNS void AS $$
DECLARE
    col_list TEXT;
    final_sql TEXT;
BEGIN
    --  Dynamically building  column list based on unique b_product_code values
    SELECT string_agg(
        format(
            'SUM(CASE WHEN b_product_code = %L THEN sales_qty ELSE 0 END) AS "%s"', 
            b_product_code, b_product_code
        ), 
        ', ' || E'\n'
    )
    INTO col_list
    FROM (
        SELECT DISTINCT b_product_code 
        FROM sales_country_name 
        ORDER BY b_product_code
    ) AS codes;

 --Building  final SQL for CREATE OR REPLACE VIEW
    final_sql := format($f$
        CREATE OR REPLACE VIEW pivoted_by_product_code AS
        SELECT
            rc_code,
            %s
        FROM sales_country_name
        GROUP BY rc_code
    $f$, col_list);

    -- Executing SQL to create or replace the view
	EXECUTE 'DROP VIEW IF EXISTS pivoted_by_product_code';
    EXECUTE final_sql;

    RAISE NOTICE 'View created or replaced successfully.';
END;
$$ LANGUAGE plpgsql;

-- Call the function to create the view
SELECT create_dynamic_pivot_view();

