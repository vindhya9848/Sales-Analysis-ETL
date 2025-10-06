INSERT INTO sales_country_name (
    base_sku,
    default_sku_name,
    transactional_uom_code,
    country_code,
    country_name,
    fiscal_year,
    fiscal_year_period_number,
    rc_code,
    b_product_code,
    b_name,
    sales_qty,
    month_of_year,
    business_date
)
SELECT
    s.base_sku,
    s.default_sku_name,
    s.transactional_uom_code,
    c.country_code,
    c.country_name,
    s.fiscal_year,
    s.fiscal_year_period_number,
    s.rc_code,
    s.b_product_code,
    s.b_name,
    s.sales_qty,
    s.month_of_year,
    s.business_date
FROM cleaned_sales s
LEFT JOIN cleaned_countries c -- To fetch all sales records from left table
    ON s.country_code = c.country_code where date(s.business_date) = %s; 
-- Only insert records where the business_date is for a particular day (loading incremental data)