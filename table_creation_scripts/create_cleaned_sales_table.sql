CREATE TABLE IF NOT EXISTS cleaned_sales (
    base_sku VARCHAR(50),
    default_sku_name VARCHAR(40),
    transactional_uom_code VARCHAR(2),
    country_code VARCHAR(2),
    fiscal_year INT,
    fiscal_year_period_number INT,
    rc_code VARCHAR(20),
    b_product_code VARCHAR(50),
    b_name VARCHAR(100),
    Sales_qty INT,
    month_of_year VARCHAR(20),
    business_date DATE
);