create table metric_table_sales(
    total_sales NUMERIC,
    country_name VARCHAR(100),
    fiscal_year INT,
    month_of_year VARCHAR(20)
);

-- adding unique contstraint so that when incremental data arrives, the total_Sales is upadted
-- by adding the new sales_qty to the existing total_sales
ALTER TABLE metric_table_sales
ADD CONSTRAINT uniq_sales_metrics UNIQUE (country_name, fiscal_year, month_of_year);