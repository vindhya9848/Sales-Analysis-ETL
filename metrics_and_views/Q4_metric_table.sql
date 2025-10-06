insert into metric_table_sales(
total_sales, country_name, fiscal_year, month_of_year
)
select sum(sales_qty)as  total_sales,
country_name, fiscal_year,month_of_year
from sales_country_name 
where date(business_date) = %s
group by country_name, fiscal_year,month_of_year order  by total_sales desc
ON CONFLICT (country_name, fiscal_year, month_of_year)
DO UPDATE 
SET total_sales = metric_table_sales.total_sales + EXCLUDED.total_sales;

-- this query also updates total_sales when unique of (country_name, fiscal_year, month_of_year) already exists in metric_table_sales by
-- adding the new total_sales to the existing total_sales for that unique combination.

