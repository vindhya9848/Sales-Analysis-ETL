create OR REPLACE view track_countries as 
select country_name,fiscal_year,
sum(abs(CASE WHEN sales_qty<0 THEN sales_qty ELSE 0 END)) as returned_qty
from sales_country_name
group by country_name, fiscal_year;
-- This view tracks the total returned quantity of sales by country and fiscal year.

--select * from track_countries order by returned_qty desc;