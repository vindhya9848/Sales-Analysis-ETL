CREATE TABLE IF NOT EXISTS cleaned_countries (
    country_code VARCHAR(2),
    country_name VARCHAR(100) NOT NULL,
    business_date date
);

---Adding unique constraint to prevent duplicate entries in cleaned_countries table
---Ensure that each country_code and country_name combination is unique
ALter table cleaned_countries
ADD CONSTRAINT uniq_country_row UNIQUE (country_code, country_name);