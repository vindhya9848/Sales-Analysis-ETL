CREATE TABLE IF NOT EXISTS rejected_countries (
    country_code VARCHAR(2),
    country_name VARCHAR(100) NOT NULL,
    business_date DATE,
    reason VARCHAR(255)
);