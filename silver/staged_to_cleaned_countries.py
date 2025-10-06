import pandas as pd
from typing import Tuple
from .data_ingestion_unique import data_ingestion_unique
from config.get_db_config import get_db_config
import pycountry
from .is_valid_country import is_valid_country

def  staged_to_cleaned_countries(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Cleans and quarantines records from the input DataFrame based on country validation.
    validates country codes and names, checks for nulls, and quarantines invalid records.
    valid country names are checked against the pycountry library.
    """
    # Ensure required columns exist
    expected_columns = {'country_code', 'country_name', 'business_date'}

   # print(df.columns)
    if not expected_columns.issubset(df.columns):
        raise ValueError(f"Input DataFrame must contain columns: {expected_columns}")
    
    # Get valid country names using pycountry
    valid_countries = {country.name.lower() for country in pycountry.countries}
    
    # Lists to store records
    cleaned_records = []
    quarantined_records = []

    for _, row in df.iterrows():
        code = row['country_code']
        name = row['country_name']
        business_date = row['business_date']
        reason = None

        # Check for nulls
        if pd.isnull(code) or pd.isnull(name):
            reason = "Missing country_code or country"
        else:
            # Force uppercase country code
            code = code.strip().upper()
            name = name.strip()

            # Check if country name is valid
            if not is_valid_country(name, valid_countries):
                reason = f"Invalid country name: {name}"

        # Route to appropriate bucket
        if reason:
            quarantined_records.append({
                'country_code': code if code else None,
                'country_name': name if name else None,
                'business_date': business_date,
                'reason': reason
            })
        else:
            cleaned_records.append({
                'country_code': code,
                'country_name': name,
                'business_date': business_date
            })

    # Create final DataFrames
    db_config= get_db_config()
    cleaned_df = pd.DataFrame(cleaned_records)
    # to prevent insertion of records that already exist in the cleaned_countries table same logic can be implemented for cleanes_sales table too
    unique_df, rejected_df =data_ingestion_unique(cleaned_df, list(cleaned_df.columns), 'cleaned_countries', db_config, ['country_code', 'country_name'])
    quarantined_df = pd.DataFrame(quarantined_records)
    rejected_df.drop_duplicates(subset=['country_code', 'business_date'], inplace=True)
    quarantined_df = pd.concat([quarantined_df, rejected_df], ignore_index=True)


    return unique_df, quarantined_df
