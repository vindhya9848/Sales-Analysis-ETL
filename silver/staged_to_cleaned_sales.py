import pandas as pd

import pandas as pd


def staged_to_cleaned_sales(df, columns):
    """
    Perform data quality checks:
      Non-null on all columns in columns
      Remove duplicate records (keep first occurence of the record)
      Add 'reason' column for rejected rows
      Add  'month' column to cleaned data based on fiscal period

    """

    quarantined_parts = []

    df = df.copy()  # Avoid modifying the original DataFrame
    df = df.where(pd.notnull(df), None)

    expected_columns = {'base_sku', 'default_sku_name', 'transactional_uom_code',
                         'country_code', 'fiscal_year', 'fiscal_year_period_number', 
                         'rc_code', 'b_product_code', 'b_name', 'sales_qty','business_date'}
    # Ensure required columns exist
    if not expected_columns.issubset(df.columns):
        raise ValueError(f"Input DataFrame must contain columns: {expected_columns}")
    
    # 1. Check for nulls
    null_mask = df[columns].isnull().any(axis=1)
    if null_mask.any():
        null_df = df[null_mask].copy()
        null_df['reason'] = 'Missing values'
        quarantined_parts.append(null_df)
        df = df[~null_mask]

    # 2. Check duplicates (on all columns)
    duplicate_mask = df.duplicated(subset=columns, keep='first')
    if duplicate_mask.any():
        duplicate_df = df[duplicate_mask].copy()
        duplicate_df['reason'] = 'Duplicate record'
        quarantined_parts.append(duplicate_df)
        df = df[~duplicate_mask]

    # Combine quarantined parts
    if quarantined_parts:
        quarantined_df = pd.concat(quarantined_parts).drop_duplicates()
    else:
        quarantined_df = pd.DataFrame(columns=list(df.columns) + ['reason'])

    # 3. Add 'month' column to cleaned data based on fiscal period
    month_map = {
        1: "January", 2: "February", 3: "March", 4: "April",
        5: "May", 6: "June", 7: "July", 8: "August",
        9: "September", 10: "October", 11: "November", 12: "December"
    }
    df['month_of_year'] = df['fiscal_year_period_number'].map(month_map)

    cleaned_df = df.copy()


    return cleaned_df, quarantined_df


    