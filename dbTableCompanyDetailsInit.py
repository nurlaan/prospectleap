# This workflow initiates Total Stock population (all stocks <$20 per share) from csv file

import ibis
import pandas as pd
from ibis import Schema

# Connect to SQLite database
con = ibis.sqlite.connect('prospectleap.db')

# Define the schema for the target table with ticker as primary key
schema = Schema({
    'id': 'int32',
    'ticker': 'string',
    'company': 'string',
    'sector': 'string',
    'industry': 'string',
    'country': 'string',
    'market_cap': 'string',
    'price': 'float64',
    'float': 'float64'
})

# Read CSV file using pandas
df = pd.read_csv('/home/nurlan/projects/prospect_leap/dev/Stocks_population.csv')

# Rename columns to match SQLite table structure
column_mapping = {
    'No.': 'id',
    'Ticker': 'ticker',
    'Company': 'company',
    'Sector': 'sector',
    'Industry': 'industry',
    'Country': 'country',
    'Market Cap': 'market_cap',
    'Price': 'price',
    'Float': 'float'
}
df = df.rename(columns=column_mapping)

# Drop existing table if it exists
if 'companyDetails' in con.list_tables():
    con.drop_table('companyDetails')

# Create the table with PRIMARY KEY constraint
create_table_sql = """
CREATE TABLE companyDetails (
    id INTEGER,
    ticker TEXT PRIMARY KEY,
    company TEXT,
    sector TEXT,
    industry TEXT,
    country TEXT,
    market_cap TEXT,
    price NUMERIC,
    float NUMERIC
)
"""
con.raw_sql(create_table_sql)

# Create an ibis table from the pandas DataFrame
table = ibis.memtable(df)

# Insert data into SQLite table
con.insert('companyDetails', table)

print("Data successfully loaded into SQLite database!")


