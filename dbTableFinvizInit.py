import ibis

# Connect to the SQLite database
conn = ibis.sqlite.connect('prospectleap.db')

# SQL query to create table
create_table_query = """
CREATE TABLE IF NOT EXISTS finviz (
    ticker TEXT,
    date TEXT,
    title TEXT,
    link TEXT,
    isInNewsDetails TEXT,
    FOREIGN KEY (ticker) REFERENCES companyDetails(ticker)
)
"""
# Execute the SQL query
conn.raw_sql(create_table_query)
conn.con.commit()

# Confirm message
print("Table 'finviz' successfully created")
