import ibis

# Connect to the SQLite database
conn = ibis.sqlite.connect('prospectleap.db')

try:
    # Drop the existing table if it exists
    conn.raw_sql('DROP TABLE IF EXISTS trackerNews')
    
    # Create the new table
    create_table_query = '''
    CREATE TABLE trackerFinviz (
        ticker TEXT NOT NULL,
        finvizStatus TEXT NOT NULL,
        FOREIGN KEY (ticker) REFERENCES companyDetails(ticker)
    )
    '''
    conn.raw_sql(create_table_query)

    # Insert data, handling 'NA' ticker correctly
    insert_query = '''
    INSERT INTO trackerFinviz (ticker, finvizStatus)
    SELECT 
        COALESCE(ticker, 'NA') as ticker,
        'TODO' as finvizStatus
    FROM companyDetails
    '''
    
    conn.raw_sql(insert_query)

    # Verify the insertion
    tracker_table = conn.table('trackerFinviz')
    count = tracker_table.count().execute()
    print(f"Inserted {count} rows into trackerFinviz")
    
    # Show sample data
    print("\nSample data:")
    sample_data = tracker_table.order_by('ticker').limit(5).execute()
    print(sample_data)

except Exception as e:
    print(f"An error occurred: {e}")
