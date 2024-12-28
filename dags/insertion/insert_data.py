import pyodbc
import pandas as pd
from airflow.hooks.base_hook import BaseHook

def insert_to_staging(file_path):
    # Fetch connection details from Airflow's connection store
    
    # Establish a connection to the MSSQL server
    connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER=192.168.42.239;DATABASE=Apache_MiniProject;UID=sa;PWD=NewPassword'
    conn_sql = pyodbc.connect(connection_string)
    cursor = conn_sql.cursor()

    # Enable fast executemany to optimize bulk inserts
    cursor.fast_executemany = True

    # Read the data from the Parquet file
    df = pd.read_parquet(file_path)
    
    # Define the staging table name
    table_name = 'staging_airline_data'  # Update with your actual staging table name
    
    # Prepare the data for bulk insertion
    data_to_insert = df[['PlaneID', 'Date', 'Source', 'Destination', 'Fare', 'Currency']].values.tolist()

    # Insert the data in bulk
    insert_query = f"""
        INSERT INTO {table_name} (PlaneID, Date, Source, Destination, Fare, Currency)
        VALUES (?, ?, ?, ?, ?, ?)
    """
    cursor.executemany(insert_query, data_to_insert)

    # Commit the transaction and close the connection
    conn_sql.commit()
    cursor.close()
    conn_sql.close()

    print("Data inserted into staging table successfully")
