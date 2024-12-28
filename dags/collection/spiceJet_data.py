import pyodbc
import pandas as pd
import os

def get_spicejet_data_from_sql():
    connection_string = "Driver={ODBC Driver 17 for SQL Server};Server=192.168.42.239;Database=Apache_MiniProject;UID=sa;PWD=NewPassword;"
    
    try:
        with pyodbc.connect(connection_string) as conn:
            query = "SELECT PlaneID,CONVERT(varchar, Date, 120) as Date,Source,Destination,Fare,Currency FROM SpiceJet_Data"
            df = pd.read_sql(query, conn)
            save_data_to_parquet(df.to_dict(orient='records'), '/opt/airflow/dags/ixigo.parquet')
            print("SpiceJet data fetched and saved successfully.")
    except Exception as e:
        print(f"Error fetching SpiceJet data: {e}")
        raise

def save_data_to_parquet(data, file_path):
    df = pd.DataFrame(data)
    
    # Rename columns
    df = df.rename(columns={
        "PlaneID": "PlaneID",
        "Date": "Date",
        "Source": "Source",
        "Destination": "Destination",
        "Fare": "Fare",
        "Currency": "Currency"
    }).reindex(columns=['PlaneID', 'Date', 'Source', 'Destination', 'Fare', 'Currency'])

    # Convert Fare to numeric, coerce errors, and fill NaNs
    df['Fare'] = pd.to_numeric(df['Fare'], errors='coerce')
    df['Fare'].fillna(0, inplace=True)
    print(f"Number of rows in the extracted data: {len(df)}")

    # Check if file exists
    if os.path.exists(file_path):
        # Read the existing data
        existing_df_vistara = pd.read_parquet(file_path)
        print(f"Existing data rows before append: {len(existing_df_vistara)}")

        # Combine new data with existing data
        combined_df_vistara = pd.concat([existing_df_vistara, df], ignore_index=True)
        print(f"Combined data rows before dropping duplicates: {len(combined_df_vistara)}")
        
        # Drop duplicates based on PlaneID and Date, keeping the last entry
        combined_df_vistara = combined_df_vistara.drop_duplicates(subset=['PlaneID', 'Date'], keep='last')
        print(f"Combined data rows after dropping duplicates: {len(combined_df_vistara)}")
    else:
        combined_df_vistara = df
        combined_df_vistara = combined_df_vistara.drop_duplicates(subset=['PlaneID', 'Date'], keep='last')
        
    # Write the combined data back to the Parquet file
    combined_df_vistara.to_parquet(file_path, index=False)
    print(f"Data appended successfully to {file_path}")
