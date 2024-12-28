import os
import csv
from airflow.hooks.base_hook import BaseHook
import pandas as pd
import json

def check_airindia_status(conn_id):
    conn = BaseHook.get_connection(conn_id)
    file_path = json.loads(conn.extra)['path']
    print(file_path)
    if os.path.isfile(file_path):
        print(f"The file {file_path} exists")
        return True
    else:
        print(f"The file {file_path} does not exist")
        raise Exception(f"File {file_path} not found")

def get_airindia_data(conn_id):
    conn = BaseHook.get_connection(conn_id)
    file_path = json.loads(conn.extra)['path']
    
    # Define the required columns
    required_columns = ['PlaneID', 'Date', 'Source', 'Destination', 'Fare', 'Currency']
    
    # Read only the required columns from CSV
    try:
        # First, peek at the CSV to get available columns
        with open(file_path, 'r') as f:
            csv_columns = csv.reader(f).__next__()
        
        # Create a mapping of lowercase column names to actual column names
        column_mapping = {col.lower(): col for col in csv_columns}
        
        # Find which required columns are present in the CSV
        usecols = []
        final_column_mapping = {}
        for req_col in required_columns:
            if req_col.lower() in column_mapping:
                usecols.append(column_mapping[req_col.lower()])
                final_column_mapping[column_mapping[req_col.lower()]] = req_col
        
        # Read CSV with only the required columns
        df = pd.read_csv(file_path, usecols=usecols)
        
        # Rename columns to standardized names
        df = df.rename(columns=final_column_mapping)
        
        # Add any missing required columns with default values
        for col in required_columns:
            if col not in df.columns:
                if col == 'Fare':
                    df[col] = 0
                elif col == 'Currency':
                    df[col] = 'INR'  # default currency
                else:
                    df[col] = ''  # empty string for other missing columns
        
        # Reorder columns to match required order
        df = df.reindex(columns=required_columns)
        
        save_data_to_parquet(df, '/opt/airflow/dags/ixigo.parquet')
        
    except Exception as e:
        print(f"Error processing CSV file: {str(e)}")
        raise

def save_data_to_parquet(df, file_path):
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
    
    # Write the combined data back to the Parquet file
    combined_df_vistara.to_parquet(file_path, index=False)
    print(f"Data appended successfully to {file_path}")