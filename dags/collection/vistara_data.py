import requests
from airflow.hooks.base_hook import BaseHook
import json
from airflow.utils.dates import days_ago
import pandas as pd
import os

def check_vistara_status(conn_id):
    conn = BaseHook.get_connection(conn_id)
    url = f"{conn.host}/Sahil-Vernekar16/Apache_Airflow/refs/heads/main/vistara.json"
    response = requests.get(url)
    if response.status_code == 200:
        print("The link is working")
        return True
    else:
        print("The link is not working")
        raise Exception(f"API is not active. Status code: {response.status_code}")

def get_vistara_data():
    conn = BaseHook.get_connection('githubgistconn')
    url = f"{conn.host}/Sahil-Vernekar16/Apache_Airflow/refs/heads/main/vistara.json"
    response = requests.get(url)
    
    if response.status_code == 200:
        json_data = response.json()
        
        # Define required columns and their possible variations
        column_variations = {
            'planeid': 'PlaneID',
            'plane_id': 'PlaneID',
            'flight_id': 'PlaneID',
            'flightno': 'PlaneID',
            'date': 'Date',
            'flight_date': 'Date',
            'departure_date': 'Date',
            'source': 'Source',
            'from': 'Source',
            'from_city': 'Source',
            'origin': 'Source',
            'destination': 'Destination',
            'to': 'Destination',
            'to_city': 'Destination',
            'fare': 'Fare',
            'price': 'Fare',
            'ticket_fare': 'Fare',
            'currency': 'Currency',
            'fare_currency': 'Currency',
            'currency_code': 'Currency'
        }
        
        # Convert JSON data to DataFrame
        df = pd.DataFrame(json_data['data'])
        
        # Create mapping for existing columns
        final_column_mapping = {}
        required_columns = ['PlaneID', 'Date', 'Source', 'Destination', 'Fare', 'Currency']
        
        # Map existing columns to standard names
        for col in df.columns:
            if col.lower() in column_variations:
                final_column_mapping[col] = column_variations[col.lower()]
            elif col in required_columns:
                final_column_mapping[col] = col
        
        # Rename columns using the mapping
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
        print("Vistara data saved successfully")
    else:
        print("Failed to fetch data")
        raise Exception(f"API is not active. Status code: {response.status_code}")

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