import random
import json
from faker import Faker
from datetime import datetime, timedelta

# Initialize Faker instance for random city generation
fake = Faker()

# List of cities for source and destination
cities = ["Delhi", "Mumbai", "Chennai", "Bengaluru", "Hyderabad", "Pune", "Kolkata", "Jaipur", "Lucknow", "Goa"]

# Currency types to assign
currencies = ["INR", "USD", "EUR", "GBP"]

# Function to generate a random date within a range
def generate_random_date(start_date, end_date):
    delta = end_date - start_date
    random_days = random.randint(0, delta.days)
    return (start_date + timedelta(days=random_days)).strftime('%Y-%m-%d')

# Function to generate random Vistara data
def generate_vistara_data(num_rows, batch_size=100000):
    data = []
    start_date = datetime(2024, 3, 1)
    end_date = datetime(2024, 12, 31)  # Extended date range for more variability

    with open('vistara_data_large.json', 'w') as json_file:
        json_file.write('{"data": [\n')  # Start of JSON file
        for i in range(num_rows):
            plane_id = f"VIS{random.randint(300, 999)}"
            date = generate_random_date(start_date, end_date)
            source = random.choice(cities)
            destination = random.choice([city for city in cities if city != source])
            fare = round(random.uniform(5000, 7500), 2)  # Random fare between 5000 and 7500
            currency = random.choice(currencies)

            # Append the data in JSON format
            json_file.write(json.dumps({
                "PlaneID": plane_id,
                "Date": date,
                "Source": source,
                "Destination": destination,
                "Fare": fare,
                "Currency": currency
            }))
            if i < num_rows - 1:
                json_file.write(',\n')  # Add a comma except for the last row
        
        json_file.write('\n]}')  # End of JSON file

    print(f"Large Vistara dataset with {num_rows} rows has been generated and saved as 'vistara_data_large.json'.")

# Generate the data for 1,000,000 rows
generate_vistara_data(10000)
