import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import os
import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

# Function to get aircraft details from AeroBase Group using tail number with retry logic
def get_aircraft_details(tail_number, retries=3):
    url = f"https://aerobasegroup.com/tail-number-lookup/{tail_number.lower()}"
    session = requests.Session()
    retry = Retry(total=retries, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)

    try:
        response = session.get(url)
        response.raise_for_status()  # Raise HTTPError for bad responses (4xx and 5xx)
    except requests.exceptions.RequestException as e:
        print(f"Failed to get data for {tail_number}: {e}")
        return tail_number, 'N/A', 'N/A'

    soup = BeautifulSoup(response.text, 'html.parser')

    try:
        # Locate the table containing the details
        details_tables = soup.find_all('table', {'class': 'table-border'})
        details = {}
        for table in details_tables:
            rows = table.find_all('tr')
            for row in rows:
                cells = row.find_all('td')
                if len(cells) == 2:
                    key = cells[0].text.strip()
                    value = cells[1].text.strip()
                    details[key] = value

        manufacturer = details.get('Manufacturer', 'N/A')
        model = details.get('Model', 'N/A')

        return tail_number, manufacturer, model
    except (AttributeError, IndexError) as e:
        print(f"Error parsing details for {tail_number}: {e}")
        return tail_number, 'N/A', 'N/A'

# Function to process tail numbers concurrently
def process_tail_numbers_concurrently(tail_numbers, max_workers=20):
    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_tail_number = {executor.submit(get_aircraft_details, tail_number): tail_number for tail_number in tail_numbers}
        for future in as_completed(future_to_tail_number):
            tail_number = future_to_tail_number[future]
            try:
                result = future.result()
                results.append(result)
                print(f"Completed {tail_number}")
            except Exception as e:
                print(f"Exception for {tail_number}: {e}")
                results.append((tail_number, 'N/A', 'N/A'))
    return results

# Ensure the file exists
input_file = 'Merged_Arrivals.xlsx'
if not os.path.exists(input_file):
    print(f"Error: The file '{input_file}' does not exist.")
else:
    try:
        # Load the data while skipping the initial rows that don't contain column headers
        df = pd.read_excel(input_file, engine='openpyxl', skiprows=0)

        # Print column names to debug
        print("Column names in the file:")
        print(df.columns.tolist())

        # Strip whitespace from column names
        df.columns = df.columns.str.strip()

        # Assuming the tail number column is named 'Tail Number'
        if 'Tail Number' not in df.columns:
            raise ValueError("Error: 'Tail Number' column not found in the file.")
        else:
            tail_numbers = df['Tail Number']

            # Dictionary to store details of tail numbers already fetched
            tail_number_details = {}

            # Process tail numbers concurrently
            results = process_tail_numbers_concurrently(tail_numbers)

            # Update DataFrame with results
            for tail_number, manufacturer, model in results:
                indices = df[df['Tail Number'] == tail_number].index
                for idx in indices:
                    df.at[idx, 'Manufacturer'] = manufacturer
                    df.at[idx, 'Model'] = model

                print(f"Completed updating rows for {tail_number}")

            # Save the updated DataFrame to a new Excel file
            output_file = 'Merged_Arrivals_Updated.xlsx'
            df.to_excel(output_file, index=False)
            print(f"Data saved to {output_file}")
    except ValueError as ve:
        print(f"ValueError: {ve}")
    except Exception as e:
        print(f"An error occurred: {e}")