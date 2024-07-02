import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import logging
import os
import airportsdata
import math
import sys
import numpy as np
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import WebDriverException, TimeoutException, InvalidSessionIdException
from webdriver_manager.chrome import ChromeDriverManager
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from datetime import datetime
from selenium.webdriver.common.keys import Keys
import atexit
import threading 

driver = None 

start_time = time.time()

if len(sys.argv) != 3:
    print("Usage: python arrivals.py <date> <directory>")
    sys.exit(1)

date = sys.argv[1]
date_directory = sys.argv[2]

def create_date_directory(date):
    if not os.path.exists(date):
        os.makedirs(date)

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

# Load airport data
airports = airportsdata.load('IATA')

def get_airport_coords(iata_code):
    airport = airports.get(iata_code)
    if airport:
        return airport['lat'], airport['lon']
    return None, None

def haversine(lat1, lon1, lat2, lon2):
    R = 6371  # Earth radius in kilometers
    d_lat = math.radians(lat2 - lat1)
    d_lon = math.radians(lat2 - lon1)
    a = math.sin(d_lat / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(d_lon / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c

#CO2 emissions per kilometer for different aircraft models (kg CO2 per km)
co2_emissions_per_km = {
    'Aerospatiale AS350 B2 AStar': 4.5,
    'Aerospatiale AS350 B2 AStar': 4.5,
    'Aerospatiale ATR 72-212F': 4.0,
    'Aerospatiale ATR 72-212F': 4.0,
    'Aerospatiale ATR 72-600': 4.2,
    'Aerospatiale ATR 72-600': 4.2,
    'AgustaWestland AW139': 4.5,
    'AgustaWestland AW139': 4.5,
    'Airbus A220-100': 6.0,
    'Airbus A220-100': 6.0,
    'Airbus A220-371': 6.5,
    'Airbus A220-371': 6.5,
    'Airbus A300B4-605R(F)': 10.5,
    'Airbus A300B4-605R(F)': 10.5,
    'Airbus A300F4-605R': 10.5,
    'Airbus A300F4-605R': 10.5,
    'Airbus A319-114': 6.5,
    'Airbus A319-114': 6.5,
    'Airbus A319-131': 6.5,
    'Airbus A319-131': 6.5,
    'Airbus A319-132': 6.5,
    'Airbus A319-132': 6.5,
    'Airbus A320-214': 6.5,
    'Airbus A320-214': 6.5,
    'Airbus A320-232': 6.5,
    'Airbus A320-232': 6.5,
    'Airbus A321-211': 6.5,
    'Airbus A321-211': 6.5,
    'Airbus A321-253NX': 6.5,
    'Airbus A321-253NX': 6.5,
    'Airbus A330-202': 10.5,
    'Airbus A330-202': 10.5,
    'Airbus A330-243': 10.5,
    'Airbus A330-243': 10.5,
    'Airbus A330-243(F)': 10.5,
    'Airbus A330-243(F)': 10.5,
    'Airbus A330-302': 10.5,
    'Airbus A330-302': 10.5,
    'Airbus A330-900neo': 10.5,
    'Airbus A330-900neo': 10.5,
    'Airbus A350-941': 10.5,
    'Airbus A350-941': 10.5,
    'B190': 4.5,
    'B208B': 4.0,
    'B737': 7.0,
    'B737-8': 7.0,
    'B737-900ER': 7.8,
    'B738': 7.8,
    'B739': 8.0,
    'B741': 9.3,
    'B742': 10.0,
    'B744': 10.0,
    'B748': 10.5,
    'B763': 9.0,
    'B77F': 12.5,
    'B77L': 12.5,
    'Beech 1900C': 4.5,
    'Beech 1900C': 4.5,
    'Beech 1900C-1': 4.5,
    'Beech 1900C-1': 4.5,
    'Beech 1900D': 4.5,
    'Beech 1900D': 4.5,
    'Beech B200 Super King Air': 4.5,
    'Boeing 717': 7.0,
    'Boeing 737 MAX 8': 6.0,
    'Boeing 737 MAX 9/Boeing 737-9': 6.5,
    'Boeing 737-31BF': 7.0,
    'Boeing 737-330': 7.0,
    'Boeing 737-3Q8F': 7.0,
    'Boeing 737-436F': 7.5,
    'Boeing 737-700': 7.2,
    'Boeing 737-790': 7.5,
    'Boeing 737-790SF': 7.5,
    'Boeing 737-7B5 BBJ': 7.5,
    'Boeing 737-8': 7.0,
    'Boeing 737-800': 7.8,
    'Boeing 737-800WL': 7.8,
    'Boeing 737-824': 7.8,
    'Boeing 737-832': 7.8,
    'Boeing 737-852': 7.8,
    'Boeing 737-890': 7.5,
    'Boeing 737-8F2': 7.0,
    'Boeing 737-8FH': 7.0,
    'Boeing 737-9': 8.0,
    'Boeing 737-900': 7.8,
    'Boeing 737-900ER': 7.8,
    'Boeing 737-900WL': 7.8,
    'Boeing 737-924ER': 7.8,
    'Boeing 737-932ER': 8.0,
    'Boeing 737-990': 8.0,
    'Boeing 737-990ER': 8.0,
    'Boeing 737-9GPER': 8.0,
    'Boeing 747-400': 9.3,
    'Boeing 747-409F': 10.0,
    'Boeing 747-409LCF Dreamlifter': 10.0,
    'Boeing 747-412F': 10.0,
    'Boeing 747-412SF': 10.0,
    'Boeing 747-419SF': 10.0,
    'Boeing 747-422': 10.0,
    'Boeing 747-428ERF': 10.5,
    'Boeing 747-428ERF': 10.5,
    'Boeing 747-428F': 10.5,
    'Boeing 747-428SF': 10.0,
    'Boeing 747-443': 10.0,
    'Boeing 747-446F': 10.0,
    'Boeing 747-446SF': 10.0,
    'Boeing 747-44AF': 10.0,
    'Boeing 747-45EF': 10.0,
    'Boeing 747-45ESF': 10.0,
    'Boeing 747-467ERF': 10.5,
    'Boeing 747-46NF': 10.0,
    'Boeing 747-47UF': 10.0,
    'Boeing 747-481': 10.5,
    'Boeing 747-481F': 10.5,
    'Boeing 747-481SF': 10.5,
    'Boeing 747-48EF': 10.0,
    'Boeing 747-48ESF': 10.0,
    'Boeing 747-4B5': 10.0,
    'Boeing 747-4B5ERF': 10.0,
    'Boeing 747-4B5F': 10.0,
    'Boeing 747-4B5SF': 10.0,
    'Boeing 747-4EVERF': 10.5,
    'Boeing 747-4FTF': 10.0,
    'Boeing 747-4H6F': 10.0,
    'Boeing 747-4H6LCF Dreamlifter': 10.0,
    'Boeing 747-4H6SF': 10.0,
    'Boeing 747-4HAERF': 10.0,
    'Boeing 747-4HQERF': 10.0,
    'Boeing 747-4J6LCF Dreamlifter': 10.0,
    'Boeing 747-4KZF': 10.0,
    'Boeing 747-4R7F': 10.0,
    'Boeing 747-8': 10.5,
    'Boeing 747-867F': 10.5,
    'Boeing 747-87UF': 10.0,
    'Boeing 747-8B5F': 10.5,
    'Boeing 747-8F': 10.5,
    'Boeing 747-8HTF': 10.5,
    'Boeing 747-8KZF': 10.5,
    'Boeing 747-8R7F': 10.5,
    'Boeing 747-8U': 10.5,
    'Boeing 757-223': 8.0,
    'Boeing 757-231': 8.0,
    'Boeing 757-232': 8.0,
    'Boeing 757-236SF': 8.0,
    'Boeing 757-23ASF': 8.0,
    'Boeing 757-23N': 8.0,
    'Boeing 757-24ASF': 8.0,
    'Boeing 757-251': 8.0,
    'Boeing 757-256': 8.0,
    'Boeing 757-26D': 8.0,
    'Boeing 757-27BSF': 8.0,
    'Boeing 757-2B7': 8.0,
    'Boeing 757-2B7SF': 8.0,
    'Boeing 757-2Q8': 8.0,
    'Boeing 767-300F': 9.0,
    'Boeing 767-306ERSF': 9.0,
    'Boeing 767-31AER': 9.0,
    'Boeing 767-31BER': 9.0,
    'Boeing 767-323ERSF': 9.0,
    'Boeing 767-324ER': 9.0,
    'Boeing 767-332ER': 9.0,
    'Boeing 767-338ERSF': 9.0,
    'Boeing 767-34AERF': 9.0,
    'Boeing 767-36NER': 9.0,
    'Boeing 767-375ER': 9.0,
    'Boeing 767-37DERSF': 9.0,
    'Boeing 767-38EER': 9.0,
    'Boeing 767-3JHF': 9.0,
    'Boeing 767-3S1ER': 9.0,
    'Boeing 767-3S2F': 9.0,
    'Boeing 767-3Y0ERSF': 9.0,
    'Boeing 777-200LR / Boeing 777F': 12.5,
    'Boeing 777-300ER': 12.5,
    'Boeing 777-F': 12.5,
    'Boeing 777-F16': 12.5,
    'Boeing 777-F1B': 12.5,
    'Boeing 777-F1H': 12.5,
    'Boeing 777-F6N': 12.5,
    'Boeing 777-FB5': 12.5,
    'Boeing 777-FBT': 12.5,
    'Boeing 777-FEZ': 12.5,
    'Boeing 777-FFT': 10.5,
    'Boeing 777-FFX': 12.5,
    'Boeing 777-FHT': 12.5,
    'Boeing 777-FS2': 12.5,
    'Boeing 777-FZB': 12.5,
    'Boeing 77F': 12.5,
    'Boeing 77L': 12.5,
    'Boeing 787-8 BBJ': 9.0,
    'Boeing 787-8': 9.0,
    'Boeing 787-9': 9.5,
    'Bombardier BD-100-1A10 Challenger 300': 5.0,
    'Bombardier BD-100-1A10 Challenger 350': 5.0,
    'Bombardier BD-700-1A10 Global 6000': 5.0,
    'Bombardier BD-700-1A10 Global Express XRS': 5.0,
    'Bombardier BD-700-2A12 Global 7500': 5.0,
    'C208': 4.0,
    'CASA 212-200': 4.5,
    'CASA 212-200CB': 4.5,
    'CASA C-212-CC Aviocar 200': 4.5,
    'Cessna 208B Grand Caravan EX': 4.0,
    'Cessna 208b Grand Caravan': 4.0,
    'Cessna 208b Grand Caravan': 4.0,
    'Cessna 208B Super Cargomaster': 4.0,
    'Cessna 408 SkyCourier': 4.0,
    'DC93': 8.0,
    'De Havilland Canada DHC-8-100 Dash 8 / 8Q': 5.0,
    'De Havilland Canada DHC-8-102 Dash 8': 5.0,
    'De Havilland Canada DHC-8-102A Dash 8': 5.0,
    'De Havilland Canada DHC-8-103 Dash 8': 5.0,
    'De Havilland Canada DHC-8-106 Dash 8': 5.0,
    'De Havilland Canada DHC-8-Q402 Dash 8': 5.0,
    'DH8': 5.0,
    'DH8A': 5.0,
    'DH8D': 5.0,
    'Diamond DA 42 Twin Star': 3.5,
    'Douglas C-118A': 8.0,
    'Douglas DC-6B': 8.0,
    'E75L': 5.5,
    'Embraer 170-200LR-175LR': 5.5,
    'Embraer 175 (long wing)': 5.5,
    'Embraer 190-100AR': 6.0,
    'Embraer 190-100LR': 6.0,
    'Embraer EMB 545 Legacy 450': 6.0,
    'Embraer EMB 550 Legacy 500': 6.0,
    'Embraer Praetor 600': 6.0,
    'Eurocopter AS350 B2 AStar': 4.5,
    'Eurocopter EC135 P2+': 4.5,
    'Fokker 100': 6.0,
    'GLF4': 5.0,
    'Gulfstream Aerospace GV': 5.0,
    'Gulfstream Aerospace GV-SP (G550)': 5.0,
    'Gulfstream Aerospace GVI (G650ER)': 5.0,
    'Learjet 35A': 4.0,
    'Learjet 60': 4.5,
    'Lockheed 100-30 Hercules': 12.0,
    'Lockheed L-182 / 282 / 382 (L-100) Hercules': 12.0,
    'McDonnell Douglas MD-11F': 11.0,
    'McDonnell Douglas MD-82SF': 8.0,
    'McDonnell Douglas MD-83SF': 8.0,
    'MD11': 12.0,
    'MD82': 8.0,
    'MD83': 8.0,
    'Pilatus PC-12': 3.5,
    'Pilatus PC-12/45': 3.5,
    'Piper PA-24-250 Comanche 250': 4.5,
    'Piper PA-31-350 Navajo Chieftain': 4.5, 
    'Saab 2000': 5.0,
    'Saab 340A': 5.0,
    'Saab 340A(F)': 5.0,
    'SB20': 5.0,
    'SF34': 5.0,
    'Sikorsky S-92A': 4.5,
    'Unknown': 0.0
}

# Extract values for calculation
boeing_values = [value for key, value in co2_emissions_per_km.items() if "Boeing" in key]
airbus_values = [value for key, value in co2_emissions_per_km.items() if "Airbus" in key]
non_boeing_airbus_values = [value for key, value in co2_emissions_per_km.items() if "Boeing" not in key and "Airbus" not in key]

# Calculate averages
average_boeing = np.mean(boeing_values)
average_airbus = np.mean(airbus_values)
average_non_boeing_airbus = np.mean(non_boeing_airbus_values)

average_boeing, average_airbus, average_non_boeing_airbus

# Function to calculate CO2 emissions for a flight
def calculate_co2_emission(flight, flight_type="departure"):
    if flight_type == "arrival":
        dep_iata = flight['Origin'].split('(')[1].split(' / ')[0]
        dest_iata = 'ANC'
    else:
        dep_iata = 'ANC'
        try:
            dest_iata = flight['Destination'].split('(')[1].split(' / ')[0]
        except IndexError:
            return 'Unknown'
    
    dep_lat, dep_lon = get_airport_coords(dep_iata)
    dest_lat, dest_lon = get_airport_coords(dest_iata)
    
    if dep_lat is None or dest_lat is None:
        print(f"Could not find coordinates for airports: {dep_iata} or {dest_iata}")
        return 'Unknown'
    
    distance = haversine(dep_lat, dep_lon, dest_lat, dest_lon)
    aircraft_model = flight['Aircraft Info']
    
    co2_per_km = co2_emissions_per_km.get(aircraft_model, None)
    if co2_per_km is None:
        print(f"CO2 emissions data not found for aircraft model: {aircraft_model}. Assigning temporary average value.")
        
        # Check and add missing models to the file without duplicates and ignoring "nan"
        missing_file_path = 'missing_aircraft_models.txt'
        
        if os.path.exists(missing_file_path):
            with open(missing_file_path, 'r') as f:
                existing_models = set(line.strip() for line in f)
        else:
            existing_models = set()
        
        if pd.notna(aircraft_model) and aircraft_model not in existing_models:
            with open(missing_file_path, 'a') as f:
                f.write(f"{aircraft_model}\n")
        
        # Assign temporary average value
        if "Boeing" in aircraft_model or re.match(r'B\d+', aircraft_model):
            co2_per_km = average_boeing
        elif "Airbus" in aircraft_model:
            co2_per_km = average_airbus
        else:
            co2_per_km = average_non_boeing_airbus
    
    return round(distance * co2_per_km)

def filter_by_date(df, date):
    # Convert the date from '2024-06-21' to '23 Jun'
    target_date = datetime.strptime(date, '%Y-%m-%d').strftime('%d %b')
    
    # Extract the date part from the 'Date & Status' column and convert to '23 Jun'
    df['Extracted Date'] = df['Date & Status'].apply(lambda x: ' '.join(x.split()[:2]))
    
    # Filter the DataFrame to keep only rows where the extracted date matches the specified date
    filtered_df = df[df['Extracted Date'] == target_date]
    
    # Drop the temporary 'Extracted Date' column
    filtered_df = filtered_df.drop(columns=['Extracted Date'])
    
    return filtered_df

# Cleanup function to close the driver
def cleanup():
    if driver:
        driver.quit()

atexit.register(cleanup)

def scrape_flights(date, flight_type):
    global driver
    base_url = f'https://www.flightera.net/en/airport/Anchorage/PANC/{flight_type}/{date}'
    
    # Set up Chrome options to suppress SSL errors and logging
    options = Options()
    options.add_argument('--ignore-certificate-errors')
    options.add_argument('--ignore-ssl-errors')
    options.add_argument('--disable-gpu')
    options.add_argument('--log-level=3')  # Suppress logs
    options.page_load_strategy = 'eager'  # Load page faster by waiting for document ready state
    
    retries = 3
    def initialize_driver():
        global driver
        for attempt in range(retries):
            try:
                driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=options)
                break
            except WebDriverException as e:
                if attempt < retries - 1:
                    time.sleep(3)  # wait for a few seconds before retrying
                else:
                    raise e

    initialize_driver()

    all_flights = []

    # Only 00_00 interval is active
    time_intervals = ["00_00", "02_00", "04_00", "06_00", "08_00", "10_00", "12_00", "14_00", "16_00", "18_00", "20_00", "22_00"]

    def load_page(url):
        driver.get(url)
        
    for interval in time_intervals:
        url = f"{base_url}%20{interval}?"

        page_thread = threading.Thread(target=load_page, args=(url,))
        page_thread.start()
        page_thread.join(timeout=150)

        if page_thread.is_alive():
            logger.warning(f"Timeout while loading interval {interval}. Restarting driver.")
            driver.quit()
            initialize_driver()
            continue  # Skip to the next interval

        try:
            WebDriverWait(driver, 30).until(
                EC.presence_of_element_located((By.CLASS_NAME, 'min-w-full'))
            )
        except TimeoutException:
            logger.warning(f"Timeout while loading interval {interval}")
            driver.quit()
            initialize_driver()
            continue  # Skip to the next interval

        # Parse the page content using BeautifulSoup
        try:
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            table = soup.find('table', {'class': 'min-w-full divide-y divide-gray-200 table-auto'})

            if table:
                rows = table.find('tbody').find_all('tr')
                for row in rows:
                    cols = row.find_all('td')
                    if len(cols) >= 4:  # Ensure there are enough columns
                        # Extract relevant elements
                        date_status_element = cols[0].find('span', {'class': 'whitespace-nowrap'})
                        status_element = cols[0].find('span', class_=lambda x: x and 'inline-flex items-center' in x)
                        flight_number_element = cols[1].find('a')
                        second_flight_number_element = flight_number_element.find_next('span', {'class': 'text-gray-700'})
                        location_element = cols[2].find('a')
                        airline_element = cols[1].find('span', {'class': 'whitespace-nowrap'})
                        
                        # Extract text from elements
                        date_status = date_status_element.text.strip() if date_status_element else "Unknown"
                        primary_flight_number = flight_number_element.text.strip() if flight_number_element else "Unknown"
                        flight_number = second_flight_number_element.text.strip() if second_flight_number_element else primary_flight_number
                        location = location_element.text.strip() if location_element else "Unknown"
                        airline = airline_element.text.strip() if airline_element else "Unknown"
                        status = status_element.text.strip() if status_element else "Unknown"

                        if flight_type == "arrival":
                            origin = location
                            flight_data = [date_status, primary_flight_number, flight_number, airline, origin, status]
                        else:
                            destination = location
                            flight_data = [date_status, primary_flight_number, flight_number, airline, destination, status]

                        all_flights.append(flight_data)
        except InvalidSessionIdException:
            logger.warning(f"Invalid session ID while processing interval {interval}. Restarting driver.")
            driver.quit()
            initialize_driver()
            continue  # Skip to the next interval
        except Exception as e:
            logger.error(f"Error while processing interval {interval}: {e}")
            driver.quit()
            initialize_driver()
            continue  # Skip to the next interval

        # Close the current tab after processing
        if len(driver.window_handles) > 1:
            driver.close()
            driver.switch_to.window(driver.window_handles[0])

    driver.quit()

    # Create DataFrame and remove rows with "Unknown" or "Cancelled" status
    if flight_type == "arrival":
        df = pd.DataFrame(all_flights, columns=['Date & Status', 'Primary Flight Number', 'Flight Number', 'Airline', 'Origin', 'Status'])
    else:
        df = pd.DataFrame(all_flights, columns=['Date & Status', 'Primary Flight Number', 'Flight Number', 'Airline', 'Destination', 'Status'])
    
    df = df[~df['Status'].str.lower().isin(['unknown', 'cancelled'])]  # Filter out rows with "Unknown" or "Cancelled" status
    df.drop_duplicates(inplace=True)  # Remove duplicate rows

    return df

# Function to get aircraft details from Radarbox using flight number with retry logic
def get_aircraft_details(flight_number, retries=3):
    url = f"https://www.radarbox.com/data/flights/{flight_number}"
    session = requests.Session()
    retry = Retry(total=retries, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)

    try:
        response = session.get(url)
        response.raise_for_status()  # Raise HTTPError for bad responses (4xx and 5xx)
        time.sleep(0.1)  # Adding a small delay between requests to avoid getting rate-limited
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to get data for {flight_number}: {e}")
        return flight_number, 'Unknown'

    soup = BeautifulSoup(response.text, 'html.parser')

    try:
        # Locate the div containing the aircraft model
        aircraft_info = soup.find('div', id='model')
        if aircraft_info:
            model_info = aircraft_info.get('title', 'Unknown')
            logger.info(f"Successfully fetched model for flight number {flight_number}: {model_info}")
            return flight_number, model_info
    except (AttributeError, IndexError) as e:
        logger.error(f"Error parsing details for {flight_number}: {e}")

    return flight_number, 'Unknown'

# Function to process flight numbers concurrently
def process_flight_numbers_concurrently(flight_numbers, max_workers=50):
    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_flight_number = {executor.submit(get_aircraft_details, flight_number): flight_number for flight_number in flight_numbers}
        for future in as_completed(future_to_flight_number):
            flight_number = future_to_flight_number[future]
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                logger.error(f"Exception for {flight_number}: {e}")
                results.append((flight_number, 'Unknown'))
    return results

# Main script to calculate CO2 emissions for an existing DataFrame
if __name__ == "__main__":
    # Specify the date for scraping flights
    date = sys.argv[1] # Date passed as a command-line argument
    df_arrivals = scrape_flights(date, "arrival")

    # Assuming the flight number column is named 'Primary Flight Number'
    flight_numbers_arrivals = df_arrivals['Primary Flight Number'].unique()

    # Process flight numbers concurrently
    results_arrivals = process_flight_numbers_concurrently(flight_numbers_arrivals)

    # Update DataFrame with results
    for flight_number, model in results_arrivals:
        df_arrivals.loc[df_arrivals['Primary Flight Number'] == flight_number, 'Aircraft Info'] = model

    # Calculate CO2 emissions for each flight
    df_arrivals['CO2 Emission (kg)'] = df_arrivals.apply(calculate_co2_emission, axis=1, flight_type="arrival")

    # Apply the filter_by_date function
    df_arrivals = filter_by_date(df_arrivals, date)

    output_file_arrivals = os.path.join(date_directory, f'{date}_arrivals.csv')
    df_arrivals.to_csv(output_file_arrivals, index=False)

    print(f"Updated data with CO2 emissions saved to {output_file_arrivals}")

    # Save the dataframe to a pickle file for easy loading later
    output_pickle_file_arrivals = os.path.join(date_directory, f'{date}_arrivals.pkl')
    df_arrivals.to_pickle(output_pickle_file_arrivals)
    print(f"Updated data with CO2 emissions saved to {output_pickle_file_arrivals}")

    print(df_arrivals)

    print("Process finished --- %s seconds ---" % (time.time() - start_time))
