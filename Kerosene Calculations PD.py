import pandas as pd
from geopy.distance import geodesic
import streamlit as st
import plotly.graph_objects as go
import matplotlib.pyplot as plt

def draw_barrel(total_reduction, total_original_emissions):
    fig, ax = plt.subplots(figsize=(8, 6))
    
    # Draw the barrel with reduced height
    barrel_height = 6  # Reduced height by 40%
    barrel_width = 4
    ax.barh(1, total_original_emissions, height=barrel_height, color='#de4b40', label='Original Emissions')
    ax.barh(1, total_reduction, height=barrel_height, color='#4aa848', label='Reduction', left=0)
    
    ax.set_xlim(0, total_original_emissions + 10)
    ax.set_ylim(0, 2)
    
    ax.set_xlabel('Kilograms of CO2 Emissions')
    ax.set_title('CO2 Emissions Reduction with SAF')
    ax.legend(loc='best')
    
    ax.get_yaxis().set_visible(False)
    ax.get_xaxis().set_major_formatter(plt.FuncFormatter(lambda x, _: f'{int(x):,}'))
    
    st.pyplot(fig)


# Sample fuel consumption rates in liters per minute
fuel_consumption_rates = {
    '737-900ER': 50,  # Example values, replace with actual rates
    '737-790': 45,
    '737-9': 55,
    '737-990ER': 53,
    '737-890': 48,
    '737-8FH': 49,
    '737-990': 50,
    # Add other models as needed
}

# CO2 emissions factor for kerosene (kg CO2 per liter)
kerosene_emission_factor = 2.52

# Coordinates for ANC (Ted Stevens International Anchorage Airport)
anc_coords = (61.1743, -149.9983)

# Load the data
df_departures = pd.read_excel('Passengers_Departures_Updated.xlsx')
df_arrivals = pd.read_excel('Passengers_Arrivals_Updated.xlsx')  

# Ensure necessary columns are present
required_departure_columns = [
    'Carrier Code', 'Date (MM/DD/YYYY)', 'Flight Number', 'Tail Number', 'Destination Airport',
    'Scheduled departure time', 'Actual departure time', 'Scheduled elapsed time (Minutes)',
    'Actual elapsed time (Minutes)', 'Departure delay (Minutes)', 'Wheels-off time',
    'Taxi-Out time (Minutes)', 'Delay Carrier (Minutes)', 'Delay Weather (Minutes)',
    'Delay National Aviation System (Minutes)', 'Delay Security (Minutes)',
    'Delay Late Aircraft Arrival (Minutes)', 'Manufacturer', 'Model'
]
required_arrival_columns = [
    'Carrier Code', 'Date (MM/DD/YYYY)', 'Flight Number', 'Tail Number', 'Origin Airport',
    'Scheduled Arrival Time', 'Actual Arrival Time', 'Scheduled Elapsed Time (Minutes)',
    'Actual Elapsed Time (Minutes)', 'Arrival Delay (Minutes)', 'Wheels-on Time',
    'Taxi-In time (Minutes)', 'Delay Carrier (Minutes)', 'Delay Weather (Minutes)',
    'Delay National Aviation System (Minutes)', 'Delay Security (Minutes)',
    'Delay Late Aircraft Arrival (Minutes)', 'Manufacturer', 'Model'
]

if not all(col in df_departures.columns for col in required_departure_columns):
    raise ValueError(f"Departures DataFrame must contain the following columns: {required_departure_columns}")

if not all(col in df_arrivals.columns for col in required_arrival_columns):
    raise ValueError(f"Arrivals DataFrame must contain the following columns: {required_arrival_columns}")

# Strip whitespace from column names
df_departures.columns = df_departures.columns.str.strip()
df_arrivals.columns = df_arrivals.columns.str.strip()

# Define a dictionary for airport coordinates
airport_coords = {
    'ANC': (61.1743, -149.9982),
    'SEA': (47.4502, -122.3088),
    'JFK': (40.6413, -73.7781),
    'SFO': (37.6213, -122.3790),
    'LAX': (33.9416, -118.4085),
    'ORD': (41.9742, -87.9073),
    'DFW': (32.8998, -97.0403),
    'DEN': (39.8561, -104.6737),
    'IAH': (29.9902, -95.3368),
    'ATL': (33.6407, -84.4277),
    'MIA': (25.7959, -80.2870),
    'CLT': (35.2140, -80.9431),
    'PHX': (33.4373, -112.0078),
    'LAS': (36.0840, -115.1537),
    'PDX': (45.5898, -122.5951),
    'BOS': (42.3656, -71.0096),
    'MSP': (44.8820, -93.2218),
    'DTW': (42.2162, -83.3554),
    'FLL': (26.0726, -80.1527),
    'SAN': (32.7338, -117.1933),
    'MCO': (28.4312, -81.3081),
    'BWI': (39.1754, -76.6683),
    'TPA': (27.9773, -82.5310),
    'SLC': (40.7884, -111.9780),
    'STL': (38.7487, -90.3700),
    'BNA': (36.1263, -86.6774),
    'AUS': (30.1945, -97.6699),
    'CLE': (41.4109, -81.8499),
    'RDU': (35.8801, -78.7880),
    'MCI': (39.2976, -94.7139),
    'HNL': (21.3187, -157.9225),
    'OAK': (37.7126, -122.2197),
    'SJC': (37.3626, -121.9290),
    'SMF': (38.6951, -121.5919),
    'RNO': (39.4991, -119.7681),
    'BOI': (43.5644, -116.2228),
    'PHL': (39.8744, -75.2424),
    'TUS': (32.1161, -110.9410),
    'KOA': (19.7388, -156.0456),
    'LIH': (21.9750, -159.3380),
    'MDW': (41.7868, -87.7522),
    'BDL': (41.9389, -72.6832),
    'SAT': (29.5337, -98.4698),
    'SNA': (33.6757, -117.8678),
    'MSY': (29.9934, -90.2580),
    'BUR': (34.2006, -118.3585),
    'ABQ': (35.0494, -106.6172),
    'PBI': (26.6832, -80.0956),
    'SJU': (18.4394, -66.0018),
    'HOU': (29.6454, -95.2789),
    'PIT': (40.4915, -80.2329),
    'IND': (39.7169, -86.2956),
    'CMH': (39.9980, -82.8919),
    'MEM': (35.0425, -89.9767),
    'OGG': (20.8986, -156.4305),
    'ONT': (34.0556, -117.6000),
    'RIC': (37.5052, -77.3197),
    'BHM': (33.5629, -86.7535),
    'OMA': (41.2996, -95.8994),
    'TUL': (36.1984, -95.8881),
    # Add more airports as needed
}

# Function to calculate fuel consumption
def calculate_fuel_consumption(model, actual_elapsed_time):
    rate = fuel_consumption_rates.get(model, 0)  # Default to 0 if model not found
    return rate * actual_elapsed_time

# Function to calculate distance for departures
def calculate_distance(destination_airport):
    dest_coords = airport_coords.get(destination_airport)
    if dest_coords:
        return geodesic(anc_coords, dest_coords).kilometers
    else:
        return None

# Function to calculate distance for arrivals
def calculate_distance_arrival(origin_airport):
    origin_coords = airport_coords.get(origin_airport)
    if origin_coords:
        return geodesic(anc_coords, origin_coords).kilometers
    else:
        return None

# Calculate fuel consumption and add to DataFrame
df_departures['Fuel Consumption (L)'] = df_departures.apply(lambda row: calculate_fuel_consumption(row['Model'], row['Actual elapsed time (Minutes)']), axis=1)
df_arrivals['Fuel Consumption (L)'] = df_arrivals.apply(lambda row: calculate_fuel_consumption(row['Model'], row['Actual Elapsed Time (Minutes)']), axis=1)

# Calculate CO2 emissions and add to DataFrame
df_departures['CO2 Emissions (kg)'] = df_departures['Fuel Consumption (L)'] * kerosene_emission_factor
df_arrivals['CO2 Emissions (kg)'] = df_arrivals['Fuel Consumption (L)'] * kerosene_emission_factor

# Calculate distances to destination airports for departures
df_departures['Distance (km)'] = df_departures['Destination Airport'].apply(calculate_distance)
# Calculate distances to origin airports for arrivals
df_arrivals['Distance (km)'] = df_arrivals['Origin Airport'].apply(calculate_distance_arrival)

# Function to calculate reduced emissions
def calculate_reduced_emissions(saf_percentage, fuel_consumption):
    saf_factor = saf_percentage / 100  # Correct the SAF factor calculation
    kerosene_factor = 1 - saf_factor
    return fuel_consumption * kerosene_factor * kerosene_emission_factor







#Streamlit 









st.title('Fuel Usage and Carbon Emissions Tracker')

# Checkboxes for Departures and Arrivals
show_departures = st.checkbox('Show Departures', value=True, key="departures_checkbox")
show_arrivals = st.checkbox('Show Arrivals', value=False, key="arrivals_checkbox")

# JavaScript to handle the click events on the slider
st.markdown(f"""
    <script>
        document.addEventListener('DOMContentLoaded', function() {{
            const slider = document.querySelector('div[data-baseweb="slider"] > div');
            slider.addEventListener('click', function(event) {{
                const rect = slider.getBoundingClientRect();
                const offsetX = event.clientX - rect.left;
                const percentage = Math.round((offsetX / rect.width) * 100);
                const streamlitInput = document.querySelector('input[role="slider"]');
                streamlitInput.value = percentage;
                streamlitInput.dispatchEvent(new Event('change', {{ bubbles: true }}));
            }});
        }});
    </script>
""", unsafe_allow_html=True)

# SAF percentage slider
if show_departures and show_arrivals:
    saf_percentage = st.slider('Sustainable Aviation Fuel Percentage (Departures and Arrivals)', 0, 100, 50, key="combined_slider")
else:
    if show_departures:
        saf_percentage_departures = st.slider('Sustainable Aviation Fuel Percentage (Departures)', 0, 100, 50, key="departures_slider")
    if show_arrivals:
        saf_percentage_arrivals = st.slider('Sustainable Aviation Fuel Percentage (Arrivals)', 0, 100, 50, key="arrivals_slider")

if show_departures and show_arrivals:
    # Sync percentage with combined slider
    saf_percentage_departures = saf_percentage
    saf_percentage_arrivals = saf_percentage

    # Calculate reduced emissions for Departures
    df_departures['Reduced CO2 Emissions (kg)'] = df_departures.apply(lambda row: calculate_reduced_emissions(saf_percentage_departures, row['Fuel Consumption (L)']), axis=1)
    # Calculate reduced emissions for Arrivals
    df_arrivals['Reduced CO2 Emissions (kg)'] = df_arrivals.apply(lambda row: calculate_reduced_emissions(saf_percentage_arrivals, row['Fuel Consumption (L)']), axis=1)
    
    # Calculate combined total emissions reduction
    total_original_emissions_departures = df_departures['CO2 Emissions (kg)'].sum()
    total_reduced_emissions_departures = df_departures['Reduced CO2 Emissions (kg)'].sum()
    reduction_departures = total_original_emissions_departures - total_reduced_emissions_departures

    total_original_emissions_arrivals = df_arrivals['CO2 Emissions (kg)'].sum()
    total_reduced_emissions_arrivals = df_arrivals['Reduced CO2 Emissions (kg)'].sum()
    reduction_arrivals = total_original_emissions_arrivals - total_reduced_emissions_arrivals

    total_original_emissions_combined = total_original_emissions_departures + total_original_emissions_arrivals
    total_reduced_emissions_combined = total_reduced_emissions_departures + total_reduced_emissions_arrivals
    reduction_combined = reduction_departures + reduction_arrivals

    st.write(f'Total CO2 Emissions Reduction (Combined): {reduction_combined:.2f} kg')

    # Draw the combined barrel graphic with Matplotlib
    draw_barrel(reduction_combined, total_original_emissions_combined)

    # Display the data for Departures and Arrivals
    st.dataframe(df_departures)
    st.dataframe(df_arrivals)

else:
    if show_departures:
        # Calculate reduced emissions for Departures
        df_departures['Reduced CO2 Emissions (kg)'] = df_departures.apply(lambda row: calculate_reduced_emissions(saf_percentage_departures, row['Fuel Consumption (L)']), axis=1)
        
        # Show total emissions reduction for Departures
        total_original_emissions_departures = df_departures['CO2 Emissions (kg)'].sum()
        total_reduced_emissions_departures = df_departures['Reduced CO2 Emissions (kg)'].sum()
        reduction_departures = total_original_emissions_departures - total_reduced_emissions_departures

        st.write(f'Total CO2 Emissions Reduction (Departures): {reduction_departures:.2f} kg')

        # Draw the barrel graphic with Matplotlib for Departures
        draw_barrel(reduction_departures, total_original_emissions_departures)

        # Display the data for Departures
        st.dataframe(df_departures)

    if show_arrivals:
        # Calculate reduced emissions for Arrivals
        df_arrivals['Reduced CO2 Emissions (kg)'] = df_arrivals.apply(lambda row: calculate_reduced_emissions(saf_percentage_arrivals, row['Fuel Consumption (L)']), axis=1)
        
        # Show total emissions reduction for Arrivals
        total_original_emissions_arrivals = df_arrivals['CO2 Emissions (kg)'].sum()
        total_reduced_emissions_arrivals = df_arrivals['Reduced CO2 Emissions (kg)'].sum()
        reduction_arrivals = total_original_emissions_arrivals - total_reduced_emissions_arrivals

        st.write(f'Total CO2 Emissions Reduction (Arrivals): {reduction_arrivals:.2f} kg')

        # Draw the barrel graphic with Matplotlib for Arrivals
        draw_barrel(reduction_arrivals, total_original_emissions_arrivals)

        # Display the data for Arrivals
        st.dataframe(df_arrivals)