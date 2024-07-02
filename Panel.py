import panel as pn
import pandas as pd
import pickle
import os
import requests 
import dask.dataframe as dd
from datetime import date
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def download_file_from_github(repo, path, save_as):
    url = f"https://github.com/LChelkowski/CO2-Emissions-Tracker-Ted-Stevens-Anchorage-International-Airport/tree/master/data"
    response = requests.get(url)
    if response.status_code == 200:
        os.makedirs(os.path.dirname(save_as), exist_ok=True)
        with open(save_as, 'wb') as f:
            f.write(response.content)
        print(f"Downloaded {save_as} from GitHub")
    else:
        print(f"Failed to download {path} from GitHub: {response.status_code}")



# Initialize Panel extension
pn.extension(sizing_mode="stretch_width", theme="dark")

# Load custom CSS
pn.config.raw_css.append("""
body {
    background-color: #2e2e2e; /* Dark background */
    color: #e0e0e0; /* Light text color */
    font-family: 'Arial', sans-serif;
    margin: 0;
    padding: 0;
}

.bk-root .bk-btn, .bk-root .bk-slider, .bk-root .bk-input {
    background-color: #4caf50; /* Light green */
    color: #ffffff;
    border-color: #4caf50;
    border-radius: 5px;
    font-size: 14px;
}

.bk-root .bk-btn:hover, .bk-root .bk-slider:hover, .bk-root .bk-input:hover {
    background-color: #388e3c; /* Darker green */
    border-color: #388e3c;
}

.bk-root .bk-panel, .bk-root .bk-markdown {
    background-color: #3e3e3e; /* Darker panel background */
    color: #e0e0e0; /* Light text color */
    border-radius: 5px;
    padding: 10px;
}

.bk-root .bk-dataframe {
    background-color: #3e3e3e; /* Uniform dataframe background */
    color: #e0e0e0; /* Light text color */
    border-color: #1b5e20; /* Dark green border */
    border-radius: 5px;
    width: 100% !important; /* Ensure full width */
}

/* Ensure uniform background for DataFrame rows */
.bk-root .bk-dataframe .slick-row {
    background-color: #3e3e3e !important; /* Uniform row background */
    color: #e0e0e0 !important; /* Light text color */
}

.bk-root .bk-slider-title, .bk-root .bk-input-title {
    color: #e0e0e0; /* Light text color */
    font-weight: bold;
}

/* Custom class to extend the button and center the text */
.full-width-btn {
    width: 100% !important;
    text-align: center !important;
    font-size: 20px !important;
    padding: 15px !important;
}

/* Custom class for smaller bubble */
.small-bubble {
    display: inline-block;
    padding: 10px;
    border-radius: 5px;
    background-color: #3e3e3e;
    color: #e0e0e0;
    text-align: center;
    margin: auto;
    width: 50%;
    margin-top: 10px;
}
.center {
    display: flex;
    justify-content: center;
    align-items: center;
    width: 100%;
}
""")

# Directory where data is stored
data_directory = 'data'


# Load pickle files function with row limit and unique indexing
def load_pickle_files_dask(date_range_list, row_limit=None):
    df_list = []
    total_rows = 0
    repo = "LChelkowski/CO2-Emissions-Tracker-Ted-Stevens-Anchorage-International-Airport"
    for date in date_range_list:
        combined_file = os.path.join(data_directory, f"{date}", f"{date}_combined.pkl")
        if not os.path.exists(combined_file):
            # Download the file from GitHub if it doesn't exist
            download_file_from_github(repo, f"data/{date}/{date}_combined.pkl", combined_file)
        if os.path.exists(combined_file):
            with open(combined_file, 'rb') as f:
                df = pickle.load(f)
                if row_limit is not None and total_rows + len(df) > row_limit:
                    remaining_rows = row_limit - total_rows
                    df_list.append(df.iloc[:remaining_rows])
                    break
                else:
                    df_list.append(df)
                    total_rows += len(df)
    if df_list:
        combined_df = pd.concat(df_list)
        combined_df.reset_index(drop=True, inplace=True)  # Ensure unique indexing
        combined_df = combined_df.sort_index()  # Sort by index to maintain order
        return dd.from_pandas(combined_df, npartitions=4)  # Convert to Dask DataFrame
    else:
        return None


# CO2 emission reduction based on SAF percentage
def calculate_saf_reduction(df, saf_percentage):
    reduction_factor = saf_percentage / 100
    df['CO2 Emission (kg)'] = pd.to_numeric(df['CO2 Emission (kg)'], errors='coerce')
    df['Reduced CO2 Emission (metric tons)'] = df['CO2 Emission (kg)'] * reduction_factor / 1000
    df['CO2 Emission (metric tons)'] = df['CO2 Emission (kg)'] / 1000
    return df

# Global variable to keep track of the current number of rows
current_rows_display = 10000
combined_df_all = None

# Widgets
start_date_picker = pn.widgets.DatePicker(name='Start date', value=date(2023, 1, 1), start=date(2018, 1, 1))
end_date_picker = pn.widgets.DatePicker(name='End date', value=date(2023, 12, 31), start=date(2018, 1, 1))
saf_slider = pn.widgets.IntSlider(name='Select SAF percentage', start=0, end=100, value=20)
saf_input = pn.widgets.IntInput(name='SAF percentage', value=20, start=0, end=100)
file_path_input = pn.widgets.TextInput(name='File path', placeholder='Enter file path and name...')
increase_rows_button = pn.widgets.Button(name='Increase by 5000 rows', button_type='success', css_classes=['full-width-btn'])

# Sync the slider and input box
def sync_saf_slider(event):
    saf_input.value = event.new

def sync_saf_input(event):
    saf_slider.value = event.new

saf_slider.param.watch(sync_saf_slider, 'value')
saf_input.param.watch(sync_saf_input, 'value')

# Function to increase the number of rows
def increase_rows(event):
    global current_rows_display
    current_rows_display = min(current_rows_display + 5000, 20000)
    if current_rows_display >= 20000:
        increase_rows_button.disabled = True
    update_data()

increase_rows_button.on_click(increase_rows)

def update_data(event=None):
    global combined_df_all, current_rows_display
    start_date = start_date_picker.value
    end_date = end_date_picker.value
    saf_percentage = saf_slider.value
    row_limit = current_rows_display
    
    date_range_list = pd.date_range(start_date, end_date).strftime("%Y-%m-%d").tolist()
    
    # Load and process all data within the date range for calculations
    combined_df_all = load_pickle_files_dask(date_range_list)
    
    if combined_df_all is not None:
        combined_df_all = combined_df_all.compute()
        combined_df_all = calculate_saf_reduction(combined_df_all, saf_percentage)
        
        # Calculate totals for the entire dataset
        total_co2_emission = combined_df_all['CO2 Emission (metric tons)'].sum()
        total_saf_reduction = combined_df_all['Reduced CO2 Emission (metric tons)'].sum()
        median_co2 = combined_df_all['CO2 Emission (metric tons)'].median()
        
        top_origins = combined_df_all['Origin'].value_counts().nlargest(3).index.tolist()
        top_destinations = combined_df_all['Destination'].value_counts().nlargest(3).index.tolist()
        top_airlines = combined_df_all['Airline'].value_counts().nlargest(5)
        
        # Load and process limited data for display
        display_df = load_pickle_files_dask(date_range_list, row_limit=row_limit)
        if display_df is not None:
            display_df = display_df.compute()
            display_df = calculate_saf_reduction(display_df, saf_percentage)
        
        update_panel.objects = [
            pn.Row(
                pn.pane.HTML(f"<div style='background-color: #3e3e3e; color: #e0e0e0; padding: 10px; border-radius: 5px; width: 100%;'>Total CO2 Emissions: {total_co2_emission:,.2f} metric tons</div>"),
                pn.pane.HTML(f"<div style='background-color: #3e3e3e; color: #e0e0e0; padding: 10px; border-radius: 5px; width: 100%;'>Total SAF Reduction: {total_saf_reduction:,.2f} metric tons</div>")
            ),
            pn.Row(
                pn.pane.HTML(
                    f"<div class='center'><div class='small-bubble'>Median CO2 Emissions: {median_co2:,.2f} metric tons</div></div>"
                )
            ),
            pn.Row(
                pn.pane.HTML(f"<div style='background-color: #3e3e3e; color: #e0e0e0; padding: 10px; border-radius: 5px;'>Top 3 Origins: {', '.join(top_origins)}</div>"),
                pn.pane.HTML(f"<div style='background-color: #3e3e3e; color: #e0e0e0; padding: 10px; border-radius: 5px;'>Top 3 Destinations: {', '.join(top_destinations)}</div>")
            ),
            pn.Row(
                pn.pane.HTML(
                    f"<div style='background-color: #3e3e3e; color: #e0e0e0; padding: 10px; border-radius: 5px; text-align: center;'>"
                    f"<strong style='font-size: 18px;'>Top 5 Airlines:</strong> "
                    + " ".join([f"<span style='font-size: 16px; text-decoration: underline;'>{airline}</span>: {count}" for airline, count in top_airlines.items()])
                    + "</div>"
                )
            ),
            # Remove the following line
            # pn.Row(export_csv_button, export_message, export_excel_button),
            pn.pane.DataFrame(display_df, sizing_mode='stretch_both')
        ]

    else:
        update_panel.objects = [
            pn.pane.HTML("<div style='background-color: #3e3e3e; color: #e0e0e0; padding: 10px; border-radius: 5px;'>Data for the selected date range is not available.</div>")
        ]

    # Enable or disable the button based on the row limit
    if current_rows_display >= 20000:
        increase_rows_button.disabled = True
    else:
        increase_rows_button.disabled = False


# Watch changes on date pickers and SAF slider
start_date_picker.param.watch(update_data, 'value')
end_date_picker.param.watch(update_data, 'value')
saf_slider.param.watch(update_data, 'value')


# Layout
header = pn.pane.HTML("<h2 style='color: #4caf50; text-align: center;'>Anchorage Airport: Sustainable Aviation Fuel (SAF) CO2 Reduction Calculator</h2>")
widgets = pn.WidgetBox(
    pn.Row(start_date_picker, end_date_picker),
    pn.Row(saf_slider, saf_input),
    pn.Row(file_path_input),
    pn.Row(increase_rows_button),
    sizing_mode='stretch_width'
)
update_panel = pn.Column(sizing_mode='stretch_both')

main = pn.Column(
    header,
    widgets,
    update_panel,
    sizing_mode='stretch_both'
)

# Trigger initial data load
update_data()

# Serve the app
main.servable()
