import pandas as pd

# Define the list of files to be merged
files = [f'{i}_Departures.xlsx' for i in range(1, 13)]

# Define the columns to keep for departures
columns_to_keep = [
    'Carrier Code', 'Date (MM/DD/YYYY)', 'Flight Number', 'Tail Number', 'Destination Airport',
    'Scheduled departure time', 'Actual departure time', 'Scheduled elapsed time (Minutes)', 
    'Actual elapsed time (Minutes)', 'Departure delay (Minutes)', 'Wheels-off time', 
    'Taxi-Out time (Minutes)', 'Delay Carrier (Minutes)', 'Delay Weather (Minutes)', 
    'Delay National Aviation System (Minutes)', 'Delay Security (Minutes)', 
    'Delay Late Aircraft Arrival (Minutes)'
]

# Initialize an empty DataFrame to hold the merged data
merged_df = pd.DataFrame()

# Function to read and process each file
def read_and_process_file(file_path, columns_to_keep):
    try:
        # Read the Excel file
        df = pd.read_excel(file_path, engine='openpyxl')
        print(f"File {file_path} read successfully with shape: {df.shape}")

        # Find the row where the desired columns start
        header_row_index = None
        for i, row in df.iterrows():
            if set(columns_to_keep).issubset(set(row)):
                header_row_index = i
                break

        if header_row_index is None:
            raise ValueError(f"Desired columns not found in file {file_path}")

        # Read the file again from the correct header row
        df = pd.read_excel(file_path, engine='openpyxl', skiprows=header_row_index + 1)
        print(f"File {file_path} re-read from header row {header_row_index + 1} with shape: {df.shape}")
        
        # Normalize column names: strip whitespace and convert to the specified case
        df.columns = df.columns.str.strip()
        print(f"Columns after normalization: {df.columns.tolist()}")
        
        # Select only the columns to keep
        df = df[columns_to_keep]
        return df
    except Exception as e:
        print(f"Error processing file {file_path}: {e}")
        return pd.DataFrame()

# Loop through each file and append the relevant data to the merged DataFrame
for file in files:
    df = read_and_process_file(file, columns_to_keep)
    if not df.empty:
        merged_df = pd.concat([merged_df, df], ignore_index=True)
    else:
        print(f"No data added for file {file}")

# Save the merged DataFrame to a new Excel file
output_file = 'Merged_Departures.xlsx'
try:
    merged_df.to_excel(output_file, index=False)
    print(f"Merged file saved to {output_file}")
except Exception as e:
    print(f"Error saving merged file: {e}")
