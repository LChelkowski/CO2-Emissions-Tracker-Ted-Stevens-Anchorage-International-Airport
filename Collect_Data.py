import subprocess
import pandas as pd
import time
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import sys

def generate_date_range(start_date, end_date):
    return pd.date_range(start_date, end_date)

def run_script(script_name, date, date_directory):
    try:
        result = subprocess.run([sys.executable, script_name, date, date_directory], check=True, capture_output=True, text=True)
        print(result.stdout)
        return script_name, None
    except subprocess.CalledProcessError as e:
        print(f"Error running {script_name} for date {date}: {e}")
        print(e.stdout)  # Print standard output
        print(e.stderr)  # Print error output
        return script_name, e

def collect_data(start_date, end_date):
    dates = generate_date_range(start_date, end_date)
    start_time = time.time()

    for date in dates:
        date_str = date.strftime('%Y-%m-%d')
        date_directory = f"./data/{date_str}"
        
        # Create the directory for the specified date if it doesn't exist
        os.makedirs(date_directory, exist_ok=True)

        # Run arrivals.py and departures.py concurrently
        scripts = ["arrivals.py", "departures.py"]
        with ThreadPoolExecutor() as executor:
            futures = [executor.submit(run_script, script, date_str, date_directory) for script in scripts]
            for future in as_completed(futures):
                script_name, error = future.result()
                if error:
                    print(f"Script {script_name} failed with error: {error}")

        # Read the resulting CSV files
        try:
            arrivals_df = pd.read_csv(os.path.join(date_directory, f"{date_str}_arrivals.csv"))
            departures_df = pd.read_csv(os.path.join(date_directory, f"{date_str}_departures.csv"))

            # Combine the dataframes
            combined_df = pd.concat([arrivals_df, departures_df])

            # Save the combined dataframe to a new CSV file
            combined_output_file = os.path.join(date_directory, f"{date_str}_combined.csv")
            combined_df.to_csv(combined_output_file, index=False)

            print(f"Combined data saved to {combined_output_file}")
            print(combined_df)

            # Save the combined dataframe to a pickle file for easy loading later
            combined_pickle_file = os.path.join(date_directory, f"{date_str}_combined.pkl")
            combined_df.to_pickle(combined_pickle_file)
            print(f"Combined data saved to {combined_pickle_file}")

        except FileNotFoundError as e:
            print(f"Error: {e}")

    print("Process finished --- %s seconds ---" % (time.time() - start_time))

if __name__ == "__main__":
    start_date = '2023-01-10'
    end_date = '2023-01-20'
    collect_data(start_date, end_date)
