import os
import csv
import time
from flask import Flask, request, jsonify
from numba import njit, prange, typed, types
from numba.typed import List

app = Flask(__name__)

DATA_DIR = '../DATA/AirNow fires/fire-2020-full-data/data'
EXPORT_DIR = '../../'

def get_csv_files_in_date_range(start_date, end_date):
    csv_files_by_folder = {}
    for folder_name in os.listdir(DATA_DIR):
        folder_path = os.path.join(DATA_DIR, folder_name)
        if os.path.isdir(folder_path):
            csv_files = []
            for file in os.listdir(folder_path):
                if file.endswith('.csv'):
                    file_date = file.split('.')[0]
                    if start_date <= file_date <= end_date:
                        csv_files.append(os.path.join(folder_path, file))
            if csv_files:
                csv_files_by_folder[folder_name] = csv_files
    return csv_files_by_folder

def read_csv_file(file_path):
    aqi_values = []
    site_name_values = []
    site_agency_values = []
    parameter_values = []

    with open(file_path, mode='r') as file:
        csv_reader = csv.reader(file)
        header = next(csv_reader)  # Skip the header row
        for row in csv_reader:
            aqi_values.append(float(row[0]))  # Assuming AQI is in the first column
            site_name_values.append(row[1])  # Assuming Site Name is in the second column
            site_agency_values.append(row[2])  # Assuming Site Agency is in the third column
            parameter_values.append(row[3])  # Assuming Parameter is in the fourth column

    return aqi_values, site_name_values, site_agency_values, parameter_values

@njit(parallel=True)
def compute_dataframe(aqi_values, site_name_values, site_agency_values, parameter_values):
    total_aqi = 0.0
    count_aqi = 0
    site_name_freq = typed.Dict.empty(key_type=types.unicode_type, value_type=types.int64)
    site_agency_freq = typed.Dict.empty(key_type=types.unicode_type, value_type=types.int64)
    parameter_freq = typed.Dict.empty(key_type=types.unicode_type, value_type=types.int64)

    for i in prange(len(aqi_values)):
        aqi = aqi_values[i]
        site_name = site_name_values[i]
        site_agency = site_agency_values[i]
        parameter = parameter_values[i]

        total_aqi += aqi
        count_aqi += 1

        if site_name in site_name_freq:
            site_name_freq[site_name] += 1
        else:
            site_name_freq[site_name] = 1

        if site_agency in site_agency_freq:
            site_agency_freq[site_agency] += 1
        else:
            site_agency_freq[site_agency] = 1

        if parameter in parameter_freq:
            parameter_freq[parameter] += 1
        else:
            parameter_freq[parameter] = 1

    return total_aqi, count_aqi, site_name_freq, site_agency_freq, parameter_freq

@app.route('/process_batch_csv', methods=['GET'])
def process_batch_csv():
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    start_time = time.time()

    if not start_date or not end_date:
        return jsonify({"error": "Please provide both start_date and end_date in format YYYYMMDD"}), 400
    
    try:
        csv_files_by_folder = get_csv_files_in_date_range(start_date, end_date)
        
        if not csv_files_by_folder:
            return jsonify({"message": "No files found in the provided date range."})
        
        folder_summaries = {}
        
        expected_headers = [
            'Latitude', 'Longitude', 'Time', 'Parameter', 'Concentration', 
            'Unit', 'Raw-Concentration', 'AQI', 'Category', 'Site-name', 
            'Site-agency', 'AQS-ID', 'Full_AQS-ID'
        ]
        
        for folder, csv_files in csv_files_by_folder.items():
            
            data_frames = []
            for file in csv_files:
                with open(file, mode='r') as f:
                    csv_reader = csv.reader(f)
                    header = next(csv_reader)  # Skip the header row
                    rows = [row for row in csv_reader]
                
                if len(rows[0]) != len(expected_headers):
                    return jsonify({"error": f"Column length mismatch in file {file}. Expected {len(expected_headers)} columns, found {len(rows[0])} columns."}), 400
                
                data_frames.extend(rows)
            
            combined_data = [expected_headers] + data_frames

            # Process combined_data as needed
            # For example, convert to numeric and compute summaries
            aqi_values = [float(row[7]) for row in combined_data[1:]]  # Assuming AQI is in the 8th column
            site_name_values = [row[9] for row in combined_data[1:]]  # Assuming Site Name is in the 10th column
            site_agency_values = [row[10] for row in combined_data[1:]]  # Assuming Site Agency is in the 11th column
            parameter_values = [row[3] for row in combined_data[1:]]  # Assuming Parameter is in the 4th column

            total_aqi, count_aqi, site_name_freq, site_agency_freq, parameter_freq = compute_dataframe(
                List(aqi_values), List(site_name_values), List(site_agency_values), List(parameter_values)
            )

            folder_summaries[folder] = {
                "total_aqi": total_aqi,
                "count_aqi": count_aqi,
                "site_name_freq": dict(site_name_freq),
                "site_agency_freq": dict(site_agency_freq),
                "parameter_freq": dict(parameter_freq)
            }
        
        end_time = time.time()
        elapsed_time = end_time - start_time
        return jsonify({"folder_summaries": folder_summaries, "processing_time": elapsed_time})
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)