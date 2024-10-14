import os
import csv
import time
from flask import Flask, request, jsonify
from numba import njit, prange, typed, types
from collections import Counter
from concurrent.futures import ProcessPoolExecutor

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

@njit(parallel=True)
def compute_avg_aqi(aqi_values):
    total_aqi = 0.0
    count_aqi = 0

    for i in prange(len(aqi_values)):
        aqi = aqi_values[i]
        total_aqi += aqi
        count_aqi += 1

    avg_aqi = total_aqi / count_aqi if count_aqi > 0 else 0.0
    return avg_aqi

def compute_freq(values):
    return Counter(values)

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
                    rows = [row for row in csv_reader]
                
                if len(rows[0]) != len(expected_headers):
                    return jsonify({"error": f"Column length mismatch in file {file}. Expected {len(expected_headers)} columns, found {len(rows[0])} columns."}), 400
                
                data_frames.extend(rows)
            
            combined_data = [expected_headers] + data_frames

            aqi_values = [0 if int(row[7]) == -999 else int(row[7]) for row in combined_data[1:]]
            site_name_values = [str(row[9]) for row in combined_data[1:]]
            site_agency_values = [str(row[10]) for row in combined_data[1:]]
            parameter_values = [str(row[3]) for row in combined_data[1:]]

            with ProcessPoolExecutor() as executor:
                future_site_name_freq = executor.submit(compute_freq, site_name_values)
                future_site_agency_freq = executor.submit(compute_freq, site_agency_values)
                future_parameter_freq = executor.submit(compute_freq, parameter_values)

                site_name_freq = future_site_name_freq.result()
                site_agency_freq = future_site_agency_freq.result()
                parameter_freq = future_parameter_freq.result()

            avg_aqi = compute_avg_aqi(aqi_values)

            folder_summaries[folder] = {
                "avg_aqi": avg_aqi,
                "site_name_freq": dict(site_name_freq),
                "site_agency_freq": dict(site_agency_freq),
                "parameter_freq": dict(parameter_freq)
            }
        
        end_time = time.time()
        elapsed_time = end_time - start_time
        return jsonify({"processing_time": elapsed_time,"folder_summaries": folder_summaries })
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=False)