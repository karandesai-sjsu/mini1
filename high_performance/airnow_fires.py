import os
import pandas as pd
import numpy as np
from numba import njit,prange
from flask import Flask, request, jsonify
import time

app = Flask(__name__)

DATA_DIR = '../DATA\AirNow fires/fire-2020-full-data/data' 
EXPORT_DIR = '../'

@njit(parallel=True)
def calculate_average_aqi(aqi_values):
    total = 0.0
    count = 0
    for i in prange(len(aqi_values)):
        if aqi_values[i] != -999:
            total += aqi_values[i]
            count += 1
    return total / count if count > 0 else 0

# function to get CSV files for a specific date range
def get_csv_files_in_date_range(start_date, end_date):
    csv_files_by_folder = {}
    for folder_name in os.listdir(DATA_DIR):
        folder_path = os.path.join(DATA_DIR, folder_name)
        if os.path.isdir(folder_path):
            csv_files = []
            for file in os.listdir(folder_path):
                if file.endswith('.csv'):
                    # Extract date from file name and check if it's in the range
                    file_date = file.split('.')[0]
                    if start_date <= file_date <= end_date:
                        csv_files.append(os.path.join(folder_path, file))
            if csv_files:
                csv_files_by_folder[folder_name] = csv_files
    return csv_files_by_folder

# function to get CSV files for a specific date
def get_csv_files_for_exact_date(date):
    csv_files = []
    # Check if folder name is the exact date
    if date.isdigit():
        folder_path = os.path.join(DATA_DIR, date)
        if os.path.exists(folder_path):
            # Add all CSV files from this folder
            for file in os.listdir(folder_path):
                if file.endswith('.csv'):
                    csv_files.append(os.path.join(folder_path, file))

    return csv_files

# API endpoint to process files based on the date range
@app.route('/process_batch_csv', methods=['GET'])
def process_batch_csv():
    # Get the start_date and end_date from request arguments
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    
    if not start_date or not end_date:
        return jsonify({"error": "Please provide both start_date and end_date in format YYYYMMDD"}), 400
    
    try:
        # Get list of CSV files in the date range
        csv_files_by_folder = get_csv_files_in_date_range(start_date, end_date)
        
        if not csv_files_by_folder:
            return jsonify({"message": "No files found in the provided date range."})
        
        folder_summaries = {}
        
        expected_headers = [
            'Latitude', 'Longitude', 'Time', 'Parameter', 'Concentration', 
            'Unit', 'Raw-Concentration', 'AQI', 'Category', 'Site-Name', 
            'Site-Agency', 'AQS-ID', 'Full_AQS-ID'
        ]
        
        for folder, csv_files in csv_files_by_folder.items():
            
            data_frames = []
            for file in csv_files:
                df = pd.read_csv(file, header=None)
                
                # Log the columns to debug
                print(f"Columns in DataFrame for file {file}: {df.columns.tolist()}")
                
                if len(df.columns) != len(expected_headers):
                    return jsonify({"error": f"Column length mismatch in file {file}. Expected {len(expected_headers)} columns, found {len(df.columns)} columns."}), 400
                
                df.columns = expected_headers
                data_frames.append(df)
            
            combined_df = pd.concat(data_frames, ignore_index=True)

            valid_aqi_df = combined_df[combined_df['AQI'] != -999]

            aqi_values = valid_aqi_df['AQI'].values
            
            start_time = time.time()
            avg_aqi = calculate_average_aqi(aqi_values)

            end_time = time.time()
            
            # site_name_freq = combined_df['Site-Name'].value_counts().to_dict()
            # site_agency_freq = combined_df['Site-Agency'].value_counts().to_dict()
            # parameter_freq = combined_df['Parameter'].value_counts().to_dict()
            
            folder_summaries[folder] = {
                "time taken" : (end_time -start_time),
                "average_AQI": avg_aqi
                # "site_name_frequency": site_name_freq,
                # "site_agency_frequency": site_agency_freq,
                # "parameter_frequency": parameter_freq
            }
            
        
        return jsonify({"message": "Processed files successfully.", "summaries": folder_summaries})
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
# API endpoint to process files based on the exact date
@app.route('/process_csv', methods=['GET'])
def process_csv():
    # Get the date from request arguments
    date = request.args.get('date')
    
    if not date:
        return jsonify({"error": "Please provide the date in format YYYYMMDD"}), 400
    
    try:
        # Get list of CSV files for the exact date
        csv_files = get_csv_files_for_exact_date(date)
        
        if not csv_files:
            return jsonify({"message": "No files found for the provided date."})
        
        # Read and process CSV files
        data_frames = []
        for file in csv_files:
            df = pd.read_csv(file)
            data_frames.append(df)
        
        # Further processing of data_frames can be done here
        
        return jsonify({"message": "Files processed successfully."})
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)
