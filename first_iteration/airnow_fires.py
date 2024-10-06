import os
import pandas as pd
from flask import Flask, request, jsonify
import time

app = Flask(__name__)

DATA_DIR = '../DATA\AirNow fires/fire-2020-full-data/data' 
EXPORT_DIR = '../../'

#get CSV files for a specific date range
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

#get CSV files for a specific date
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

#process files based on the date range
@app.route('/process_batch_csv', methods=['GET'])
def process_batch_csv():
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    
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
                df = pd.read_csv(file, header=None)               
                if len(df.columns) != len(expected_headers):
                    return jsonify({"error": f"Column length mismatch in file {file}. Expected {len(expected_headers)} columns, found {len(df.columns)} columns."}), 400
                
                df.columns = expected_headers
                data_frames.append(df)
            
            combined_df = pd.concat(data_frames, ignore_index=True)
            combined_df['AQI'] = pd.to_numeric(combined_df['AQI'], errors='coerce')
            combined_df['Site-name'] = combined_df['Site-name'].astype(str)
            combined_df['Site-agency'] = combined_df['Site-agency'].astype(str)
            combined_df['Parameter'] = combined_df['Parameter'].astype(str)

            result_df = combined_df[['AQI', 'Site-name', 'Site-agency', 'Parameter']]
            
            start_time = time.time()
            site_name_freq = {}
            site_agency_freq = {}
            parameter_freq = {}

            total_aqi = 0.0
            count_aqi = 0

            for index, row in result_df.iterrows():
                aqi = row['AQI']
                site_name = row['Site-name']
                site_agency = row['Site-agency']
                parameter = row['Parameter']

                if pd.notna(aqi) and aqi != -999:
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

            avg_aqi = total_aqi / count_aqi if count_aqi > 0 else 0

            end_time = time.time()
        
            
            folder_summaries[folder] = {
                "time taken" : (end_time -start_time),
                # "average_AQI": avg_aqi,
                # "site_name_frequency": site_name_freq,
                # "site_agency_frequency": site_agency_freq,
                # "parameter_frequency": parameter_freq
            }
            
        
        return jsonify({"message": "Processed files successfully.", "summaries": folder_summaries})
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    

if __name__ == '__main__':
    app.run(debug=True)
