import os
import time
import csv
from flask import Flask, request, jsonify
from collections import Counter

app = Flask(__name__)

DATA_DIR = '../DATA/AirNow fires/fire-2020-full-data/data'
EXPORT_DIR = '../../'

# function to get CSV files for a specific date range
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
    with open(file_path, mode='r') as file:
        csv_reader = csv.reader(file)
        data = [row for row in csv_reader]
    return data

#endpoint to process files based on the date range
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
            combined_data = []
            for file in csv_files:
                data = read_csv_file(file)
                if len(data[0]) != len(expected_headers):
                    return jsonify({"error": f"Column length mismatch in file {file}. Expected {len(expected_headers)} columns, found {len(data[0])} columns."}), 400
                combined_data.extend(data)
        
            result_data = [(row[7], row[9], row[10], row[3]) for row in combined_data]
            
            site_name_freq = {}
            site_agency_freq = {}
            parameter_freq = {}

            total_aqi = 0.0
            count_aqi = 0

            for row in result_data:
                try:
                    aqi = int(row[0]) if row[0] != '-999' else 0
                except ValueError:
                    return jsonify({"error": f"Invalid AQI value: {row[0]}"}), 400
                
                site_name = row[1]
                site_agency = row[2]
                parameter = row[3]

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

            folder_summaries[folder] = {
                "average_AQI": avg_aqi,
                "site_name_frequency": site_name_freq,
                "site_agency_frequency": site_agency_freq,
                "parameter_frequency": parameter_freq
            }
        
        return jsonify({"message": "Processed files successfully.", "summaries": folder_summaries})
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)
