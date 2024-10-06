import os
import pandas as pd
import time
from flask import Flask, request, jsonify
from numba import njit, prange ,typed , types
from numba.typed import List

app = Flask(__name__)

DATA_DIR = '../DATA\AirNow fires/fire-2020-full-data/data' 
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
def compute_dataframe(aqi_values, site_name_values, site_agency_values, parameter_values):
    total_aqi = 0.0
    count_aqi = 0
    site_name_freq = typed.Dict.empty(key_type=types.unicode_type, value_type=types.int64)
    site_agency_freq = typed.Dict.empty(key_type=types.unicode_type, value_type=types.int64)
    parameter_freq = typed.Dict.empty(key_type=types.unicode_type, value_type=types.int64)

    for i in prange(len(aqi_values)):
        print(i)
        aqi = aqi_values[i]
        site_name = site_name_values[i]
        site_agency = site_agency_values[i]
        parameter = parameter_values[i]

        if aqi != -999:
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

    return avg_aqi, site_name_freq, site_agency_freq, parameter_freq

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
            
            aqi_values = result_df['AQI'].values.astype(float)
            site_name_values = List([
                str(name) if isinstance(name, str) else "unknown"
                for name in result_df['Site-name'].values.tolist()
            ])

            site_agency_values = List([
                str(agency) if isinstance(agency, str) else "unknown"
                for agency in result_df['Site-agency'].values.tolist()
            ])

            parameter_values = List([
                str(param) if isinstance(param, str) else "unknown"
                for param in result_df['Parameter'].values.tolist()
            ])
            
            avg_aqi, site_name_freq, site_agency_freq, parameter_freq = compute_dataframe(
                aqi_values, site_name_values, site_agency_values, parameter_values
            )

            end_time = time.time()
                        
            folder_summaries[folder] = {
                "time taken": (end_time - start_time),
                # "average_AQI": avg_aqi,
                # "site_name_frequency": dict(site_name_freq),
                # "site_agency_frequency": dict(site_agency_freq),
                # "parameter_frequency": dict(parameter_freq)
            }
                    
        return jsonify({"message": "Processed files successfully.", "summaries": folder_summaries})
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=False)
