from flask import Flask, request, jsonify
import pandas as pd

app = Flask(__name__)

violation_codes_df = pd.read_csv('../DATA/data_export/ParkingViolationCodes_January2020.csv')
violation_fees = violation_codes_df.set_index('VIOLATION CODE').to_dict(orient='index')

def search_parking_violations(file_path, plate_number):
    columns = [
        "Summons Number", "Plate ID", "Registration State", "Issue Date",
        "Street Name", "Violation Code", "Violation Description"
    ]
    
    chunksize = 10**6  # Adjust the chunk size as needed
    results = []

    for chunk in pd.read_csv(file_path, usecols=columns, chunksize=chunksize):
        # Filter the chunk based on the plate number
        filtered_chunk = chunk[chunk["Plate ID"] == plate_number]
        results.append(filtered_chunk)

    result_df = pd.concat(results)
    
    result_df["Manhattan Fee"] = result_df["Violation Code"].apply(lambda code: calculate_fee(code, "Manhattan"))
    result_df["Base Fee"] = result_df["Violation Code"].apply(lambda code: calculate_fee(code, "Base"))

    return result_df

def calculate_fee(violation_code, fee_type):
    # Get the fee based on the violation code
    fee_info = violation_fees.get(violation_code, {})
    if fee_type == "Manhattan":
        return fee_info.get("Manhattan  96th St. & below\n(Fine Amount $)", 0)
    elif fee_type == "Base":
        return fee_info.get("All Other Areas\n(Fine Amount $)", 0)
    return 0

@app.route('/search', methods=['GET'])
def search():
    plate_number = request.args.get('plate_number')
    if not plate_number:
        return jsonify({"error": "plate_number parameter is required"}), 400

    file_path = '../DATA/data_export/Parking_Violations_Issued_2022.csv'
    result_df = search_parking_violations(file_path, plate_number)
    result = result_df.to_dict(orient='records')
    return jsonify(result)

if __name__ == '__main__':
    app.run(debug=True)