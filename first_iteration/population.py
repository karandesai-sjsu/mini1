from flask import Flask, jsonify, request
import pandas as pd
import time
app = Flask(__name__)

df = pd.read_csv('../DATA/API_SP.POP.TOTL_DS2_en_csv_v2_3401680/API_SP.POP.TOTL_DS2_en_csv_v2_3401680.csv')

df = df[['Country Name', 'Country Code'] + [str(year) for year in range(1960, 2022)]]

@app.route('/population/<country>/<year>', methods=['GET'])
def get_population_by_year(country, year):
    start_time = time.time()
    try:
        year = str(year)
        result = df[df['Country Name'].str.lower() == country.lower()][['Country Name', year]]
        end_time = time.time()
        if not result.empty:
            population = result[year].values[0]
            return jsonify({"Time" : (end_time - start_time) , "Country": country, "Year": year, "Population": population})
        else:
            return jsonify({"error": "Country or Year not found"}), 404
    except Exception as e:
        return jsonify({"error": str(e)}), 500

#cumulative population for a specific country over all years
@app.route('/population/cumulative/country/<country>', methods=['GET'])
def get_cumulative_population_by_country(country):
    start_time = time.time()
    try:
        result = df[df['Country Name'].str.lower() == country.lower()]
        end_time = time.time()
        if not result.empty:
            population_sum = result.iloc[:, 2:].sum(axis=1).values[0]
            return jsonify({"Time" : (end_time - start_time) ,"Country": country, "Cumulative Population": population_sum})
        else:
            return jsonify({"error": "Country not found"}), 404
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# 3. Get cumulative population for all countries in a specific year
@app.route('/population/cumulative/year/<year>', methods=['GET'])
def get_cumulative_population_by_year(year):
    start_time = time.time()
    try:
        year = str(year)
        if year in df.columns:
            population_sum = df[year].sum()
            end_time = time.time()
            return jsonify({"Time" : (end_time - start_time) ,"Year": year, "Cumulative Population": population_sum})
        else:
            return jsonify({"error": "Year not found"}), 404
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Run the app
if __name__ == '__main__':
    app.run(debug=True ,port =8001)
