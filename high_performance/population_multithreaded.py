from flask import Flask, jsonify, request
import pandas as pd
import time
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import ProcessPoolExecutor

app = Flask(__name__)

df = pd.read_csv('../DATA/API_SP.POP.TOTL_DS2_en_csv_v2_3401680/API_SP.POP.TOTL_DS2_en_csv_v2_3401680.csv')
df = df[['Country Name', 'Country Code'] + [str(year) for year in range(1960, 2022)]]

executor = ThreadPoolExecutor(max_workers=4)

# Function to get population for a specific year
def get_population_data(country, year):
    year = str(year)
    result = df[df['Country Name'].str.lower() == country.lower()][['Country Name', year]]
    if not result.empty:
        return {"Country": country, "Year": year, "Population": result[year].values[0]}
    return {"error": "Country or Year not found"}

# Function to get cumulative population for a specific country
def get_cumulative_country_data(country):
    result = df[df['Country Name'].str.lower() == country.lower()]
    if not result.empty:
        population_sum = result.iloc[:, 2:].sum(axis=1).values[0]
        return {"Country": country, "Cumulative Population": population_sum}
    return {"error": "Country not found"}

# Function to get cumulative population for a specific year
def get_cumulative_year_data(year):
    year = str(year)
    if year in df.columns:
        population_sum = df[year].sum()
        return {"Year": year, "Cumulative Population": population_sum}
    return {"error": "Year not found"}

# Route to get population data for a specific year and country
@app.route('/population/<country>/<year>', methods=['GET'])
def get_population(country, year):
    start_time = time.time()  # Start the timer
    future = executor.submit(get_population_data, country, year)
    result = future.result()
    end_time = time.time()  # End the timer
    processing_time_ms = (end_time - start_time)
    return jsonify({"result": result, "processing_time": processing_time_ms})

# Route to get cumulative population for a specific country
@app.route('/population/cumulative/country/<country>', methods=['GET'])
def get_cumulative_population_country(country):
    start_time = time.time()  # Start the timer
    future = executor.submit(get_cumulative_country_data, country)
    result = future.result()
    end_time = time.time()  # End the timer
    processing_time_ms = (end_time - start_time)
    return jsonify({"result": result, "processing_time": processing_time_ms})

# Route to get cumulative population for all countries in a specific year
@app.route('/population/cumulative/year/<year>', methods=['GET'])
def get_cumulative_population_year(year):
    start_time = time.time()  # Start the timer
    future = executor.submit(get_cumulative_year_data, year)
    result = future.result()
    end_time = time.time()  # End the timer
    processing_time_ms = (end_time - start_time)
    return jsonify({"result": result, "processing_time": processing_time_ms})

# Run the app
if __name__ == '__main__':
    app.run(debug=True, threaded=True)
