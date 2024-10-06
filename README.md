# mini1
Code optimization using Multi-Threading and Multiprocessing 
Contains Python and C++ code

## Instructions to Run Python Files

### Prerequisites
- Python 3.x installed
- Required Python packages installed (Flask, pandas, conda enviroment)

### Installation
1. Clone the repository:
   ```sh
   git clone <repository-url>
   cd <repository-directory>
   ```

2. Check for Conda environment and create one if not present:
   ```sh
   conda info --envs  # List all conda environments
   conda create --name myenv python=3.x  # Create a new environment named 'myenv'
   conda activate myenv  # Activate the environment
   ```

## Running Flask application
1. Navigate to the directory containing the Flask application:
    cd <path-to-flask-application>
2. Run Flask App 
    python file_name.py
    
    The application will start, and you can access it at http://127.0.0.1:5000.

## Example API Request
1. Request for Population Application : "http://127.0.0.1:5000/population/Angola/2000"
2. Request for California Fire Reports : "http://127.0.0.1:5000/process_batch_csv?start_date=20200814&end_date=20200817"
3. Request for Parking Violations Application (Non threaded) : "http://127.0.0.1:5000/search?plate_number=KLB3701"
4. Request for Parking Violations Application (Threaded) : "http://127.0.0.1:5000/search?plate_number=JEB5683&max_workers=16"