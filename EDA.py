import pandas as pd
from sqlalchemy import create_engine
import os

# ==========================================
# 1. CONFIGURATION
# ==========================================
# Database Connection Details
DB_USER = 'root'
DB_PASSWORD = 'Vishal#09' 
DB_HOST = 'localhost'
DB_PORT = '3306' 
DB_NAME = 'vaccination_db'

# File Paths
# Use raw strings (r"...") for Windows paths
RAW_DATA_PATH = r"C:\Vishal\Vaccination Report\dataset\raw"
CLEAN_DATA_PATH = r"C:\Vishal\Vaccination Report\dataset\clean"

# Create clean folder if it doesn't exist
os.makedirs(CLEAN_DATA_PATH, exist_ok=True)

# Map Filenames to Target SQL Table Names
files_map = {
    "coverage-data.xlsx": "vaccination_coverage",
    "incidence-rate-data.xlsx": "disease_incidence",
    "reported-cases-data.xlsx": "reported_cases",
    "vaccine-introduction-data.xlsx": "vaccine_introduction",
    "vaccine-schedule-data.xlsx": "vaccine_schedule"
}

# ==========================================
# 2. SETUP CONNECTION
# ==========================================
# Create SQLAlchemy Engine
connection_str = f'mysql+mysqlconnector://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
try:
    engine = create_engine(connection_str)
    # Test connection
    with engine.connect() as conn:
        print(f"✔ Successfully connected to database: {DB_NAME}\n")
except Exception as e:
    print(f"❌ Database Connection Error: {e}")
    print("Check your password and ensure MySQL server is running.")
    exit()

# ==========================================
# 3. MAIN PROCESSING LOOP
# ==========================================
def process_data():
    for filename, table_name in files_map.items():
        file_path = os.path.join(RAW_DATA_PATH, filename)
        
        if not os.path.exists(file_path):
            print(f"⚠ Warning: File not found: {filename}")
            continue
            
        try:
            print(f"Processing {filename}...")
            
            # A. READ DATA
            df = pd.read_excel(file_path)
            
            # B. CLEAN DATA
            # 1. Normalize column names (lowercase, no spaces)
            df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')
            
            # 2. Handle Numeric Columns (Fix "Incorrect decimal value" error)
            # Identify likely numeric columns based on keywords or manual list
            numeric_cols = [
                'year', 'coverage', 'cases', 'incidence_rate', 
                'target_number', 'doses', 'schedulerounds'
            ]
            
            for col in numeric_cols:
                if col in df.columns:
                    # Convert to numeric, turn errors (text) into NaN
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                    # Fill NaN with 0 so SQL accepts it
                    df[col] = df[col].fillna(0)
            
            # 3. Remove Duplicates
            df.drop_duplicates(inplace=True)
            
            # C. SAVE LOCAL BACKUP
            save_path = os.path.join(CLEAN_DATA_PATH, f"clean_{filename.replace('.xlsx', '.csv')}")
            df.to_csv(save_path, index=False)
            print(f"   -> Saved clean CSV to: {save_path}")
            
            # D. UPLOAD TO SQL
            print(f"   -> Uploading to SQL table: {table_name}...")
            df.to_sql(table_name, engine, if_exists='replace', index=False)
            print(f"   ✔ Success! ({len(df)} rows)\n")
            
        except Exception as e:
            print(f"❌ Error processing {filename}: {e}\n")

if __name__ == "__main__":
    process_data()