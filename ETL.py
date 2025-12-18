import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# ==========================================
# 1. CONFIGURATION
# ==========================================
DB_USER = 'root'
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = 'localhost'
DB_PORT = '3306' 
DB_NAME = 'vaccination_db'

RAW_DATA_PATH = r"C:\Vishal\Vaccination Report\dataset\raw"
CLEAN_DATA_PATH = r"C:\Vishal\Vaccination Report\dataset\clean"

os.makedirs(CLEAN_DATA_PATH, exist_ok=True)

# Map Filenames to SQL Tables
files_map = {
    "coverage-data.xlsx": "vaccination_coverage",
    "incidence-rate-data.xlsx": "disease_incidence",
    "reported-cases-data.xlsx": "reported_cases",
    "vaccine-introduction-data.xlsx": "vaccine_introduction",
    "vaccine-schedule-data.xlsx": "vaccine_schedule"
}

# ==========================================
# 2. HELPER: BRIDGE TABLE GENERATOR
# ==========================================
def generate_bridge_table(engine):
    """
    Creates and uploads the critical link between Vaccines and Diseases.
    """
    print("Generating Vaccine-Disease Bridge Table...")
    
    # Hardcoded logic derived from your Data/Reference analysis
    bridge_data = [
        ('DTPCV1', 'DIPHTHERIA'), ('DTPCV1', 'PERTUSSIS'), ('DTPCV1', 'TTETANUS'),
        ('DTPCV3', 'DIPHTHERIA'), ('DTPCV3', 'PERTUSSIS'), ('DTPCV3', 'TTETANUS'),
        ('DIPHCV4', 'DIPHTHERIA'), ('DIPHCV5', 'DIPHTHERIA'), ('DIPHCV6', 'DIPHTHERIA'),
        ('MCV1', 'MEASLES'), ('MCV2', 'MEASLES'),
        ('RCV1', 'RUBELLA'), ('RCV1', 'CRS'),
        ('MUMPS', 'MUMPS'),
        ('POL3', 'POLIO'), ('IPV1', 'POLIO'),
        ('YFV', 'YFEVER'),
        ('JAPENC', 'JAPENC'),
        ('MEN_A', 'INVASIVE_MENING')
    ]
    
    bridge_df = pd.DataFrame(bridge_data, columns=['vaccine_code', 'target_disease_code'])
    
    # Upload
    try:
        bridge_df.to_sql('vaccine_disease_bridge', engine, if_exists='replace', index=False)
        print(" -> ✔ Success! Bridge table uploaded.\n")
    except Exception as e:
        print(f" -> ❌ Error uploading bridge table: {e}\n")

# ==========================================
# 3. MAIN PROCESSING
# ==========================================
def process_data():
    # Setup Connection
    connection_str = f'mysql+mysqlconnector://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
    try:
        engine = create_engine(connection_str)
        with engine.connect() as conn:
            print(f"✔ Connected to: {DB_NAME}\n")
    except Exception as e:
        print(f"❌ Connection Error: {e}")
        return

    # 1. Generate the Bridge Table first
    generate_bridge_table(engine)

    # 2. Process Files
    for filename, table_name in files_map.items():
        file_path = os.path.join(RAW_DATA_PATH, filename)
        
        if not os.path.exists(file_path):
            print(f"⚠ File not found: {filename}")
            continue
            
        try:
            print(f"Processing {filename}...")
            
            # CHECK EXTENSION (Handle CSV or Excel)
            if filename.endswith('.csv'):
                df = pd.read_csv(file_path)
            else:
                df = pd.read_excel(file_path)
            
            # --- CLEANING START ---
            
            # 1. Normalize Headers
            df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')
            
            # 2. SAFE Numeric Conversion (Do NOT fillna=0 for Cases!)
            # We convert to numeric, coercing errors to NaN. 
            # We leave NaN as NaN so SQL treats them as NULL.
            numeric_cols = ['year', 'coverage', 'cases', 'incidence_rate', 'target_number', 'doses']
            
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                    
                    # SPECIAL CASE: 'Year' cannot be Null in SQL usually, so we might drop rows without year
                    if col == 'year':
                        df = df.dropna(subset=['year'])
            
            # 3. Filter Logic (Optional but Recommended)
            # If this is coverage data, maybe you only want WUENIC?
            # if table_name == "vaccination_coverage":
            #     df = df[df['coverage_category'] == 'WUENIC']

            # 4. Remove Duplicates
            df.drop_duplicates(inplace=True)
            
            # --- CLEANING END ---

            # Save Local Backup
            clean_filename = f"clean_{os.path.splitext(filename)[0]}.csv"
            save_path = os.path.join(CLEAN_DATA_PATH, clean_filename)
            df.to_csv(save_path, index=False)
            
            # Upload to SQL
            print(f" -> Uploading to {table_name}...")
            df.to_sql(table_name, engine, if_exists='replace', index=False)
            print(f" -> ✔ Success! ({len(df)} rows)\n")
            
        except Exception as e:
            print(f"❌ Error processing {filename}: {e}\n")

if __name__ == "__main__":
    process_data()