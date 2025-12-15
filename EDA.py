import pandas as pd
import os

# 1. SETUP: Define file paths
raw_data_path = r"C:/Vishal/Vaccination Report/dataset/raw"
output_data_path = r"C:/Vishal/Vaccination Report/dataset/clean"



files = {
    "coverage": os.path.join(raw_data_path, "coverage-data.xlsx"),
    "incidence": os.path.join(raw_data_path, "incidence-rate-data.xlsx"),
    "cases": os.path.join(raw_data_path, "reported-cases-data.xlsx"),
    "intro": os.path.join(raw_data_path, "vaccine-introduction-data.xlsx"),
    "schedule": os.path.join(raw_data_path, "vaccine-schedule-data.xlsx")
}

def clean_and_save(file_path, output_name):
    try:
        # Load the dataset
        df = pd.read_excel(file_path)

        # Display initial info
        print(f"Initial info for {output_name}:")
        print(df.info())
        print("\n")

        # Drop rows with any missing values
        df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')

        # Normalize Units (Converts Year to Int, coverage to Numric)
        if 'year' in df.columns:
            df['year'] = pd.to_numeric(df['year'], errors='coerce').fillna(0).astype(int)

        if 'coverage' in df.columns:
            df['coverage'] = pd.to_numeric(df['coverage'], errors='coerce').fillna(0)

        if 'cases' in df.columns:
            df['cases'] = pd.to_numeric(df['cases'], errors='coerce').fillna(0).astype(int)

        # Remove duplicates
        df.drop_duplicates(inplace=True)

        # Save cleaned dataset
        save_path = os.path.join(output_data_path, f"clean_{output_name}.csv")
        df.to_csv(save_path,index=False)
        print(f"Successfully cleaned and saved: {save_path}\n")

    except Exception as e:
        print(f"Error processing {output_name}: {e}\n") 

# 2. Process each file
for key, path in files.items():
    clean_and_save(path, key) 