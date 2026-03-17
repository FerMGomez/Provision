import pandas as pd

try:
    df = pd.read_excel('Tarifario_macro.xlsm', sheet_name="Tarifario", skiprows=11, engine='openpyxl')
    print("Columns in 'Tarifario_macro.xlsm':")
    print(df.columns.tolist())
    
    if 'CARRIER' in df.columns and 'TRANSPORT ZONE' in df.columns:
        duplicates = df[df.duplicated(subset=['CARRIER', 'TRANSPORT ZONE'], keep=False)]
        if not duplicates.empty:
            print("\nFound duplicate rows based on ['CARRIER', 'TRANSPORT ZONE']:")
            print(duplicates)
        else:
            print("\nNo duplicates found based on ['CARRIER', 'TRANSPORT ZONE'].")
    else:
        print("\nCould not check for duplicates because 'CARRIER' or 'TRANSPORT ZONE' column is missing.")
        
except Exception as e:
    print(f"Error reading 'Tarifario_macro.xlsm': {e}")
