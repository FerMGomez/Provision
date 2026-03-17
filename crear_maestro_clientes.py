import pandas as pd
import os

def load_customer_master(path):
    """
    Carga y pre-procesa el archivo maestro de clientes (ZCUST).

    Lee el archivo, selecciona y renombra columnas relevantes, filtra clientes,
    elimina duplicados y crea un campo combinado de nombre y ciudad.
    """
    df = pd.read_excel(path, engine='openpyxl')
    
    required_columns = [
        'SOrg.', 'Customer Number', 'Sold To Num', 'Ship To',
        'Customer number of business partner', 'Street', 'PostalCode', 'City',
        'Tax Number 1', 'OrBlk', 'Region (State, Province, County)',
        'TranspZone', 'Name 2'
    ]
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Faltan las siguientes columnas en el archivo '{path}': {', '.join(missing_columns)}")

    df = df[required_columns].copy()
    df['Sold To Num'] = df['Sold To Num'].astype(str)
    df['Ship To'] = df['Ship To'].astype(str)

    # Filtrar por las condiciones especificadas
    sold_to_mask = df['Sold To Num'].str.startswith('1')
    ship_to_mask = (df['Ship To'].str.len() == 10) & (df['Ship To'].str.startswith(('1', '3')))
    df = df[sold_to_mask & ship_to_mask].copy()

    df.drop_duplicates(subset=['Ship To'], inplace=True)
    df['Customer_Name_City'] = df['Customer Number'] + ' (' + df['City'] + ')'
    df.rename(columns={
        'Sold To Num': 'CLIENTE_MC_NUM', 
        'Customer Number': 'CLIENTE_MC_NAME',
        'TranspZone': 'TRANSPORT ZONE'
    }, inplace=True)
    return df

if __name__ == "__main__":
    maestro_file = 'ZCUST.xlsx'
    output_file = 'maestro_clientes.xlsx'

    try:
        print(f"Cargando y procesando el archivo '{maestro_file}'...")
        customer_master_df = load_customer_master(maestro_file)
        
        print(f"Exportando los datos a '{output_file}'...")
        customer_master_df.to_excel(output_file, index=False, engine='xlsxwriter')
        
        print(f"¡Proceso completado! El archivo '{output_file}' ha sido creado con éxito.")
        print(f"Total de clientes procesados: {len(customer_master_df)}")

    except FileNotFoundError:
        print(f"Error: No se pudo encontrar el archivo de entrada '{maestro_file}'. Asegúrate de que esté en la misma carpeta.")
    except Exception as e:
        print(f"Ocurrió un error inesperado: {e}")

