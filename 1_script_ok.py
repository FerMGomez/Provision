import pandas as pd
import numpy as np
import re
import os

# --- PARÁMETRO DE ANÁLISIS ---
MES_DE_ANALISIS = 1 # Modifica esta variable para cambiar el mes a analizar (e.g., 8 para Agosto)

# --- Funciones de Utilidad ---
def normalize_column(df, column):
    """Normaliza una columna de un DataFrame convirtiéndola a string,
    eliminando espacios y rellenando valores nulos."""
    df[column] = df[column].astype(str).str.strip().fillna("")
    return df

def normalize_columns(df, columns):
    """Aplica la normalización a una lista de columnas en un DataFrame."""
    for col in columns:
        df = normalize_column(df, col)
    return df

def clasificar_t_viaje(row):
    """Clasifica el tipo de viaje (T_VIAJE) basándose en el ID del viaje y
    el tipo de viaje original utilizando expresiones regulares."""
    id_viaje = str(row.get('ID_VIAJES', ''))
    tipo_viaje = str(row.get('TIPO DE VIAJE', '')).strip()

    # Re_Viaje: ej. '220250825_BUE_29_B' y tipo 'Simple'
    if re.match(r'^\d+_[A-Z]{3}_\d+_[A-Z]$', id_viaje) and tipo_viaje == 'Simple':
        return 'Re_Viaje'
    # Unico: ej. '20250828_BUE_07' y tipo 'Simple'
    if re.match(r'^\d+_[A-Z]{3}_\d+$', id_viaje) and tipo_viaje == 'Simple':
        return 'Unico'
    # Alcance_Expo: ej. '20250901_EX_02' y tipo 'Exportación'
    if re.match(r'^\d+_EX_\d+$', id_viaje) and tipo_viaje == 'Exportación':
        return 'Alcance_Expo'
    # Alcance: ej. 'AR00BA1002' y tipo 'Alcance'
    if re.match(r'^AR[A-Z0-9]{8}$', id_viaje) and tipo_viaje == 'Alcance':
        return 'Alcance'
    # Retiro: ej. 'AR00BA1002' y tipo 'Retiro'
    if re.match(r'^AR[A-Z0-9]{8}$', id_viaje) and tipo_viaje == 'Retiro':
        return 'Retiro'
    # Distribución - Troncal: ej. '20250828_BUE_07' y tipo 'Distribución - Troncal'
    if re.match(r'^\d+_[A-Z]{3}_\d+$', id_viaje) and tipo_viaje == 'Distribución - Troncal':
        return 'Distribución'
    
    return 'Otro' # Valor por defecto si no cumple ninguna regla

# --- Carga de Datos (Modificada) ---
def load_billing(files, columns, month_to_filter):
    """Carga y pre-procesa los archivos de facturación (billing).

    Concatena múltiples archivos Excel, filtra por mes si se especifica,
    excluye clientes específicos y normaliza columnas clave.
    """
    df_list = []
    for f in files:
        df = pd.read_excel(f, engine='openpyxl')
        df['source_file'] = os.path.basename(f)
        df_list.append(df)
    
    billing = pd.concat(df_list, ignore_index=True)
    cols_to_keep = list(set(columns + ['source_file']))
    billing = billing[[col for col in cols_to_keep if col in billing.columns]]

    billing['Billing date'] = pd.to_datetime(billing['Billing date'])
    if month_to_filter is not None:
        print(f"Filtrando facturación para el mes: {month_to_filter}")
        billing = billing[billing['Billing date'].dt.month == month_to_filter].copy()

    ship_to_exclude = ["3000021508", "1000078395", "1000078394", "1000078144"]
    billing['Ship to party'] = billing['Ship to party'].astype(str)
    billing = billing[~billing['Ship to party'].isin(ship_to_exclude)]

    billing = normalize_columns(billing, ['Reference Document number', 'Ship to party'])
    return billing

def load_customer_master(path):
    """Carga y pre-procesa el archivo maestro de clientes (ZCUST).

    Lee el archivo, selecciona y renombra columnas relevantes, filtra clientes,
    elimina duplicados y crea un campo combinado de nombre y ciudad.
    """
    df = pd.read_excel(path, engine='openpyxl')
    df = df[['Sold To Num', 'Ship To', 'Customer Number', 'City', 'TranspZone']].copy()
    df['Sold To Num'] = df['Sold To Num'].astype(str)
    df = df[df['Sold To Num'].str.startswith('1')].copy()
    df.drop_duplicates(subset=['Ship To'], inplace=True)
    df['Customer_Name_City'] = df['Customer Number'] + ' (' + df['City'] + ')'
    df.rename(columns={
        'Sold To Num': 'CLIENTE_MC_NUM', 
        'Customer Number': 'CLIENTE_MC_NAME',
        'TranspZone': 'TRANSPORT ZONE'
    }, inplace=True)
    return df

def load_tarifario(path):
    """Carga y pre-procesa el archivo de tarifario.

    Lee la hoja "Tarifario" de un archivo Excel, omite las primeras filas
    y normaliza las columnas de transportista y zona de transporte.
    """
    df = pd.read_excel(path, sheet_name="Tarifario", skiprows=9, engine='openpyxl')
    df = normalize_columns(df, ['CARRIER', 'TRANSPORT ZONE'])
    return df

# --- Lógica de Tarifas ---
def asignar_tarifa_simple(df_viajes, tarifario):
    """Asigna tarifas a los viajes y determina el motivo del resultado.

    Itera sobre un DataFrame de viajes y, según el tipo de viaje, busca
    la tarifa correspondiente en el tarifario. Maneja lógicas específicas
    para viajes directos, de distribución, alcances y retiros.
    """
    tarifas, motivos = [], []
    viajes_directos_tipos = ['SIMPLE', 'DOS PUNTOS', 'EXPORTACIÓN', 'ALCANCE', 'RETIRO']
    for _, row in df_viajes.iterrows():
        t_viaje_val, tipo_viaje, transportista, zona, tipo_unidad, peso_total = (
            str(row.get('T_VIAJE', '')).strip(), str(row.get('TIPO DE VIAJE', '')).strip().upper(),
            str(row.get('TRANSPORTE', '')).strip().upper(), str(row.get('TRANSPORT ZONE', '')).strip().upper(),
            str(row.get('UNIDAD', '')).strip().upper(), row.get('Gross weight'))
        tarifa, motivo = None, ''

        # For 'Alcance' and 'Retiro', the transport zone is the trip ID itself.
        if t_viaje_val in ['Alcance', 'Retiro']:
            zona = str(row.get('ID_VIAJES', '')).strip().upper()

        if tipo_viaje in viajes_directos_tipos:
            filtro = tarifario[(tarifario['CARRIER'].str.upper() == transportista) & (tarifario['TRANSPORT ZONE'].str.upper() == zona)]
            if filtro.empty: motivo = f"No se encontró tarifa para {transportista} en zona {zona}"
            elif tipo_unidad not in filtro.columns: motivo = f"Falta tipo de unidad '{tipo_unidad}' en tarifario"
            else:
                tarifa = filtro.iloc[0][tipo_unidad]
                if pd.isna(tarifa): tarifa, motivo = None, f"Valor de tarifa nulo para '{tipo_unidad}'"
                else: motivo = 'OK'
        elif tipo_viaje == 'DISTRIBUCIÓN - TRONCAL':
            if pd.isna(peso_total) or peso_total == 0: motivo = 'Falta Gross weight'
            else:
                filtro = tarifario[(tarifario['CARRIER'].str.upper() == transportista) & (tarifario['TRANSPORT ZONE'].str.upper() == zona)]
                if filtro.empty: motivo = f"No se encontró tarifa para {transportista} en zona {zona}"
                else:
                    if transportista == 'ANDREANI LOGISTICA S.A.':
                        t_base, t_kg_ex = filtro.iloc[0].get('1s'), filtro.iloc[0].get('X KG')
                        if pd.notna(t_base) and pd.notna(t_kg_ex): 
                            tarifa = t_base if peso_total <= 900 else t_base + (peso_total - 900) * t_kg_ex
                            motivo = 'OK'
                        else: motivo = "Faltan valores '1s' o 'X KG' para Andreani"
                    elif transportista in ['GIAMPAOLETTI BUOSI S.A.', 'LOGISCHER NEA SA']:
                        t_base, t_kg = filtro.iloc[0].get('1s'), filtro.iloc[0].get('X KG')
                        if pd.notna(t_base) and pd.notna(t_kg): 
                            tarifa = t_base + (peso_total * t_kg)
                            motivo = 'OK'
                        else: motivo = "Faltan valores '1s' o 'X KG'"
                    else: motivo = f"Transportista de Distribución no contemplado: {transportista}"
        else: motivo = f"Tipo de viaje no contemplado: {tipo_viaje}"
        tarifas.append(tarifa)
        motivos.append(motivo)
    df_viajes['Tarifa'] = tarifas
    df_viajes['Motivo_Tarifa'] = motivos
    return df_viajes

# --- INICIO: Script principal ---
print("Iniciando procesamiento...")
try:
    df_viajes_trafico = pd.read_excel('Viajes trafico.xlsx', sheet_name='Hoja1', engine='openpyxl')
    df_alcance_config = pd.read_excel('Tarifas_Alcance.xlsx', engine='openpyxl')
    maestro_file, tarifario_file = 'ZCUST.xlsx', 'Tarifario_macro.xlsm'
    tarifario = load_tarifario(tarifario_file)
    customer_master_df = load_customer_master(maestro_file)

    base_path_1 = r'C:\Users\fgomez\OneDrive - Reckitt\Documents 1\SAP\SAP GUI'
    cierreF = [fr'{base_path_1}\AR02_IcADR_F.XLSX', fr'{base_path_1}\AR06_IcADR_F.XLSX',fr'{base_path_1}\FACTURAS_2025_Q3.xlsx']
    filtro_a = ['LR number', 'Billing date', 'Plant', 'Ship to party', 'Ship to party name', 'Reference Document number', 'Material', 'Batch number', 'Billed quantity (Base UoM)', 'Gross weight','Sales UoM', 'Billing document', 'Accounting document number', 'Reference']
    billing_b_full = load_billing(cierreF, filtro_a, month_to_filter=None) # Cargar todo para V_BASE
    billing_b_filtered = billing_b_full[billing_b_full['Billing date'].dt.month == MES_DE_ANALISIS].copy()
    
    billing_b_full = pd.merge(billing_b_full, customer_master_df, left_on='Ship to party', right_on='Ship To', how='left')
    billing_b_filtered = pd.merge(billing_b_filtered, customer_master_df, left_on='Ship to party', right_on='Ship To', how='left')
    billing_b_full.drop(columns=['Ship To'], inplace=True)
    billing_b_filtered.drop(columns=['Ship To'], inplace=True)

except FileNotFoundError as e: print(f"Error: No se encontró el archivo {e.filename}."); exit()
except Exception as e: print(f"Error al leer archivos Excel: {e}"); exit()

# --- Lógica de procesamiento de viajes (V_BASE) ---
transportes_correctos = ['ANDREANI LOGISTICA S.A.', 'CELSUR LOGISTICA S.A.', 'DISTRI 10 S.R.L.', 'GIAMPAOLETTI BUOSI S.A.', 'I-FLOW S.A.', 'LOGISCHER NEA SA', 'TTES. LOS AMIGOS S.A.', 'WAL-MART ARGENTINA SRL']
df = df_viajes_trafico[df_viajes_trafico['TRANSPORTE'].isin(transportes_correctos)].copy()
df['ID_VIAJES'] = df['N DE VIAJE'].astype(str).str.split(',').str[0].str.split('&').str[0].str.strip()
df['T_VIAJE'] = df.apply(clasificar_t_viaje, axis=1)

columnas_finales = ['FECHA CTA', 'N DE VIAJE', 'ID_VIAJES', 'T_VIAJE', 'TRANSPORTE', 'UNIDAD', 'CLIENTE', 'OBD', 'LOCALIDAD', 'TIPO DE VIAJE', 'PRESENTISMO']
df_final = df[[c for c in columnas_finales if c in df.columns]].copy()
df_final['OBD'] = df_final['OBD'].astype(str).str.split(',').str[0].str.strip()

# --- DEBUG: Contar tipos de viaje ---
print("\n--- Tipos de Viaje en df_final (V_Base) ---")
print(df_final['TIPO DE VIAJE'].value_counts())
print("--------------------------------------------\n")

df_directos = df_final[df_final['TIPO DE VIAJE'].isin(['Simple', 'Dos Puntos', 'Exportación', 'Alcance', 'Retiro'])].copy()
df_distribucion = df_final[df_final['TIPO DE VIAJE'] == 'Distribución - Troncal'].copy()

df_directos = pd.merge(df_directos, billing_b_full.drop_duplicates(subset=['Reference Document number']), left_on='OBD', right_on='Reference Document number', how='left')

# --- NUEVA LÓGICA PARA BUSCAR ZONA DE TRANSPORTE ---
# Para viajes no facturados, la zona no se obtiene. La buscamos en otros viajes del mismo cliente.
print("Buscando zonas de transporte para viajes no facturados...")
zonas_conocidas = df_directos[df_directos['TRANSPORT ZONE'].notna() & (df_directos['CLIENTE'].notna())].drop_duplicates(subset=['CLIENTE'])
mapeo_zonas = pd.Series(zonas_conocidas['TRANSPORT ZONE'].values, index=zonas_conocidas['CLIENTE']).to_dict()

# Rellenamos las zonas vacías usando el mapeo por cliente
original_nan_count = df_directos['TRANSPORT ZONE'].isna().sum()
df_directos['TRANSPORT ZONE'] = df_directos['TRANSPORT ZONE'].fillna(df_directos['CLIENTE'].map(mapeo_zonas))
filled_nan_count = original_nan_count - df_directos['TRANSPORT ZONE'].isna().sum()
if filled_nan_count > 0:
    print(f"Se completaron {filled_nan_count} zonas de transporte faltantes.")
# --- FIN NUEVA LÓGICA ---

# Agrupa la facturación por viaje y cliente para sumar el peso de todas las líneas de producto.
# Esto corrige el error de contar solo una línea por entrega en viajes de distribución.
billing_dist_agg = billing_b_full.groupby(
    ['LR number', 'Ship to party', 'TRANSPORT ZONE', 'Customer_Name_City'],
    as_index=False
).agg({'Gross weight': 'sum'})

df_distribucion = pd.merge(df_distribucion, billing_dist_agg, left_on='ID_VIAJES', right_on='LR number', how='left')

df_directos = asignar_tarifa_simple(df_directos, tarifario)
df_distribucion_detalle = asignar_tarifa_simple(df_distribucion, tarifario)

tarifas_directos = df_directos[['N DE VIAJE', 'Tarifa']]
tarifas_dist_sumadas = df_distribucion_detalle.groupby('ID_VIAJES')['Tarifa'].sum().reset_index()
dist_trip_map = df_distribucion[['N DE VIAJE', 'ID_VIAJES']].drop_duplicates()
tarifas_dist = pd.merge(dist_trip_map, tarifas_dist_sumadas, on='ID_VIAJES', how='left')[['N DE VIAJE', 'Tarifa']]
df_final = pd.merge(df_final, pd.concat([tarifas_directos, tarifas_dist]), on='N DE VIAJE', how='left')

df_alcance_config.rename(columns={'TRANSPORTISTA': 'TRANSPORTE'}, inplace=True)
df_final = pd.merge(df_final, df_alcance_config, on=['TRANSPORTE', 'TIPO DE VIAJE'], how='left')
df_final['Alcance'] = df_final.get('ALCANCE', 0).fillna(0)
df_final.drop(columns=['ALCANCE'], inplace=True, errors='ignore')
df_final['Tarifa'] = df_final['Tarifa'].fillna(0)

df_final['Tarifa_Total'] = df_final['Tarifa'] + df_final['Alcance']

# --- Preparación de hojas de salida (Directos, Distribucion) ---
df_directos_final = df_directos # (Simplificado, ajustar columnas si es necesario)
df_distribucion_final = df_distribucion_detalle.groupby(['ID_VIAJES','TRANSPORTE', 'Ship to party','Customer_Name_City', 'TRANSPORT ZONE']).agg({'Gross weight': 'sum', 'Tarifa': 'sum'}).reset_index()

# --- NUEVA LÓGICA PARA RESUMEN CLIENTES ---
print(f"Generando Resumen de Clientes para el mes: {MES_DE_ANALISIS}")
source_map = {'AR02_IcADR_F.XLSX': '2', 'AR06_IcADR_F.XLSX': '6', 'FACTURAS_2025_Q3.xlsx': 'Q3'}
billing_b_filtered['source_suffix'] = billing_b_filtered['source_file'].map(source_map)
pivot_inicial = pd.pivot_table(billing_b_filtered, index=['CLIENTE_MC_NUM', 'CLIENTE_MC_NAME'], columns='source_suffix', values=['Reference Document number', 'Billed quantity (Base UoM)'], aggfunc={'Reference Document number': pd.Series.nunique, 'Billed quantity (Base UoM)': 'sum'})
pivot_inicial.columns = [f'{val}_{col}' for val, col in pivot_inicial.columns]
column_rename_map = {'Reference Document number_2': 'OBD_ARA2', 'Reference Document number_6': 'OBD_ARA6', 'Reference Document number_Q3': 'OBD_Q3', 'Billed quantity (Base UoM)_2': 'CAR_ARA2', 'Billed quantity (Base UoM)_6': 'CAR_ARA6', 'Billed quantity (Base UoM)_Q3': 'CAR_Q3'}
pivot_inicial.rename(columns=column_rename_map, inplace=True)
pivot_inicial = pivot_inicial.fillna(0)
obd_cols = [col for col in pivot_inicial.columns if 'OBD' in col]; car_cols = [col for col in pivot_inicial.columns if 'CAR' in col]
pivot_inicial['Total_OBD'] = pivot_inicial[obd_cols].sum(axis=1)
pivot_inicial['Total_Cajas'] = pivot_inicial[car_cols].sum(axis=1)

df_costos = pd.merge(billing_b_filtered, df_final, left_on='LR number', right_on='ID_VIAJES', how='left')
df_simple = df_costos[df_costos['TIPO DE VIAJE'] != 'Distribución - Troncal'].copy()
df_dist = df_costos[df_costos['TIPO DE VIAJE'] == 'Distribución - Troncal'].copy()

if not df_dist.empty:
    entregas_por_viaje = df_dist.groupby('ID_VIAJES')['Ship to party'].nunique().reset_index().rename(columns={'Ship to party': 'N_Entregas'})
    df_dist = pd.merge(df_dist, entregas_por_viaje, on='ID_VIAJES', how='left')
    df_dist['Alcance_Proporcional'] = df_dist['Alcance'] / df_dist['N_Entregas']
    df_dist['Ship to party'] = df_dist['Ship to party'].astype(str)
    df_distribucion_final['Ship to party'] = df_distribucion_final['Ship to party'].astype(str)
    df_dist = pd.merge(df_dist.drop(columns=['Tarifa']), df_distribucion_final[[ 'ID_VIAJES', 'Ship to party', 'Tarifa']], on=['ID_VIAJES', 'Ship to party'], how='left', suffixes=('', '_entrega'))
    df_dist.rename(columns={'Tarifa_entrega': 'Tarifa'}, inplace=True)
    df_dist['Costo_Total_Entrega'] = df_dist['Tarifa'] + df_dist['Alcance_Proporcional']

df_simple['Costo_Total_Entrega'] = df_simple['Tarifa_Total']

# --- DE-DUPLICACIÓN ANTES DEL RESUMEN FINAL ---
# Nos aseguramos de contar el costo de cada entrega/viaje una sola vez, eliminando
# las filas duplicadas que vienen de las líneas de producto en la facturación.
df_dist.drop_duplicates(subset=['ID_VIAJES', 'Ship to party'], inplace=True)
df_simple.drop_duplicates(subset=['ID_VIAJES'], inplace=True)

columnas_comunes = list(df_simple.columns.intersection(df_dist.columns))
df_costos_final = pd.concat([df_simple[columnas_comunes], df_dist[columnas_comunes]], ignore_index=True)

costo_total_por_cliente = df_costos_final.groupby(['CLIENTE_MC_NUM', 'CLIENTE_MC_NAME'])['Costo_Total_Entrega'].sum().reset_index().rename(columns={'Costo_Total_Entrega': 'Tarifa_Total_Cliente'})
df_reporte_final = pd.merge(pivot_inicial.reset_index(), costo_total_por_cliente, on=['CLIENTE_MC_NUM', 'CLIENTE_MC_NAME'], how='left')
df_reporte_final['Costo_por_Caja'] = (df_reporte_final['Tarifa_Total_Cliente'] / df_reporte_final['Total_Cajas']).fillna(0)

# --- Guardar en Excel ---
print("Guardando resultados en 'Provision_Calculada.xlsx'...")
with pd.ExcelWriter('Provision_Calculada.xlsx', engine='xlsxwriter') as writer:
    df_final.to_excel(writer, sheet_name='V_BASE', index=False)
    df_directos_final.to_excel(writer, sheet_name='Directos', index=False)
    df_distribucion_final.to_excel(writer, sheet_name='Distribucion', index=False)
    df_reporte_final.to_excel(writer, sheet_name='Resumen Clientes', index=False)

    # (Aquí se puede añadir el formato de moneda si se desea)

print("Procesamiento completado.")