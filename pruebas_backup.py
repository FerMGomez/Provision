import pandas as pd
import numpy as np
import re
import os
#----------------------------------------------------------------------------
#Este es el que funciona_ se corrio la ultima para calcular Enero2026
#----------------------------------------------------------------------------


# --- PARÁMETROS DE ANÁLISIS ---
MES_DE_ANALISIS = 2 # Modifica esta variable para cambiar el mes a analizar (e.g., 8 para Agosto)
RUTA_ARCHIVOS_CIERRE = r'C:\Users\fgomez\OneDrive - Reckitt\Documents 1\SAP\SAP GUI'

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

    # Nueva regla para 'Alcance - Distribución'
    if tipo_viaje == 'Alcance - Distribución':
        return 'Alcance_D'

    # Re_Viaje: ej. '220250825_BUE_29_B' y tipo 'Simple'
    if re.match(r'^\d+_[A-Z]{3}_\d+_[A-Z]$', id_viaje) and tipo_viaje == 'Simple':
        return 'Re_Viaje'
    # Unico: ej. '20250828_BUE_07' y tipo 'Simple'
    if re.match(r'^\d+_[A-Z]{3}_\d+$', id_viaje) and tipo_viaje == 'Simple':
        return 'Unico'
    # Alcance_Expo: ej. '20250901_EX_02' y tipo 'Exportación'
    if re.match(r'^\d+_EX_\d+$', id_viaje) and tipo_viaje == 'Exportación':
        return 'Alcance_Expo'
    # Alcance: ej. 'AR00BA1002' y tipo 'Alcance' o 'Exportación'
    if re.match(r'^AR[A-Z0-9]{8}$', id_viaje) and tipo_viaje in ['Alcance', 'Exportación']:
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

    # Filtrar filas donde 'Accounting document number' está vacío
    if 'Accounting document number' in billing.columns:
        billing.dropna(subset=['Accounting document number'], inplace=True)
        billing = billing[billing['Accounting document number'].astype(str).str.strip() != ''].copy()

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
    
    required_columns = ['Sold To Num', 'Ship To', 'Customer Number', 'City', 'TranspZone']
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Faltan las siguientes columnas en el archivo '{path}': {', '.join(missing_columns)}")

    df = df[required_columns].copy()
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

    Lee la hoja "Tarifario" de un archivo Excel, omite las primeras filas,
    elimina duplicados y normaliza las columnas de transportista y zona de transporte.
    """
    df = pd.read_excel(path, sheet_name="Tarifario", skiprows=11, engine='openpyxl')
    df.drop_duplicates(subset=['CARRIER', 'TRANSPORT ZONE'], inplace=True)
    df = normalize_columns(df, ['CARRIER', 'TRANSPORT ZONE'])
    return df

# --- Lógica de Tarifas ---
def asignar_tarifa_vectorizado(df_viajes, tarifario):
    """Asigna tarifas a los viajes y determina el motivo del resultado de forma vectorizada, incluyendo la lógica de penalidades."""
    df = df_viajes.copy()

    # --- 1. Normalización y Claves ---
    # Implementación de la lógica para transportistas divididos
    transporte_parts = df['TRANSPORTE'].astype(str).str.strip().str.upper().str.split('/')
    
    # Por defecto, se usa el primer transportista (o el único si no hay división)
    df['TRANSPORTE_NORM'] = transporte_parts.str.get(0)
    
    # Para viajes de 'Distribución - Troncal' con transportista dividido, se usa el segundo
    is_dist_mask = df['TIPO DE VIAJE'].astype(str).str.strip().str.upper() == 'DISTRIBUCIÓN - TRONCAL'
    has_multiple = transporte_parts.str.len() > 1
    
    # Aplicar la lógica usando .loc para una asignación segura
    mask = is_dist_mask & has_multiple
    if mask.any():
        df.loc[mask, 'TRANSPORTE_NORM'] = transporte_parts[mask].str.get(-1)

    df['TIPO_VIAJE_NORM'] = df['TIPO DE VIAJE'].astype(str).str.strip().str.upper()
    df['UNIDAD_NORM'] = df['UNIDAD'].astype(str).str.strip().str.upper()
    df['TRANSPORT_ZONE_NORM'] = df['TRANSPORT ZONE'].astype(str).str.strip().str.upper()
    df['ID_VIAJES_NORM'] = df['ID_VIAJES'].astype(str).str.strip().str.upper()
    df['PRESENTISMO_NORM'] = df['PRESENTISMO'].astype(str).str.strip().str.upper()
    df['ZONE_KEY'] = np.where(df['T_VIAJE'].isin(['Alcance', 'Retiro']), df['ID_VIAJES_NORM'], df['TRANSPORT_ZONE_NORM'])

    # --- 2. Cálculo de Tarifa Base (para todos los viajes) ---
    # Se calcula una tarifa base para todos, que luego se ajustará si es una penalidad.
    
    # Preparar tarifario en formato largo para viajes directos
    id_vars = ['CARRIER', 'TRANSPORT ZONE']
    value_vars = [col for col in tarifario.columns if col not in id_vars]
    tarifario_long = tarifario.melt(id_vars=id_vars, value_vars=value_vars, var_name='UNIDAD_NORM', value_name='Tarifa_Base')
    tarifario_long['TRANSPORTE_NORM'] = tarifario_long['CARRIER'].str.strip().str.upper()
    tarifario_long['ZONE_KEY'] = tarifario_long['TRANSPORT ZONE'].str.strip().str.upper()
    tarifario_long['UNIDAD_NORM'] = tarifario_long['UNIDAD_NORM'].str.strip().str.upper()

    # Unir para obtener tarifa de viajes directos
    df = pd.merge(df, tarifario_long, on=['TRANSPORTE_NORM', 'ZONE_KEY', 'UNIDAD_NORM'], how='left')

    # Calcular tarifa para viajes de distribución
    dist_mask = ~df['TIPO_VIAJE_NORM'].isin(['SIMPLE', 'DOS PUNTOS', 'EXPORTACIÓN', 'ALCANCE', 'RETIRO'])
    if dist_mask.any():
        tarifario_dist = tarifario.rename(columns={'CARRIER': 'TRANSPORTE_NORM', 'TRANSPORT ZONE': 'ZONE_KEY'})
        # Usamos un merge separado para distribución para no crear filas duplicadas por el melt
        df_dist_merged = pd.merge(df[dist_mask].drop(columns=['Tarifa_Base']), tarifario_dist, on=['TRANSPORTE_NORM', 'ZONE_KEY'], how='left')

        cond_andreani = df_dist_merged['TRANSPORTE_NORM'] == 'ANDREANI LOGISTICA S.A.'
        cond_giampa_logis = df_dist_merged['TRANSPORTE_NORM'].isin(['GIAMPAOLETTI BUOSI S.A.', 'LOGISCHER NEA SA'])
        cond_peso_valido = df_dist_merged['Gross weight'].notna() & (df_dist_merged['Gross weight'] > 0)
        conditions = [
            cond_peso_valido & cond_andreani & (df_dist_merged['Gross weight'] <= 900),
            cond_peso_valido & cond_andreani & (df_dist_merged['Gross weight'] > 900),
            cond_peso_valido & cond_giampa_logis
        ]
        choices = [
            df_dist_merged['Aforo x 900KG'],
            df_dist_merged['Aforo x 900KG'] + (df_dist_merged['Gross weight'] - 900) * df_dist_merged['X KG'],
            df_dist_merged['Aforo x 900KG'] + (df_dist_merged['Gross weight'] * df_dist_merged['X KG'])
        ]
        df.loc[dist_mask, 'Tarifa_Base'] = np.select(conditions, choices, default=np.nan)

    # --- 3. Lógica de Penalidades ---
    ZONAS_AMBA = ['AR00BA1001', 'AR00BA1002', 'AR00BA1003', 'AR00BA1004']
    ZONA_INTERIOR_REF = 'AR00BA1001'
    es_falso_flete = df['PRESENTISMO_NORM'] == 'FALSO FLETE'
    es_no_show = df['PRESENTISMO_NORM'] == 'NO SHOW'
    es_penalidad = es_falso_flete | es_no_show
    
    df['Tarifa'] = df['Tarifa_Base'] # Empezamos con la tarifa base

    if es_penalidad.any():
        # Creamos un dataframe con las tarifas de referencia para penalidades del interior
        tarifario_ref = tarifario_long[tarifario_long['ZONE_KEY'] == ZONA_INTERIOR_REF][['TRANSPORTE_NORM', 'UNIDAD_NORM', 'Tarifa_Base']].copy()
        tarifario_ref.rename(columns={'Tarifa_Base': 'Tarifa_Ref'}, inplace=True)
        tarifario_ref.drop_duplicates(subset=['TRANSPORTE_NORM', 'UNIDAD_NORM'], inplace=True)

        # Unimos la tarifa de referencia al dataframe principal
        df = pd.merge(df, tarifario_ref, on=['TRANSPORTE_NORM', 'UNIDAD_NORM'], how='left')

        # Aplicamos la tarifa de referencia para las penalidades del interior
        es_interior_penalidad = es_penalidad & ~df['ZONE_KEY'].isin(ZONAS_AMBA)
        df.loc[es_interior_penalidad, 'Tarifa'] = df['Tarifa_Ref']
        
        # Eliminamos la columna temporal
        df.drop(columns=['Tarifa_Ref'], inplace=True)

        # Aplicamos el multiplicador de penalidad
        df.loc[es_falso_flete, 'Tarifa'] *= 0.5
        df.loc[es_no_show, 'Tarifa'] *= -0.5 # Corregido de -0.5

    # --- 4. Motivos y Finalización ---
    motivo_conditions = [
        es_falso_flete,
        es_no_show,
        df['Tarifa'].notna(),
        (df['TIPO_VIAJE_NORM'] == 'DISTRIBUCIÓN - TRONCAL') & (df['Gross weight'].isna() | (df['Gross weight'] == 0)),
    ]
    motivo_choices = [
        'Penalidad Falso Flete',
        'Penalidad No Show',
        'OK',
        'Falta Gross weight'
    ]
    df['Motivo_Tarifa'] = np.select(motivo_conditions, motivo_choices, default='No se encontró tarifa')

    # Limpiar columnas temporales
    columnas_a_borrar = ['TRANSPORTE_NORM', 'TIPO_VIAJE_NORM', 'UNIDAD_NORM', 'TRANSPORT_ZONE_NORM', 'ID_VIAJES_NORM', 'PRESENTISMO_NORM', 'ZONE_KEY', 'Tarifa_Base']
    df.drop(columns=[col for col in columnas_a_borrar if col in df.columns], inplace=True)

    # Re-unir columnas originales que se pudieron perder en los merges
    cols_to_rejoin = [col for col in df_viajes.columns if col not in df.columns]
    if cols_to_rejoin:
        df = df.join(df_viajes[cols_to_rejoin])

    return df

# --- INICIO: Script principal ---
print("Iniciando procesamiento...")
try:
    df_viajes_trafico = pd.read_excel('Viajes trafico.xlsx', sheet_name='Hoja1', engine='openpyxl')
    df_viajes_trafico['ID_FILA'] = range(len(df_viajes_trafico))
    df_alcance_config = pd.read_excel('Tarifas_Alcance.xlsx', engine='openpyxl')
    maestro_file, tarifario_file = 'ZCUST.xlsx', 'Tarifario_macro.xlsm'
    tarifario = load_tarifario(tarifario_file)
    customer_master_df = load_customer_master(maestro_file)

    cierreF = [fr'{RUTA_ARCHIVOS_CIERRE}\AR02_IcADR_F.XLSX', fr'{RUTA_ARCHIVOS_CIERRE}\AR06_IcADR_F.XLSX',fr'{RUTA_ARCHIVOS_CIERRE}\FACTURAS_2025_Q3.xlsx']
    filtro_a = ['LR number', 'Billing date', 'Plant', 'Ship to party', 'Ship to party name', 'Reference Document number', 'Material', 'Batch number', 'Billed quantity (Base UoM)', 'Gross weight','Sales UoM', 'Billing document', 'Accounting document number', 'Reference']
    billing_b_full = load_billing(cierreF, filtro_a, month_to_filter=None) # Cargar todo para V_BASE
    
    # Hacer el merge UNA SOLA VEZ
    billing_b_full = pd.merge(billing_b_full, customer_master_df, left_on='Ship to party', right_on='Ship To', how='left')
    billing_b_full.drop(columns=['Ship To'], inplace=True)

    # Crear la version filtrada DESPUÉS del merge
    billing_b_filtered = billing_b_full[billing_b_full['Billing date'].dt.month == MES_DE_ANALISIS].copy()

except FileNotFoundError as e: print(f"Error: No se encontró el archivo {e.filename}."); exit()
except Exception as e: print(f"Error al leer archivos Excel: {e}"); exit()

# --- Lógica de procesamiento de viajes (V_BASE) ---
transportes_correctos = ['ANDREANI LOGISTICA S.A.', 'CELSUR LOGISTICA S.A.', 'DISTRI 10 S.R.L.', 'GIAMPAOLETTI BUOSI S.A.', 'I-FLOW S.A.', 'LOGISCHER NEA SA', 'TTES. LOS AMIGOS S.A.', 'WAL-MART ARGENTINA SRL']
# Se modifica el filtro para incluir transportistas combinados, usando la primera parte del nombre.
transporte_base = df_viajes_trafico['TRANSPORTE'].astype(str).str.split('/').str[0].str.strip()
df = df_viajes_trafico[transporte_base.isin(transportes_correctos)].copy()
df['ID_VIAJES'] = df['N DE VIAJE'].astype(str).str.split(',').str[0].str.split('&').str[0].str.strip()
df['T_VIAJE'] = df.apply(clasificar_t_viaje, axis=1)

columnas_finales = ['ID_FILA', 'FECHA CTA', 'N DE VIAJE', 'ID_VIAJES', 'T_VIAJE', 'TRANSPORTE', 'UNIDAD', 'CLIENTE', 'OBD', 'LOCALIDAD', 'TIPO DE VIAJE', 'PRESENTISMO']
df_final = df[[c for c in columnas_finales if c in df.columns]].copy()
df_final['OBD'] = df_final['OBD'].astype(str).str.split(',').str[0].str.strip()

# --- INICIO NUEVA LÓGICA DE CÁLCULO UNIFICADO ---

# 1. Extraer los transportistas para cada función (Alcance y Distribución)
print("Determinando transportistas de Alcance y Distribución...")
df_final['TRANSPORTE_ALCANCE'] = df_final['TRANSPORTE'].astype(str).str.split('/').str[0].str.strip()
df_final['TRANSPORTE_DIST'] = df_final['TRANSPORTE'].astype(str).str.split('/').str[0].str.strip()

mask_alc_dist = df_final['TIPO DE VIAJE'] == 'Alcance - Distribución'
if mask_alc_dist.any():
    df_final.loc[mask_alc_dist, 'TRANSPORTE_DIST'] = df_final.loc[mask_alc_dist, 'CLIENTE'].str.split(' - ').str[-1].str.strip()
    print(f"Detectados {mask_alc_dist.sum()} viajes de tipo 'Alcance - Distribución'.")

mask_split_troncal = (df_final['TIPO DE VIAJE'] == 'Distribución - Troncal') & (df_final['TRANSPORTE'].str.contains('/'))
if mask_split_troncal.any():
    df_final.loc[mask_split_troncal, 'TRANSPORTE_DIST'] = df_final.loc[mask_split_troncal, 'TRANSPORTE'].str.split('/').str[-1].str.strip()
    print(f"Detectados {mask_split_troncal.sum()} viajes de 'Distribución - Troncal' con transportista dividido.")

# 2. Calcular Tarifa de Alcance
print("Calculando tarifas de Alcance...")
df_alcance_config.rename(columns={'TRANSPORTISTA': 'TRANSPORTE_ALCANCE', 'ALCANCE': 'Tarifa_Alcance'}, inplace=True, errors='ignore')

# Para buscar la tarifa, los viajes 'Alcance - Distribución' deben mapearse a 'Alcance', 
# que es como probablemente figuran en el archivo Tarifas_Alcance.xlsx.
# Creamos una clave temporal para el merge.
df_final['TIPO_VIAJE_ALCANCE_KEY'] = df_final['TIPO DE VIAJE'].replace({'Alcance - Distribución': 'Alcance'})

# Hacemos el merge usando la clave temporal
df_final = pd.merge(
    df_final, 
    df_alcance_config[['TRANSPORTE_ALCANCE', 'TIPO DE VIAJE', 'Tarifa_Alcance']],
    left_on=['TRANSPORTE_ALCANCE', 'TIPO_VIAJE_ALCANCE_KEY'], 
    right_on=['TRANSPORTE_ALCANCE', 'TIPO DE VIAJE'],
    how='left',
    suffixes=('', '_y') # Sufijo para la columna 'TIPO DE VIAJE' del df_alcance
)

# Limpiamos columnas duplicadas o innecesarias del merge
df_final.drop(columns=[col for col in df_final.columns if col.endswith('_y')], inplace=True)
df_final.drop(columns=['TIPO_VIAJE_ALCANCE_KEY'], inplace=True)

# Llenamos con 0 los viajes que no encontraron tarifa de alcance y nos aseguramos de que la columna exista.
if 'Tarifa_Alcance' not in df_final.columns:
    df_final['Tarifa_Alcance'] = 0.0
df_final['Tarifa_Alcance'] = df_final['Tarifa_Alcance'].fillna(0)

# 3. Calcular Tarifa de Distribución
print("Calculando tarifas de Distribución...")
billing_dist_agg = billing_b_full.groupby(
    ['LR number', 'Ship to party', 'TRANSPORT ZONE', 'Customer_Name_City'], as_index=False).agg({'Gross weight': 'sum'})
df_dist_detalle = df_final[df_final['TIPO DE VIAJE'].isin(['Distribución - Troncal', 'Alcance - Distribución'])].copy()

if not df_dist_detalle.empty:
    df_dist_detalle = pd.merge(df_dist_detalle, billing_dist_agg, left_on='ID_VIAJES', right_on='LR number', how='left')
    # MUY IMPORTANTE: Sobrescribimos la columna 'TRANSPORTE' para que 'asignar_tarifa_vectorizado' use el transportista de distribución
    df_dist_detalle['TRANSPORTE'] = df_dist_detalle['TRANSPORTE_DIST']
    df_dist_detalle_tarifado = asignar_tarifa_vectorizado(df_dist_detalle, tarifario)
    tarifas_dist_sumadas = df_dist_detalle_tarifado.groupby('ID_FILA')['Tarifa'].sum().reset_index().rename(columns={'Tarifa': 'Tarifa_Distribucion'})
    df_final = pd.merge(df_final, tarifas_dist_sumadas, on='ID_FILA', how='left')
else:
    df_final['Tarifa_Distribucion'] = 0.0

df_final['Tarifa_Distribucion'] = df_final['Tarifa_Distribucion'].fillna(0)

# 4. Calcular Tarifa para viajes Directos/Simples
print("Calculando tarifas para viajes Directos...")
df_directos = df_final[~df_final['TIPO DE VIAJE'].isin(['Distribución - Troncal', 'Alcance - Distribución'])].copy()
if not df_directos.empty:
    df_directos = pd.merge(df_directos, billing_b_full.drop_duplicates(subset=['Reference Document number']), left_on='OBD', right_on='Reference Document number', how='left')
    # Re-incluyo la lógica para buscar zonas de transporte faltantes
    zonas_conocidas = df_directos[df_directos['TRANSPORT ZONE'].notna() & (df_directos['CLIENTE'].notna())].drop_duplicates(subset=['CLIENTE'])
    mapeo_zonas = pd.Series(zonas_conocidas['TRANSPORT ZONE'].values, index=zonas_conocidas['CLIENTE']).to_dict()
    df_directos['TRANSPORT ZONE'] = df_directos['TRANSPORT ZONE'].fillna(df_directos['CLIENTE'].map(mapeo_zonas))
    df_directos_tarifado = asignar_tarifa_vectorizado(df_directos, tarifario)
    df_final = pd.merge(df_final, df_directos_tarifado[['ID_FILA', 'Tarifa']], on='ID_FILA', how='left', suffixes=('', '_directo'))
    df_final.rename(columns={'Tarifa': 'Tarifa_Directo'}, inplace=True, errors='ignore')
else:
    df_final['Tarifa_Directo'] = 0.0

df_final['Tarifa_Directo'] = df_final['Tarifa_Directo'].fillna(0)


# 5. Calcular Tarifa Total
print("Calculando Tarifa Total...")

# Asegurar que las columnas base existan y estén llenas
for col in ['Tarifa_Alcance', 'Tarifa_Distribucion', 'Tarifa_Directo']:
    if col not in df_final.columns:
        df_final[col] = 0
    df_final[col] = df_final[col].fillna(0)

# Unificar costos de Alcance y Directo en la columna 'Tarifa_Alcance', según el pedido
df_final['Tarifa_Alcance'] = df_final['Tarifa_Alcance'] + df_final['Tarifa_Directo']

# El total es la suma de los dos componentes principales
df_final['Tarifa_Total'] = df_final['Tarifa_Alcance'] + df_final['Tarifa_Distribucion']

# Limpieza de columnas temporales, manteniendo las de costos para el output
df_final.drop(columns=['TRANSPORTE_ALCANCE', 'TRANSPORTE_DIST', 'Tarifa_Directo'], errors='ignore', inplace=True)

# --- FIN NUEVA LÓGICA ---

# --- Preparación de hojas de salida (Directos, Distribucion) ---
# Las variables df_directos_tarifado y df_dist_detalle_tarifado se generan en el nuevo bloque unificado.
# Si no se crearon (p.ej. no hay viajes de ese tipo), se inicializan como dataframes vacíos para evitar errores.
if 'df_directos_tarifado' not in locals():
    df_directos_tarifado = pd.DataFrame()
if 'df_dist_detalle_tarifado' not in locals():
    df_dist_detalle_tarifado = pd.DataFrame()

df_directos_final = df_directos_tarifado
df_distribucion_final = pd.DataFrame()
if not df_dist_detalle_tarifado.empty:
    df_distribucion_final = df_dist_detalle_tarifado.groupby(['ID_VIAJES','TRANSPORTE', 'Ship to party','Customer_Name_City', 'TRANSPORT ZONE']).agg({'Gross weight': 'sum', 'Tarifa': 'sum'}).reset_index()

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

# Separar los viajes. Los que tienen distribución son 'Distribución - Troncal' y 'Alcance - Distribución'.
tipos_distribucion = ['Distribución - Troncal', 'Alcance - Distribución']
df_dist = df_costos[df_costos['TIPO DE VIAJE'].isin(tipos_distribucion)].copy()
df_simple = df_costos[~df_costos['TIPO DE VIAJE'].isin(tipos_distribucion)].copy()

# Para viajes simples, el costo total del viaje se asigna a la entrega.
df_simple['Costo_Total_Entrega'] = df_simple['Tarifa_Total']

# Para viajes con distribución, el costo es la tarifa de la entrega individual + el costo de alcance prorrateado.
if not df_dist.empty:
    # 1. Obtener la tarifa por entrega individual.
    df_dist['Ship to party'] = df_dist['Ship to party'].astype(str)
    df_distribucion_final['Ship to party'] = df_distribucion_final['Ship to party'].astype(str)
    # Usamos df_distribucion_final que tiene el costo por entrega.
    df_dist = pd.merge(df_dist, df_distribucion_final[['ID_VIAJES', 'Ship to party', 'Tarifa']],
                       on=['ID_VIAJES', 'Ship to party'], how='left')
    df_dist.rename(columns={'Tarifa': 'Costo_Por_Entrega'}, inplace=True)

    # 2. Calcular el costo de alcance proporcional.
    entregas_por_viaje = df_dist.groupby('ID_VIAJES')['Ship to party'].nunique().reset_index().rename(columns={'Ship to party': 'N_Entregas'})
    df_dist = pd.merge(df_dist, entregas_por_viaje, on='ID_VIAJES', how='left')
    
    # 'Tarifa_Alcance' (costo fijo) se divide por el número de entregas.
    df_dist['Alcance_Proporcional'] = df_dist['Tarifa_Alcance'].fillna(0) / df_dist['N_Entregas']
    
    # 3. Calcular el costo total para la entrega.
    df_dist['Costo_Total_Entrega'] = df_dist['Costo_Por_Entrega'].fillna(0) + df_dist['Alcance_Proporcional'].fillna(0)

# --- DE-DUPLICACIÓN ANTES DEL RESUMEN FINAL ---
# Nos aseguramos de contar el costo de cada entrega/viaje una sola vez, eliminando
# las filas duplicadas que vienen de las líneas de producto en la facturación.
df_dist.drop_duplicates(subset=['ID_VIAJES', 'Ship to party'], inplace=True)
df_simple.drop_duplicates(subset=['ID_VIAJES'], inplace=True)

df_costos_final = pd.concat([df_simple, df_dist], ignore_index=True)

costo_total_por_cliente = df_costos_final.groupby(['CLIENTE_MC_NUM', 'CLIENTE_MC_NAME'])['Costo_Total_Entrega'].sum().reset_index().rename(columns={'Costo_Total_Entrega': 'Tarifa_Total_Cliente'})
df_reporte_final = pd.merge(pivot_inicial.reset_index(), costo_total_por_cliente, on=['CLIENTE_MC_NUM', 'CLIENTE_MC_NAME'], how='left')
df_reporte_final['Costo_por_Caja'] = (df_reporte_final['Tarifa_Total_Cliente'] / df_reporte_final['Total_Cajas']).fillna(0)

# --- NUEVA LÓGICA PARA RESUMEN SHIP-TO ---
print(f"Generando Resumen de Ship-To para el mes: {MES_DE_ANALISIS}")

# 1. Crear el pivot de cantidades a nivel de Ship To
pivot_shipto = pd.pivot_table(
    billing_b_filtered,
    index=['Ship to party', 'Ship to party name'],
    columns='source_suffix',
    values=['Reference Document number', 'Billed quantity (Base UoM)'],
    aggfunc={'Reference Document number': pd.Series.nunique, 'Billed quantity (Base UoM)': 'sum'}
)
pivot_shipto.columns = [f'{val}_{col}' for val, col in pivot_shipto.columns]
pivot_shipto.rename(columns=column_rename_map, inplace=True)
pivot_shipto = pivot_shipto.fillna(0)

# 2. Calcular totales de OBD y Cajas para Ship To
obd_cols_st = [col for col in pivot_shipto.columns if 'OBD' in col]
car_cols_st = [col for col in pivot_shipto.columns if 'CAR' in col]
pivot_shipto['Total_OBD'] = pivot_shipto[obd_cols_st].sum(axis=1)
pivot_shipto['Total_Cajas'] = pivot_shipto[car_cols_st].sum(axis=1)

# 3. Calcular el costo total por Ship To desde df_costos_final
# Aseguramos que las columnas para el groupby existan
groupby_cols_shipto = ['Ship to party', 'Ship to party name']
if all(col in df_costos_final.columns for col in groupby_cols_shipto):
    costo_total_por_shipto = df_costos_final.groupby(groupby_cols_shipto)['Costo_Total_Entrega'].sum().reset_index().rename(columns={'Costo_Total_Entrega': 'Tarifa_Total_Shipto'})
else:
    costo_total_por_shipto = pd.DataFrame(columns=groupby_cols_shipto + ['Tarifa_Total_Shipto'])


# 4. Unir la información de costos con la de cantidades
df_reporte_shipto_final = pd.merge(
    pivot_shipto.reset_index(),
    costo_total_por_shipto,
    on=['Ship to party', 'Ship to party name'],
    how='left'
)
df_reporte_shipto_final['Tarifa_Total_Shipto'] = df_reporte_shipto_final['Tarifa_Total_Shipto'].fillna(0)

# 5. Calcular costo por caja a nivel Ship To
df_reporte_shipto_final['Costo_por_Caja'] = (df_reporte_shipto_final['Tarifa_Total_Shipto'] / df_reporte_shipto_final['Total_Cajas']).replace([np.inf, -np.inf], 0).fillna(0)


# --- Guardar en Excel ---
print("Guardando resultados en 'Provision_Calculada2.xlsx'...")
with pd.ExcelWriter('Provision_Calculada.xlsx', engine='xlsxwriter') as writer:
    df_final.to_excel(writer, sheet_name='V_BASE', index=False)
    df_directos_final.to_excel(writer, sheet_name='Directos', index=False)
    df_distribucion_final.to_excel(writer, sheet_name='Distribucion', index=False)
    df_reporte_final.to_excel(writer, sheet_name='Resumen Clientes', index=False)
    df_reporte_shipto_final.to_excel(writer, sheet_name='Resumen Ship To', index=False)

    # (Aquí se puede añadir el formato de moneda si se desea)

print("Procesamiento completado.")