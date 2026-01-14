"""
================================================================================
PROJECT:        COGNITIVE CAPITAL ANALYSIS - BRAZIL
SCRIPT:         src/indicadores/extract_snis_data.py
VERSION:        1.2 (Added Source URL & Link Integrity)
DATE:           2026-01-13
--------------------------------------------------------------------------------
PRINCIPAL INVESTIGATOR:  Dr. José Aparecido da Silva
LEAD DEVELOPER:          Specialist in Applied Statistics
SOURCE:                 SNIS (Sistema Nacional de Informações sobre Saneamento)
================================================================================

ABSTRACT:
    Automated ETL pipeline for the National Sanitation Information System (SNIS).
    Extracts and aggregates municipal data to state-level (UF) indicators.

DATA SOURCE:
    - SNIS 2022 Raw Municipal Dataset (Official Brazilian Government data).
    - URL: http://app4.mdr.gov.br/serieHistorica/ (Selected: Municípios / 2022)

OUTPUTS:
    1. CSV: data/processed/indicadores/sanitation_indicators_2022.csv
    2. XLSX: reports/indicadores/xlsx/sanitation_indicators_2022.xlsx
    3. PLOT: reports/indicadores/graficos/sanitation_quadrant_analysis.png

TABLE CONTENT:
    - SG_UF_PROVA: State abbreviation (Federation Unit).
    - AGUA_ATENDIMENTO_PERC: Mean percentage of population with water access.
    - ESGOTO_ATENDIMENTO_PERC: Mean percentage of population with sewage collection.

VISUALIZATION:
    - Bivariate Scatter Plot (Quadrant Analysis) for infrastructure gap detection.

DEPENDENCIES:
    pandas, matplotlib, seaborn, openpyxl, io
================================================================================
"""
import pandas as pd
import os
import io
import matplotlib.pyplot as plt
import seaborn as sns

# --- Path Configuration ---
# Setting paths according to the required directory structure
base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
output_dir_csv = os.path.join(base_dir, 'data', 'processed', 'indicadores')
output_dir_xlsx = os.path.join(base_dir, 'reports', 'indicadores', 'xlsx')
output_dir_plots = os.path.join(base_dir, 'reports', 'indicadores', 'graficos')

input_file = os.path.join(base_dir, 'data', 'raw', 'snis_municipios_2022.csv')

# Valid Federation Unit acronyms
SIGLAS_UF = [
    'AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA', 'MT', 'MS', 
    'MG', 'PA', 'PB', 'PR', 'PE', 'PI', 'RJ', 'RN', 'RS', 'RO', 'RR', 'SC', 
    'SP', 'SE', 'TO'
]

# State Name to Acronym Mapping
DE_PARA_UF = {
    'Acre': 'AC', 'Alagoas': 'AL', 'Amapá': 'AP', 'Amazonas': 'AM', 'Bahia': 'BA', 
    'Ceará': 'CE', 'Distrito Federal': 'DF', 'Espírito Santo': 'ES', 'Goiás': 'GO', 
    'Maranhão': 'MA', 'Mato Grosso': 'MT', 'Mato Grosso do Sul': 'MS', 'Minas Gerais': 'MG', 
    'Pará': 'PA', 'Paraíba': 'PB', 'Paraná': 'PR', 'Pernambuco': 'PE', 'Piauí': 'PI', 
    'Rio de Janeiro': 'RJ', 'Rio Grande do Norte': 'RN', 'Rio Grande do Sul': 'RS', 
    'Rondônia': 'RO', 'Roraima': 'RR', 'Santa Catarina': 'SC', 'São Paulo': 'SP', 
    'Sergipe': 'SE', 'Tocantins': 'TO'
}

def execute_health_check(df):
    """
    Validates statistical premises and data integrity.
    """
    print(f"\n[Statistical Health Check]")
    errors = []
    if len(df) != 27:
        errors.append(f"Critical Error: Found {len(df)} UFs. Expected: 27.")
    
    if df['AGUA_ATENDIMENTO_PERC'].isnull().any(): errors.append("Missing values in Water Access")
    if df['ESGOTO_ATENDIMENTO_PERC'].isnull().any(): errors.append("Missing values in Sewage Access")

    if not errors:
        return True
    else:
        for error in errors: print(f"FAIL: {error}")
        return False

def generate_report_visuals(df):
    """
    Generates high-resolution visualization for the cognitive report.
    """
    sns.set_theme(style="whitegrid")
    plt.figure(figsize=(14, 10))
    
    plot = sns.scatterplot(
        data=df, 
        x='AGUA_ATENDIMENTO_PERC', 
        y='ESGOTO_ATENDIMENTO_PERC',
        s=100,
        color='teal',
        edgecolor='black'
    )
    
    # Labelling each state
    for i in range(df.shape[0]):
        plt.text(
            df.AGUA_ATENDIMENTO_PERC[i] + 0.7, 
            df.ESGOTO_ATENDIMENTO_PERC[i], 
            df.SG_UF_PROVA[i], 
            fontsize=10, 
            fontweight='bold'
        )
    
    plt.title('sanitation infrastructure profile by state (2022)', fontsize=16, pad=20)
    plt.xlabel('water supply service coverage (percentage)', fontsize=12)
    plt.ylabel('sewage collection service coverage (percentage)', fontsize=12)
    
    # Reference lines for national targets
    plt.axhline(90, color='red', linestyle='--', alpha=0.5, label='legal framework target')
    plt.axvline(99, color='blue', linestyle='--', alpha=0.5)
    
    plot_path = os.path.join(output_dir_plots, 'sanitation_quadrant_analysis.png')
    plt.savefig(plot_path, dpi=300, bbox_inches='tight')
    print(f"Visualization saved to: {plot_path}")

def extract_snis_data():
    print("Initiating SNIS Data Extraction...")
    
    if not os.path.exists(input_file):
        print(f"CRITICAL ERROR: Input file not found: {input_file}")
        return

    try:
        buffer_clean = io.StringIO()
        encoding_detected = 'utf-16-le'
        
        # Encoding Detection Logic
        try:
            with open(input_file, 'r', encoding='utf-16-le') as f: f.read(100)
        except:
            encoding_detected = 'utf-8'

        try:
            with open(input_file, 'r', encoding='utf-8') as f:
                if 'Código' in f.readline(): encoding_detected = 'utf-8'
        except: pass

        print(f"   Detected Encoding: {encoding_detected}")

        # Data Cleaning Stream
        with open(input_file, 'r', encoding=encoding_detected, errors='replace') as f:
            for line in f:
                line = line.strip()
                if not line: continue
                if line.endswith(';'): line = line[:-1]
                buffer_clean.write(line + '\n')
        
        buffer_clean.seek(0)
        df = pd.read_csv(buffer_clean, sep=';', on_bad_lines='warn')
        df.columns = [str(c).strip().replace('"', '') for c in df.columns]

        # Column Mapping (Identifying SNIS standard codes IN055 and IN056)
        col_uf = next((c for c in df.columns if c in ['Estado', 'UF', 'Sigla']), None)
        col_water = next((c for c in df.columns if 'IN055' in c), None)
        col_sewage = next((c for c in df.columns if 'IN056' in c), None)

        if not all([col_uf, col_water, col_sewage]):
            print("ERROR: Required data columns (UF, IN055, IN056) not found.")
            return

        # Standardization and UF Filtering
        df[col_uf] = df[col_uf].astype(str).str.strip().str.replace('"', '')
        df['SG_UF_PROVA'] = df[col_uf].map(DE_PARA_UF).fillna(df[col_uf])
        df = df[df['SG_UF_PROVA'].isin(SIGLAS_UF)].copy()

        # Numeric Conversion (handling Brazilian decimal separators)
        for col in [col_water, col_sewage]:
            df[col] = df[col].astype(str).str.replace('.', '', regex=False)
            df[col] = df[col].str.replace(',', '.', regex=False)
            df[col] = pd.to_numeric(df[col], errors='coerce')

        # State-Level Aggregation (Mean)
        df_final = df.groupby('SG_UF_PROVA').agg({
            col_water: 'mean',
            col_sewage: 'mean'
        }).reset_index()

        df_final.columns = ['SG_UF_PROVA', 'AGUA_ATENDIMENTO_PERC', 'ESGOTO_ATENDIMENTO_PERC']
        df_final = df_final.round(2)

        # File Exportation
        if execute_health_check(df_final):
            os.makedirs(output_dir_csv, exist_ok=True)
            os.makedirs(output_dir_xlsx, exist_ok=True)
            os.makedirs(output_dir_plots, exist_ok=True)
            
            file_xlsx = os.path.join(output_dir_xlsx, 'sanitation_indicators_2022.xlsx')
            file_csv = os.path.join(output_dir_csv, 'sanitation_indicators_2022.csv')
            
            df_final.to_excel(file_xlsx, index=False, sheet_name='SNIS_2022')
            df_final.to_csv(file_csv, index=False, sep=';', encoding='utf-8-sig')
            
            generate_report_visuals(df_final)
            
            print(f"Export Process Successfully Completed (Sheet: SNIS_2022)")

    except Exception as e:
        print(f"FATAL SCRIPT ERROR: {e}")

if __name__ == "__main__":
    extract_snis_data()