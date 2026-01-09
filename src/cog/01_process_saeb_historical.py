"""
PROJECT:     Cognitive Capital Analysis - Brazil
SCRIPT:      src/cog/01_process_saeb_historical.py
RESEARCHERS: Dr. Jose Aparecido da Silva
             Me. Cassio Dalbem Barth
DATE:        2026-01-08 (Fix v2.3: Pure Score + Context Columns)

DESCRIPTION:
    Extracts historical SAEB data (School Level).
    
    METHODOLOGY:
    - Score: 'SAEB_General' is strictly (Math + Language) / 2.
    - Context: 'SES_Index' (Socioeconomic) and 'Public_Share' are extracted 
      as independent variables for correlation analysis.
    - Architecture: Generates individual files per year (2015, 2017).

INPUT:
    - data/raw/microdados_saeb_2015.zip
    - data/raw/microdados_saeb_2017.zip

OUTPUT:
    - data/processed/saeb_table_2015.csv
    - data/processed/saeb_table_2017.csv
    - reports/varcog/xlsx/saeb_table_2015.xlsx
    - reports/varcog/xlsx/saeb_table_2017.xlsx
"""

import pandas as pd
import os
import sys
import zipfile
import numpy as np

# --- 1. SAFEGUARD IMPORT PROTOCOL ---
script_dir = os.path.dirname(os.path.abspath(__file__))
lib_path = os.path.join(script_dir, 'lib')
if lib_path not in sys.path: sys.path.append(lib_path)

try:
    from safeguard import DataGuard
except ImportError:
    DataGuard = None

# --- 2. CONFIGURATION ---
BASE_PATH = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DATA_RAW = os.path.join(BASE_PATH, 'data', 'raw')
DATA_PROCESSED = os.path.join(BASE_PATH, 'data', 'processed')
REPORT_XLSX = os.path.join(BASE_PATH, 'reports', 'varcog', 'xlsx')

os.makedirs(DATA_PROCESSED, exist_ok=True)
os.makedirs(REPORT_XLSX, exist_ok=True)

# Priority: High School (3EM) > 9th Grade (9EF)
PRIORITY_LP = ['MEDIA_3EM_LP', 'MEDIA_9EF_LP', 'MEDIA_5EF_LP', 'PROFICIENCIA_LP_SAEB', 'PROFICIENCIA_LP']
PRIORITY_MT = ['MEDIA_3EM_MT', 'MEDIA_9EF_MT', 'MEDIA_5EF_MT', 'PROFICIENCIA_MT_SAEB', 'PROFICIENCIA_MT']
UF_COLS = ['ID_UF', 'UF', 'SG_UF', 'CO_UF']

# Context Variables
# SES: Socioeconomic Level (NSE)
# TYPE: Administrative Dependency (Public vs Private)
CONTEXT_COLS = {
    'SES': ['NIVEL_SOCIO_ECONOMICO', 'NSE', 'MEDIA_NIVEL_SOCIO_ECONOMICO'], 
    'TYPE': ['IN_PUBLICA', 'ID_DEPENDENCIA_ADM'] 
}

REGIONAL_MAP = {
    'N': ['AC','AP','AM','PA','RO','RR','TO', 11, 12, 13, 14, 15, 16, 17],
    'NE': ['AL','BA','CE','MA','PB','PE','PI','RN','SE', 21, 22, 23, 24, 25, 26, 27, 28, 29],
    'CO': ['DF','GO','MT','MS', 50, 51, 52, 53],
    'SE': ['ES','MG','RJ','SP', 31, 32, 33, 35],
    'S': ['PR','RS','SC', 41, 42, 43]
}
UF_TO_REGION = {}
for reg, codes in REGIONAL_MAP.items():
    for code in codes:
        UF_TO_REGION[code] = reg
        UF_TO_REGION[str(code)] = reg

IBGE_TO_SIGLA = {
    11:'RO', 12:'AC', 13:'AM', 14:'RR', 15:'PA', 16:'AP', 17:'TO',
    21:'MA', 22:'PI', 23:'CE', 24:'RN', 25:'PB', 26:'PE', 27:'AL', 28:'SE', 29:'BA',
    31:'MG', 32:'ES', 33:'RJ', 35:'SP', 41:'PR', 42:'SC', 43:'RS',
    50:'MS', 51:'MT', 52:'GO', 53:'DF'
}

def detect_separator(file_handle):
    try:
        line = file_handle.readline().decode('latin1')
        file_handle.seek(0)
        if line.count(';') > line.count(','): return ';'
        return ','
    except Exception:
        file_handle.seek(0)
        return ';'

def find_target_col(header, priorities):
    for p in priorities:
        if p in header: return p
    return None

def process_saeb_year(year, zip_filename):
    print(f"\n[INFO] Processing SAEB {year}...")
    zip_path = os.path.join(DATA_RAW, zip_filename)
    
    if not os.path.exists(zip_path):
        print(f"   [SKIP] File not found: {zip_path}")
        return

    try:
        with zipfile.ZipFile(zip_path, 'r') as z:
            # Prefer School (aggregated) data
            target_file = next((f for f in z.namelist() if 'TS_ESCOLA' in f and f.endswith('.csv')), None)
            
            if not target_file:
                print("   [ERROR] TS_ESCOLA missing.")
                return
            
            with z.open(target_file) as f:
                # 1. Structure Detection
                sep = detect_separator(f)
                header = pd.read_csv(f, sep=sep, encoding='latin1', nrows=0).columns.tolist()
                
                # 2. Identify Columns
                col_uf = find_target_col(header, UF_COLS)
                col_lp = find_target_col(header, PRIORITY_LP)
                col_mt = find_target_col(header, PRIORITY_MT)
                
                # Context (Optional but Recommended)
                col_ses = find_target_col(header, CONTEXT_COLS['SES'])
                col_type = find_target_col(header, CONTEXT_COLS['TYPE'])
                
                if not all([col_uf, col_lp, col_mt]):
                    print(f"   [CRITICAL] Missing mandatory columns (UF/Scores).")
                    return
                
                print(f"   [COLS] Math='{col_mt}' | SES='{col_ses}' | Type='{col_type}'")
                
                # 3. Load Data
                load_cols = [col_uf, col_lp, col_mt]
                if col_ses: load_cols.append(col_ses)
                if col_type: load_cols.append(col_type)
                
                f.seek(0)
                df = pd.read_csv(f, sep=sep, encoding='latin1', usecols=load_cols)
                
                # 4. Standardize Names
                df = df.rename(columns={col_uf: 'UF_ID', col_lp: 'Language_Mean', col_mt: 'Math_Mean'})
                if col_ses: df = df.rename(columns={col_ses: 'SES_Raw'})
                if col_type: df = df.rename(columns={col_type: 'School_Type'})
                
                # 5. Data Cleaning
                # Scores to Float
                for c in ['Language_Mean', 'Math_Mean']:
                    if df[c].dtype == object:
                        df[c] = df[c].astype(str).str.replace(',', '.').astype(float)
                
                # SES Parsing (Extract number from "Grupo 1")
                if 'SES_Raw' in df.columns:
                    # If it's already numeric, keep it. If string, extract digits.
                    if pd.api.types.is_numeric_dtype(df['SES_Raw']):
                         df['SES_Index'] = df['SES_Raw']
                    else:
                        df['SES_Index'] = df['SES_Raw'].astype(str).str.extract(r'(\d+)').astype(float)
                
                # Public School Logic
                if 'School_Type' in df.columns:
                    # If col is IN_PUBLICA: 1=Public, 0=Private
                    # If col is ID_DEPENDENCIA_ADM: 1,2,3=Public, 4=Private
                    if 'IN_PUBLICA' in str(col_type):
                        df['Is_Public'] = df['School_Type']
                    else:
                        # Map 4->0 (Private), others->1 (Public)
                        df['Is_Public'] = np.where(df['School_Type'] == 4, 0, 1)

                # 6. Aggregation (State Level)
                # Define aggregation rules
                agg_rules = {'Math_Mean': 'mean', 'Language_Mean': 'mean'}
                if 'SES_Index' in df.columns: agg_rules['SES_Index'] = 'mean'
                if 'Is_Public' in df.columns: agg_rules['Is_Public'] = 'mean' # Returns % of public schools
                
                grouped = df.groupby('UF_ID').agg(agg_rules).reset_index()
                
                # 7. Metadata Mapping
                if pd.api.types.is_numeric_dtype(grouped['UF_ID']):
                    grouped['UF'] = grouped['UF_ID'].map(IBGE_TO_SIGLA)
                else:
                    grouped['UF'] = grouped['UF_ID']
                
                grouped['Region'] = grouped['UF_ID'].map(UF_TO_REGION)
                
                # GLOBAL SCORE CALCULATION (PURE)
                # Based ONLY on test scores, as requested.
                grouped['SAEB_General'] = (grouped['Math_Mean'] + grouped['Language_Mean']) / 2
                
                # Rename Context Columns for clarity
                if 'Is_Public' in grouped.columns:
                    grouped = grouped.rename(columns={'Is_Public': 'Public_Share'})
                
                # Reorder
                cols = ['Region', 'UF', 'SAEB_General', 'Math_Mean', 'Language_Mean']
                if 'SES_Index' in grouped.columns: cols.append('SES_Index')
                if 'Public_Share' in grouped.columns: cols.append('Public_Share')
                
                final_df = grouped[cols].sort_values('SAEB_General', ascending=False)
                
                # 8. SAFEGUARD
                if DataGuard:
                    print(f"   [AUDIT] Running DataGuard...")
                    guard = DataGuard(final_df, f"SAEB {year}")
                    guard.check_range(['Math_Mean'], 150, 450)
                    guard.check_historical_consistency('SAEB_General', 'UF')
                    guard.validate(strict=True)

                # 9. SAVE
                csv_out = os.path.join(DATA_PROCESSED, f'saeb_table_{year}.csv')
                xlsx_out = os.path.join(REPORT_XLSX, f'saeb_table_{year}.xlsx')
                
                final_df.to_csv(csv_out, index=False)
                final_df.to_excel(xlsx_out, index=False)
                
                print(f"   [SUCCESS] Saved: {os.path.basename(csv_out)}")

    except Exception as e:
        print(f"   [CRITICAL] Error: {e}")
        import traceback
        traceback.print_exc()

def main():
    print("="*60)
    print("[START] SAEB Historical Processing")
    print("="*60)
    for year, file in [(2015, 'microdados_saeb_2015.zip'), (2017, 'microdados_saeb_2017.zip')]:
        process_saeb_year(year, file)

if __name__ == "__main__":
    main()