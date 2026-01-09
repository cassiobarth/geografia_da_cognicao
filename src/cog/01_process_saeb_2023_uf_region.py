"""
PROJECT:     Cognitive Capital Analysis - Brazil
SCRIPT:      src/cog/01_process_saeb_2023.py
RESEARCHERS: Dr. Jose Aparecido da Silva
             Me. Cassio Dalbem Barth
DATE:        2026-01-08 (Standardized v2.0)

DESCRIPTION:
    Extracts SAEB 2023 data (High School Level).
    
    CONSISTENCY UPDATES:
    - Column Names: Renamed to English (Math_Mean, Language_Mean) to match historical files.
    - Filename: Output standardized to 'saeb_table_2023.csv'.
    - Security: Integrated DataGuard for integrity checking.

INPUT:
    - data/raw/microdados_saeb_2023.zip

OUTPUT:
    - data/processed/saeb_table_2023.csv
    - reports/varcog/xlsx/saeb_table_2023.xlsx
"""

import pandas as pd
import numpy as np
import os
import sys
import zipfile
import matplotlib.pyplot as plt
import seaborn as sns

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
REPORT_IMG = os.path.join(BASE_PATH, 'reports', 'varcog', 'graficos')

os.makedirs(DATA_PROCESSED, exist_ok=True)
os.makedirs(REPORT_XLSX, exist_ok=True)
os.makedirs(REPORT_IMG, exist_ok=True)

# MAPPINGS
IBGE_TO_SIGLA = {
    11:'RO', 12:'AC', 13:'AM', 14:'RR', 15:'PA', 16:'AP', 17:'TO',
    21:'MA', 22:'PI', 23:'CE', 24:'RN', 25:'PB', 26:'PE', 27:'AL', 28:'SE', 29:'BA',
    31:'MG', 32:'ES', 33:'RJ', 35:'SP',
    41:'PR', 42:'SC', 43:'RS',
    50:'MS', 51:'MT', 52:'GO', 53:'DF'
}

REGIONAL_MAP = {
    'N': ['RO','AC','AM','RR','PA','AP','TO'],
    'NE': ['MA','PI','CE','RN','PB','PE','AL','SE','BA'],
    'SE': ['MG','ES','RJ','SP'],
    'S': ['PR','SC','RS'],
    'CO': ['MS','MT','GO','DF']
}
UF_TO_REGION = {uf: r for r, ufs in REGIONAL_MAP.items() for uf in ufs}

def load_and_process():
    print("="*60)
    print("[START] SAEB 2023 Processing")
    print("="*60)

    zip_file = os.path.join(DATA_RAW, 'microdados_saeb_2023.zip')
    
    if not os.path.exists(zip_file):
        print(f"[ERROR] File not found: {zip_file}")
        return

    try:
        with zipfile.ZipFile(zip_file) as z:
            # Find TS_ESCOLA
            target = next((f for f in z.namelist() if 'TS_ESCOLA' in f and f.endswith('.csv')), None)
            
            if not target:
                print("[ERROR] TS_ESCOLA not found inside zip.")
                return

            print(f"[FILE] Extracting: {target}")
            
            # SAEB 2023 usually strictly uses ';'
            # We specifically want High School (EM) scores
            cols_map = {
                'ID_UF': 'UF_ID',
                'MEDIA_EM_LP': 'Language_Mean',
                'MEDIA_EM_MT': 'Math_Mean'
            }
            
            with z.open(target) as f:
                df = pd.read_csv(f, sep=';', usecols=cols_map.keys(), encoding='latin1')
            
            # Rename to Standard English
            df = df.rename(columns=cols_map)
            
            # Clean Numeric
            for col in ['Language_Mean', 'Math_Mean']:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            
            df = df.dropna(subset=['Language_Mean', 'Math_Mean'])
            
            # Map Geography
            df['UF'] = df['UF_ID'].map(IBGE_TO_SIGLA)
            df['Region'] = df['UF'].map(UF_TO_REGION)
            
            # Aggregation (State Level)
            grouped = df.groupby(['Region', 'UF'])[['Math_Mean', 'Language_Mean']].mean().reset_index()
            
            # Global Score Calculation
            grouped['SAEB_General'] = (grouped['Math_Mean'] + grouped['Language_Mean']) / 2
            
            # Sort
            grouped = grouped.sort_values('SAEB_General', ascending=False)
            
            # Reorder columns
            grouped = grouped[['Region', 'UF', 'SAEB_General', 'Math_Mean', 'Language_Mean']]

            # --- SAFEGUARD ---
            if DataGuard:
                print("[AUDIT] Running DataGuard...")
                guard = DataGuard(grouped, "SAEB 2023")
                # SAEB High School averages usually between 220 and 320
                guard.check_range(['Math_Mean', 'Language_Mean'], 200, 400)
                guard.check_historical_consistency('SAEB_General', 'UF')
                guard.validate(strict=True)

            # SAVE
            csv_path = os.path.join(DATA_PROCESSED, 'saeb_table_2023.csv')
            xlsx_path = os.path.join(REPORT_XLSX, 'saeb_table_2023.xlsx')
            img_path = os.path.join(REPORT_IMG, 'ranking_saeb_2023.png')

            grouped.to_csv(csv_path, index=False)
            grouped.to_excel(xlsx_path, index=False)
            
            print(f"[SUCCESS] Data saved:")
            print(f"          CSV:  {csv_path}")
            print(f"          XLSX: {xlsx_path}")

            # Generate Graph (Optional visual check)
            plt.figure(figsize=(10, 6))
            sns.barplot(data=grouped, x='SAEB_General', y='UF', hue='Region', dodge=False)
            plt.title('SAEB 2023 Ranking (Standardized)')
            plt.tight_layout()
            plt.savefig(img_path)
            print(f"          IMG:  {img_path}")

    except Exception as e:
        print(f"[CRITICAL ERROR] {e}")
        # import traceback; traceback.print_exc()

if __name__ == "__main__":
    load_and_process()