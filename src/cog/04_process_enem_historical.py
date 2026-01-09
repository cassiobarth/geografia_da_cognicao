"""
PROJECT:     Cognitive Capital Analysis - Brazil
SCRIPT:      src/cog/04_process_enem_historical.py
RESEARCHERS: Dr. Jose Aparecido da Silva
             Me. Cassio Dalbem Barth
DATE:        2026-01-08 (Fix v2.2: Detailed Subject Scores)

DESCRIPTION:
    Extracts historical ENEM data (2015, 2018) with full subject breakdown.
    
    IMPROVEMENTS:
    - Detailed Metrics: Extracts Math, Language, Humanities, Nature, and Essay separately.
    - Zero Handling: Converts 0.0 to NaN per subject to avoid bias.
    - Normalization: Column names standardized to English (matches PISA/SAEB scripts).

INPUT:
    - data/raw/microdados_enem_2015.zip
    - data/raw/microdados_enem_2018.zip

OUTPUT:
    - data/processed/enem_table_2015.csv / .xlsx
    - data/processed/enem_table_2018.csv / .xlsx
"""

import pandas as pd
import zipfile
import os
import sys
import numpy as np
from pathlib import Path

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

YEARS = [2015, 2018]

POSSIBLE_UF_COLS = [
    'SG_UF_RESIDENCIA', 'UF_RESIDENCIA', 'NO_UF_RESIDENCIA',
    'SG_UF_ESC', 'UF_ESC'
]

# Map original columns to Standard English names
SCORE_MAP = {
    'NU_NOTA_MT': 'Math',
    'NU_NOTA_LC': 'Language',
    'NU_NOTA_CH': 'Humanities',
    'NU_NOTA_CN': 'Natural_Sciences',
    'NU_NOTA_REDACAO': 'Essay'
}
POSSIBLE_SCORES = list(SCORE_MAP.keys())

REGIONAL_MAP = {
    'N': ['AC','AP','AM','PA','RO','RR','TO'],
    'NE': ['AL','BA','CE','MA','PB','PE','PI','RN','SE'],
    'CO': ['DF','GO','MT','MS'],
    'SE': ['ES','MG','RJ','SP'],
    'S': ['PR','RS','SC']
}
UF_TO_REGION = {uf: r for r, ufs in REGIONAL_MAP.items() for uf in ufs}

def identify_columns(header):
    """Finds UF and available Score columns."""
    uf_col = next((c for c in POSSIBLE_UF_COLS if c in header), None)
    score_cols = [c for c in POSSIBLE_SCORES if c in header]
    return uf_col, score_cols

def process_year(year):
    print("="*60)
    print(f"[START] Processing ENEM {year} (Full Breakdown)")
    print("="*60)
    
    zip_filename = f"microdados_enem_{year}.zip"
    zip_path = os.path.join(DATA_RAW, zip_filename)
    
    if not os.path.exists(zip_path):
        print(f"[SKIP] File not found: {zip_path}")
        return

    try:
        with zipfile.ZipFile(zip_path, 'r') as z:
            csv_file = next((f for f in z.namelist() 
                             if f.endswith('.csv') and 'MICRODADOS' in f and 'ITENS' not in f), None)
            
            if not csv_file:
                # Fallback size check
                csv_files = [f for f in z.namelist() if f.endswith('.csv')]
                if not csv_files: return
                csv_file = sorted(csv_files, key=lambda x: z.getinfo(x).file_size, reverse=True)[0]
            
            print(f"[FILE] Target: {csv_file}")
            
            # 1. READ HEADER
            try:
                header = pd.read_csv(z.open(csv_file), sep=';', nrows=0, encoding='latin1').columns.tolist()
                sep = ';'
            except:
                header = pd.read_csv(z.open(csv_file), sep=',', nrows=0, encoding='latin1').columns.tolist()
                sep = ','
            
            uf_col, score_cols = identify_columns(header)
            
            if not uf_col or not score_cols:
                print(f"[CRITICAL] Missing columns. Header: {header[:10]}")
                return

            print(f"[INFO] Found {len(score_cols)} subject columns. aggregating...")

            # 2. CHUNK PROCESSING
            use_cols = [uf_col] + score_cols
            chunk_size = 100000
            
            # We will store aggregated chunks here to concat later (faster than dict iteration for many cols)
            chunk_aggs = []
            
            reader = pd.read_csv(z.open(csv_file), sep=sep, encoding='latin1', 
                               usecols=use_cols, chunksize=chunk_size)
            
            batch_count = 0
            for chunk in reader:
                batch_count += 1
                if batch_count % 20 == 0:
                    print(f"       - Processed {batch_count * chunk_size // 1000}k rows...", end='\r')
                
                # A. Convert to numeric
                for col in score_cols:
                    chunk[col] = pd.to_numeric(chunk[col], errors='coerce')
                
                # B. Handle Zeros (Inflation Fix)
                chunk[score_cols] = chunk[score_cols].replace(0, np.nan)
                
                # C. Calculate General Mean (Row-wise)
                chunk['Mean_General'] = chunk[score_cols].mean(axis=1)
                
                # D. Rename to English (e.g. NU_NOTA_MT -> Math)
                chunk = chunk.rename(columns=SCORE_MAP)
                
                # E. Update score_cols list to new names
                current_scores = [SCORE_MAP[c] for c in score_cols] + ['Mean_General']
                
                # F. Group and Sum/Count
                # We aggregate Sum and Count for ALL columns separately
                grouped = chunk.groupby(uf_col)[current_scores].agg(['sum', 'count'])
                chunk_aggs.append(grouped)

            print(f"\n[INFO] Aggregation complete. Consolidating chunks...")

            # 3. CONSOLIDATE
            # Concatenate all partial sums/counts
            full_agg = pd.concat(chunk_aggs)
            
            # Sum the partial sums and counts by UF
            final_agg = full_agg.groupby(level=0).sum()
            
            # Calculate final Weighted Means
            results = []
            # Columns are MultiIndex: (Subject, Stat) -> e.g. ('Math', 'sum')
            subjects = [c[0] for c in final_agg.columns.unique()]
            
            # Calculate means
            df_means = pd.DataFrame(index=final_agg.index)
            for subj in set([x[0] for x in final_agg.columns]):
                # Sum / Count
                df_means[subj] = final_agg[(subj, 'sum')] / final_agg[(subj, 'count')]
            
            # Add Student Count (max count across subjects approx)
            df_means['Student_Count'] = final_agg[('Mean_General', 'count')]
            
            df_final = df_means.reset_index().rename(columns={uf_col: 'UF'})
            
            # 4. ENRICHMENT
            df_final['Region'] = df_final['UF'].map(UF_TO_REGION)
            
            # Reorder columns nicely
            target_order = ['Region', 'UF', 'Mean_General', 'Student_Count', 'Math', 'Language', 'Humanities', 'Natural_Sciences', 'Essay']
            # Filter only columns that exist (in case 2015 lacks something)
            final_cols = [c for c in target_order if c in df_final.columns]
            df_final = df_final[final_cols].sort_values('Mean_General', ascending=False)

            # 5. SAFEGUARD
            if DataGuard:
                print("[AUDIT] Running DataGuard...")
                guard = DataGuard(df_final, f"ENEM {year}")
                guard.check_range(['Math', 'Language'], 350, 800) # Subject ranges
                guard.check_historical_consistency('Mean_General', 'UF')
                guard.validate(strict=True)

            # 6. EXPORT
            csv_path = os.path.join(DATA_PROCESSED, f'enem_table_{year}.csv')
            xlsx_path = os.path.join(REPORT_XLSX, f'enem_table_{year}.xlsx')
            
            df_final.to_csv(csv_path, index=False)
            df_final.to_excel(xlsx_path, index=False)
            
            print(f"[SUCCESS] Saved detailed report: {csv_path}")

    except Exception as e:
        print(f"[CRITICAL ERROR] {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    for y in YEARS:
        process_year(y)