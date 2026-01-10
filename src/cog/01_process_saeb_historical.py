"""
PROJECT:     Cognitive Capital Analysis - Brazil
SCRIPT:      src/cog/saeb_unified_pipeline.py
ROLE:        Senior Data Science Advisor
DATE:        2026-01-10 (v13.0 - Hybrid Network Logic)

DESCRIPTION:
    Unified pipeline to process SAEB Microdata.
    - FIX: Recognizes 'IN_PUBLICA' (2015/2017) and 'ID_DEPENDENCIA_ADM' (2019+).
    - LOGIC: Auto-detects if column is already binary (0/1) or categorical (1-4).
"""

import pandas as pd
import numpy as np
import os
import zipfile
import sys
import logging
import time

# --- WINDOWS TIMEOUT INPUT ---
try:
    import msvcrt
    def input_timeout(prompt, timeout=5, default=''):
        print(f"{prompt} [Auto em {timeout}s]: ", end='', flush=True)
        start_time = time.time()
        input_chars = []
        while True:
            if msvcrt.kbhit():
                char = msvcrt.getwche()
                if char == '\r': 
                    print()
                    return "".join(input_chars)
                input_chars.append(char)
                return "".join(input_chars) + input() 
            if (time.time() - start_time) > timeout:
                print(f"\n[TIMEOUT] Padrão assumido.")
                return default
            time.sleep(0.05)
except ImportError:
    def input_timeout(prompt, timeout=5, default=''):
        return input(f"{prompt} [Enter para Padrão]: ")

# --- GLOBAL CONFIG ---
BASE_PATH = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DATA_RAW = os.path.join(BASE_PATH, 'data', 'raw')
DATA_PROCESSED = os.path.join(BASE_PATH, 'data', 'processed')
REPORT_XLSX = os.path.join(BASE_PATH, 'reports', 'varcog', 'xlsx')
LOG_DIR = os.path.join(BASE_PATH, 'logs')

for p in [DATA_RAW, DATA_PROCESSED, REPORT_XLSX, LOG_DIR]:
    os.makedirs(p, exist_ok=True)

IBGE_TO_SIGLA = {
    11:'RO', 12:'AC', 13:'AM', 14:'RR', 15:'PA', 16:'AP', 17:'TO',
    21:'MA', 22:'PI', 23:'CE', 24:'RN', 25:'PB', 26:'PE', 27:'AL', 28:'SE', 29:'BA',
    31:'MG', 32:'ES', 33:'RJ', 35:'SP', 41:'PR', 42:'SC', 43:'RS',
    50:'MS', 51:'MT', 52:'GO', 53:'DF'
}

class SaebPipeline:
    def __init__(self, year, file_path, filter_network):
        self.year = year
        self.file_path = file_path
        self.filter_network = filter_network 
        
        self.column_hints = {
            2023: {'3EM': {'LP': 'MEDIA_EM_LP', 'MT': 'MEDIA_EM_MT'}},
            2021: {'3EM': {'LP': 'MEDIA_3EM_LP', 'MT': 'MEDIA_3EM_MT'}},
            2017: {'5EF': {'LP': 'MEDIA_5EF_LP', 'MT': 'MEDIA_5EF_MT'},
                   '9EF': {'LP': 'MEDIA_9EF_LP', 'MT': 'MEDIA_9EF_MT'},
                   '3EM': {'LP': 'MEDIA_3EM_LP', 'MT': 'MEDIA_3EM_MT'}}
        }

    def detect_separator(self, f_handle):
        try:
            line = f_handle.readline().decode('latin1')
            f_handle.seek(0)
            return ';' if line.count(';') > line.count(',') else ','
        except:
            f_handle.seek(0)
            return ';'

    def find_col_flexible(self, header, candidates, substring_fallback=None):
        header_upper = {h.upper(): h for h in header}
        # 1. Exact Match (Case Insensitive)
        for cand in candidates:
            if cand.upper() in header_upper: return header_upper[cand.upper()]
        # 2. Substring Match
        if substring_fallback:
            for h in header:
                if substring_fallback.upper() in h.upper(): return h
        return None

    def find_columns_heuristic(self, all_cols, grade_tag):
        upper_cols = [c.upper() for c in all_cols]
        lp_cands, mt_cands = [], []
        for original, upper in zip(all_cols, upper_cols):
            if 'MEDIA' in upper or 'PROFICIENCIA' in upper:
                if grade_tag in upper or (grade_tag == '3EM' and '_EM_' in upper):
                    if 'LP' in upper or 'LINGUA' in upper: lp_cands.append(original)
                    elif 'MT' in upper or 'MAT' in upper: mt_cands.append(original)
        
        lp = min(lp_cands, key=len) if lp_cands else None
        mt = min(mt_cands, key=len) if mt_cands else None
        return lp, mt

    def process(self):
        logging.basicConfig(filename=os.path.join(LOG_DIR, f'saeb_{self.year}.log'), level=logging.INFO, force=True)
        print(f"\n[INFO] Processing SAEB {self.year}...")
        
        try:
            with zipfile.ZipFile(self.file_path, 'r') as z:
                target = next((f for f in z.namelist() if 'TS_ESCOLA' in f and f.endswith('.csv')), None)
                if not target:
                    print(f"   [ERROR] TS_ESCOLA not found in {self.year}")
                    return

                with z.open(target) as f:
                    sep = self.detect_separator(f)
                    header = pd.read_csv(f, sep=sep, encoding='latin1', nrows=0).columns.tolist()
                    
                    if len(header) < 2:
                        sep = ',' if sep == ';' else ';'
                        f.seek(0)
                        header = pd.read_csv(f, sep=sep, encoding='latin1', nrows=0).columns.tolist()

                    # 1. Identify Network Column (ADDED IN_PUBLICA)
                    network_candidates = [
                        'IN_PUBLICA',          # Used in 2015/2017
                        'ID_DEPENDENCIA_ADM',  # Used in 2019+
                        'TP_DEPENDENCIA', 
                        'TP_DEPENDENCIA_ADM', 
                        'ID_REDE'
                    ]
                    col_adm = self.find_col_flexible(header, network_candidates, substring_fallback='DEPENDENCIA')
                    
                    if col_adm:
                        print(f"   [CHECK] Network Column Found: '{col_adm}'")
                    else:
                        print(f"   [CRITICAL] Network Column NOT Found. Headers: {header[:5]}...")

                    col_uf = self.find_col_flexible(header, ['ID_UF', 'CO_UF', 'UF', 'SG_UF'])

                    # 2. Identify Grades
                    grades_to_process = []
                    for g in ['5EF', '9EF', '3EM']:
                        lp, mt = None, None
                        if self.year in self.column_hints and g in self.column_hints[self.year]:
                            h_lp = self.column_hints[self.year][g].get('LP')
                            h_mt = self.column_hints[self.year][g].get('MT')
                            if h_lp in header and h_mt in header: lp, mt = h_lp, h_mt
                        if not (lp and mt): lp, mt = self.find_columns_heuristic(header, g)
                        if lp and mt:
                            grades_to_process.append({'grade': g, 'lp': lp, 'mt': mt})

                    if not grades_to_process:
                        print(f"   [WARN] No grade columns found for {self.year}")
                        return

                    # 3. Load Data
                    cols_to_load = [col_uf] + [x['lp'] for x in grades_to_process] + [x['mt'] for x in grades_to_process]
                    if col_adm: cols_to_load.append(col_adm)
                    
                    final_use_cols = [c for c in set(cols_to_load) if c and c in header]
                    f.seek(0)
                    df = pd.read_csv(f, sep=sep, encoding='latin1', usecols=final_use_cols)

            # --- HYBRID PUBLIC SHARE CALCULATION ---
            if col_adm and col_adm in df.columns:
                # Force numeric conversion
                df['TEMP_ADM'] = pd.to_numeric(df[col_adm], errors='coerce')
                
                # CASE A: 'IN_PUBLICA' is usually 0 or 1 already.
                if 'IN_PUBLICA' in col_adm.upper():
                    # Just ensure it's mapped 1=Public, 0=Private
                    # Sometimes IN_PUBLICA is boolean, sometimes 0/1. 
                    df['Is_Public'] = df['TEMP_ADM'].apply(lambda x: 1 if x == 1 else 0)
                
                # CASE B: 'DEPENDENCIA' usually is 1,2,3,4
                else:
                    # 1,2,3 = Public | 4 = Private
                    def map_dependency(val):
                        if val in [1, 2, 3]: return 1
                        if val == 4: return 0
                        return np.nan 
                    df['Is_Public'] = df['TEMP_ADM'].apply(map_dependency)
                
            else:
                df['Is_Public'] = np.nan

            # --- FILTERING ---
            if self.filter_network == 'PUBLIC' and col_adm:
                df = df[df['Is_Public'] == 1]
            elif self.filter_network == 'PRIVATE' and col_adm:
                df = df[df['Is_Public'] == 0]

            # Standardize UF
            if col_uf:
                if pd.api.types.is_numeric_dtype(df[col_uf]): df['UF'] = df[col_uf].map(IBGE_TO_SIGLA)
                else: df['UF'] = df[col_uf]
            else: df['UF'] = 'BR'

            # --- PROCESS SPLIT FILES ---
            for item in grades_to_process:
                c_lp, c_mt = item['lp'], item['mt']
                grade_label = item['grade']
                
                for c in [c_lp, c_mt]:
                    df[c] = pd.to_numeric(df[c].astype(str).str.replace(',', '.', regex=False), errors='coerce')

                sub = df.dropna(subset=[c_lp, c_mt]).copy()
                if sub.empty: continue

                # Aggregate
                grouped = sub.groupby('UF')[[c_lp, c_mt, 'Is_Public']].mean().reset_index()
                
                grouped.columns = ['UF', 'Language_Mean', 'Math_Mean', 'Public_Share']
                grouped['SAEB_General'] = (grouped['Language_Mean'] + grouped['Math_Mean']) / 2
                grouped['Grade'] = grade_label
                grouped['Year'] = str(self.year)
                
                cols = ['Year', 'UF', 'Grade', 'SAEB_General', 'Math_Mean', 'Language_Mean', 'Public_Share']
                final_grade_df = grouped[cols].sort_values('SAEB_General', ascending=False)

                fname = f"saeb_table_{self.year}_{grade_label}"
                if self.filter_network != 'ALL':
                    fname += f"_{self.filter_network.lower()}"

                save_path = os.path.join(REPORT_XLSX, f"{fname}.xlsx")
                final_grade_df.to_excel(save_path, index=False)
                
                print(f"   -> Saved: {fname}.xlsx (Records: {len(final_grade_df)})")
            
        except Exception as e:
            print(f"[ERROR] {e}")
            logging.error(str(e))
            import traceback
            traceback.print_exc()

def main():
    os.system('cls' if os.name == 'nt' else 'clear')
    print("=== SAEB PIPELINE V13.0 (HYBRID LOGIC) ===")

    # 1. Years
    raw = input_timeout(">> Anos (ex: 2019, 2023)", timeout=5, default="2015, 2017, 2023")
    try:
        years = [int(y.strip()) for y in raw.split(',')]
    except:
        years = [2015, 2017, 2023]

    # 2. Filter
    opt = input_timeout("\n>> Filtro (1=All, 2=Pub, 3=Priv)", timeout=5, default="1")
    filter_map = {'2': 'PUBLIC', '3': 'PRIVATE'}
    selected_filter = filter_map.get(opt.strip(), 'ALL')

    print(f"\n[CONFIG] Anos: {years} | Filtro: {selected_filter}")
    print("-" * 50)

    for y in years:
        default_path = os.path.join(DATA_RAW, f"microdados_saeb_{y}.zip")
        final_path = None
        
        if os.path.exists(default_path):
            final_path = default_path
        else:
            user_path = input(f"   >> Arq. faltante {y} (Caminho ou Enter p/ pular): ").strip().replace('"', '')
            if user_path and os.path.exists(user_path):
                final_path = user_path
            else:
                print(f"   [SKIP] {y} ignorado.")
        
        if final_path:
            SaebPipeline(y, final_path, selected_filter).process()

    print("\n[DONE] Processamento concluído.")

if __name__ == "__main__":
    main()