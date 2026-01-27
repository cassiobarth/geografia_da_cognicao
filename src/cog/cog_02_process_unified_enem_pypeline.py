"""
================================================================================
PROJECT:    Geography of Cognition: Poverty, Wealth, and Inequalities in Brazil
SCRIPT:     src/cog/cog_02_process_enem_unified_pipeline.py
VERSION:    4.0 (Production - Column Selection / Multi-Filter / PT-BR)
DATE:       2026-01-26
--------------------------------------------------------------------------------
PRINCIPAL INVESTIGATOR:  Dr. José Aparecido da Silva
LEAD DATA SCIENTIST:     Me. Cássio Dalbem Barth
SOURCE:                  INEP Microdata (ENEM). Available at:
https://www.gov.br/inep/pt-br/acesso-a-informacao/dados-abertos/microdados/enem
================================================================================

ABSTRACT:
    Unified ETL pipeline for processing INEP ENEM student-level microdata. 
    
    Key Features v4.0:
    - Interactive Column Selection (User defines output schema).
    - Multi-Filter Support (Strict/Proxy/Both).
    - Student Count (N) Extraction.
    - Full PT-BR Output.

RASTREABILITY SETTINGS:
    - INPUT_ROOT:  data/raw/enem/
    - OUTPUT_CSV:  data/processed/enem_table_[year]_[filter].csv
    - LOG_FILE:    logs/enem_pipeline_[year].log

DEPENDENCIES:
    pandas, numpy, zipfile, logging, os
================================================================================
"""

import pandas as pd
import numpy as np
import os
import zipfile
import logging
import time
import warnings

warnings.filterwarnings("ignore")

# --- GLOBAL CONFIG ---
BASE_PATH = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DATA_RAW = os.path.join(BASE_PATH, 'data', 'raw', 'enem')
DATA_PROCESSED = os.path.join(BASE_PATH, 'data', 'processed')
REPORT_XLSX = os.path.join(BASE_PATH, 'reports', 'varcog', 'xlsx')
LOG_DIR = os.path.join(BASE_PATH, 'logs')

for p in [DATA_RAW, DATA_PROCESSED, REPORT_XLSX, LOG_DIR]:
    os.makedirs(p, exist_ok=True)

TARGET_COLS = {
    'UF': ['SG_UF_PROVA', 'UF_PROVA', 'SG_UF_RESIDENCIA'],
    'SCHOOL_ID': ['CO_ESCOLA', 'ID_ESCOLA'],
    'STATUS': ['TP_ST_CONCLUSAO'],
    'CN': ['NU_NOTA_CN'],
    'CH': ['NU_NOTA_CH'],
    'LC': ['NU_NOTA_LC'],
    'MT': ['NU_NOTA_MT'],
    'RED': ['NU_NOTA_REDACAO']
}

UF_REGION_MAP = {
    'RO': 'Norte', 'AC': 'Norte', 'AM': 'Norte', 'RR': 'Norte', 'PA': 'Norte', 'AP': 'Norte', 'TO': 'Norte',
    'MA': 'Nordeste', 'PI': 'Nordeste', 'CE': 'Nordeste', 'RN': 'Nordeste', 'PB': 'Nordeste', 
    'PE': 'Nordeste', 'AL': 'Nordeste', 'SE': 'Nordeste', 'BA': 'Nordeste',
    'MG': 'Sudeste', 'ES': 'Sudeste', 'RJ': 'Sudeste', 'SP': 'Sudeste',
    'PR': 'Sul', 'SC': 'Sul', 'RS': 'Sul',
    'MS': 'Centro-Oeste', 'MT': 'Centro-Oeste', 'GO': 'Centro-Oeste', 'DF': 'Centro-Oeste'
}

# --- WINDOWS TIMEOUT INPUT UTILITY ---
try:
    import msvcrt
    def input_timeout(prompt, timeout=10, default=''):
        print(f"{prompt} [Automático em {timeout}s]: ", end='', flush=True)
        start_time = time.time()
        input_chars = []
        while True:
            if msvcrt.kbhit():
                char = msvcrt.getwche()
                if char == '\r': 
                    print()
                    res = "".join(input_chars).strip()
                    return res if res else default
                input_chars.append(char)
                print(char, end='', flush=True)
            if (time.time() - start_time) > timeout:
                print(f"\n[TIMEOUT] Usando padrão: {default}")
                return default
            time.sleep(0.05)
except ImportError:
    def input_timeout(prompt, timeout=10, default=''):
        res = input(f"{prompt} [Enter para Padrão {default}]: ").strip()
        return res if res else default

class EnemPipeline:
    def __init__(self, year, file_path, filter_choice, user_cols=None):
        self.year = year
        self.file_path = file_path
        self.filter_choice = filter_choice
        self.user_cols = user_cols # Lista de colunas desejadas

    def get_largest_csv(self, z):
        csv_files = [f for f in z.namelist() if f.lower().endswith('.csv')]
        return sorted(csv_files, key=lambda x: z.getinfo(x).file_size, reverse=True)[0] if csv_files else None

    def find_col_flexible(self, header, candidates):
        header_upper = {h.upper(): h for h in header}
        for cand in candidates:
            if cand.upper() in header_upper: return header_upper[cand.upper()]
        return None

    def process(self):
        print(f"\n[INÍCIO] Processando ENEM {self.year}...")
        try:
            with zipfile.ZipFile(self.file_path, 'r') as z:
                target_filename = self.get_largest_csv(z)
                with z.open(target_filename) as f:
                    first_line = f.readline().decode('latin1')
                    sep = ';' if first_line.count(';') > first_line.count(',') else ','
                    f.seek(0)
                    header = pd.read_csv(f, sep=sep, encoding='latin1', nrows=0).columns.tolist()
                    
                    col_map = {self.find_col_flexible(header, v): k for k, v in TARGET_COLS.items() if self.find_col_flexible(header, v)}
                    
                    modes = []
                    if self.filter_choice in ['STRICT', 'BOTH'] and 'STATUS' in col_map.values(): modes.append('STRICT')
                    if self.filter_choice in ['PROXY', 'BOTH'] and 'SCHOOL_ID' in col_map.values(): modes.append('PROXY')

                    if not modes:
                        print(f"   [AVISO] Filtros não suportados para {self.year}.")
                        return

                    for mode in modes:
                        print(f"   -> Executando Filtro: {mode}")
                        f.seek(0)
                        reader = pd.read_csv(f, sep=sep, encoding='latin1', usecols=list(col_map.keys()), chunksize=500000)
                        agg_storage = []
                        score_cols = ['CN', 'CH', 'LC', 'MT', 'RED']

                        for chunk in reader:
                            chunk = chunk.rename(columns=col_map)
                            chunk = chunk[chunk['STATUS'] == 2].copy() if mode == 'STRICT' else chunk[chunk['SCHOOL_ID'].notna()].copy()
                            if chunk.empty: continue
                            
                            valid_scores = [c for c in score_cols if c in chunk.columns]
                            chunk[valid_scores] = chunk[valid_scores].apply(pd.to_numeric, errors='coerce')
                            chunk['Média_Geral'] = chunk[valid_scores].mean(axis=1)
                            chunk['N_Alunos'] = 1 
                            
                            agg_chunk = chunk.groupby('UF')[valid_scores + ['Média_Geral', 'N_Alunos']].agg(['sum'])
                            agg_storage.append(agg_chunk)

                        full_agg = pd.concat(agg_storage).groupby(level=0).sum()
                        final_df = pd.DataFrame(index=full_agg.index)
                        total_n = full_agg[('N_Alunos', 'sum')]
                        
                        for col in full_agg.columns.levels[0]:
                            final_df[col] = full_agg[(col, 'sum')] if col == 'N_Alunos' else full_agg[(col, 'sum')] / total_n

                        final_df = final_df.reset_index()
                        final_df['Região'] = final_df['UF'].map(UF_REGION_MAP)
                        final_df['Ano'] = self.year
                        final_df['Filtro'] = f"{mode}_3EM"
                        
                        rename_dict = {
                            'CN': 'Ciências_Natureza', 'CH': 'Ciências_Humanas', 
                            'LC': 'Linguagens', 'MT': 'Matemática', 'RED': 'Redação'
                        }
                        final_df = final_df.rename(columns=rename_dict)
                        
                        # --- FILTRAGEM DE COLUNAS (NOVA FEATURE) ---
                        all_cols_ptbr = ['Ano', 'Região', 'UF', 'Filtro', 'Média_Geral', 'N_Alunos'] + [rename_dict[c] for c in score_cols if c in rename_dict]
                        
                        # Se o usuário definiu colunas, fazemos a intersecção
                        if self.user_cols:
                            # Normaliza para garantir match (strip)
                            cols_to_keep = [c for c in all_cols_ptbr if c in self.user_cols]
                            # Se não sobrou nada (erro de digitação), volta para o padrão
                            if not cols_to_keep: 
                                cols_to_keep = all_cols_ptbr
                        else:
                            cols_to_keep = all_cols_ptbr

                        final_df = final_df[cols_to_keep]
                        # Tenta ordenar por Média_Geral se ela existir na seleção
                        if 'Média_Geral' in final_df.columns:
                            final_df = final_df.sort_values('Média_Geral', ascending=False)
                        
                        fname = f"enem_table_{self.year}_{mode}_3EM"
                        final_df.to_csv(os.path.join(DATA_PROCESSED, f"{fname}.csv"), index=False)
                        final_df.to_excel(os.path.join(REPORT_XLSX, f"{fname}.xlsx"), index=False)
                        
                        # Log seguro caso N_Alunos tenha sido removido da seleção
                        n_count = int(total_n.sum()) # Pega do total bruto
                        print(f"      [OK] Gerado: {fname} | N: {n_count}")

        except Exception as e:
            print(f"   [ERRO] {e}")

def main():
    os.system('cls' if os.name == 'nt' else 'clear')
    print("=== ENEM UNIFIED PIPELINE v4.0 ===")
    
    # 1. Anos
    raw_years = input_timeout(">> Anos (ex: 2015, 2022)", timeout=10, default="2015, 2018, 2022")
    try:
        years = [int(y.strip()) for y in raw_years.split(',') if y.strip()]
        if not years: raise ValueError
    except:
        years = [2015, 2018, 2022]
    
    # 2. Metodologia
    print("\nMETODOLOGIA:")
    print("1. STRICT (Concluintes) | 2. PROXY (Vínculo Escolar) | 3. AMBOS")
    raw_f = input_timeout(">> Selecione o Filtro", timeout=10, default="1")
    f_map = {'1': 'STRICT', '2': 'PROXY', '3': 'BOTH'}
    selected_filter = f_map.get(raw_f, 'STRICT')

    # 3. Colunas (Nova Feature)
    print("\nCOLUNAS DISPONÍVEIS:")
    print("[Ano, Região, UF, Filtro, Média_Geral, N_Alunos]")
    print("[Ciências_Natureza, Ciências_Humanas, Linguagens, Matemática, Redação]")
    
    raw_cols = input_timeout(">> Digite as colunas desejadas", timeout=10, default="TODAS")
    
    if raw_cols == "TODAS":
        user_cols_list = None
        print(f"\n[CONFIG] Anos: {years} | Filtro: {selected_filter} | Colunas: TODAS")
    else:
        user_cols_list = [c.strip() for c in raw_cols.split(',')]
        print(f"\n[CONFIG] Anos: {years} | Filtro: {selected_filter} | Colunas: {len(user_cols_list)} selecionadas")
    
    print("-" * 60)

    for y in years:
        path = os.path.join(DATA_RAW, f"microdados_enem_{y}.zip")
        if os.path.exists(path):
            EnemPipeline(y, path, selected_filter, user_cols_list).process()
        else:
            print(f"[PULAR] Faltando: {path}")

    print("\n[CONCLUÍDO] Processos finalizados.")

if __name__ == "__main__":
    main()