"""
================================================================================
PROJECT:    Geography of Cognition: Poverty, Wealth, and Inequalities in Brazil
SCRIPT:     src/cog/process_enem_unified_pipeline.py
VERSION:    4.3 (Fix: Force Proxy Generation & Debug Columns)
DATE:       2026-01-29
--------------------------------------------------------------------------------
PRINCIPAL INVESTIGATOR:  Dr. José Aparecido da Silva
LEAD DATA SCIENTIST:     Me. Cássio Dalbem Barth
SOURCE:                  INEP Microdata (ENEM)
================================================================================

ABSTRACT:
    Unified ETL pipeline for processing INEP ENEM student-level microdata.
    
    Update v4.3:
    - Expanded column mapping for 'SCHOOL_ID' to ensure PROXY works on 2015.
    - Added explicit debugging messages if a filter is skipped.
    - Fixed logic for Option 3 (BOTH) to ensure it attempts both filters independently.

RASTREABILITY SETTINGS:
    - INPUT_ROOT:  data/raw/enem/
    - OUTPUT_CSV:  data/processed/testes/enem_table_[year]_[filter].csv

DEPENDENCIES:
    pandas, numpy, zipfile, os
================================================================================
"""

import pandas as pd
import numpy as np
import os
import zipfile
import time
import warnings
import sys

warnings.filterwarnings("ignore")

# --- GLOBAL CONFIG ---
BASE_PATH = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DATA_RAW = os.path.join(BASE_PATH, 'data', 'raw', 'enem')
DATA_PROCESSED = os.path.join(BASE_PATH, 'data', 'processed', 'testes')
REPORT_XLSX = os.path.join(BASE_PATH, 'reports', 'varcog', 'xlsx')
LOG_DIR = os.path.join(BASE_PATH, 'logs')

for p in [DATA_RAW, DATA_PROCESSED, REPORT_XLSX, LOG_DIR]:
    os.makedirs(p, exist_ok=True)

# Mapeamento expandido para garantir que pegue 2015
TARGET_COLS = {
    'UF': ['SG_UF_PROVA', 'UF_PROVA', 'SG_UF_RESIDENCIA', 'NO_UF_PROVA'],
    'SCHOOL_ID': ['CO_ESCOLA', 'ID_ESCOLA', 'COD_ESCOLA', 'PK_COD_ENTIDADE', 'CO_ENTIDADE', 'NO_ENTIDADE'],
    'STATUS': ['TP_ST_CONCLUSAO', 'IN_TP_ENSINO', 'SITUACAO_CONCLUSAO'],
    'CN': ['NU_NOTA_CN', 'NOTA_CN'],
    'CH': ['NU_NOTA_CH', 'NOTA_CH'],
    'LC': ['NU_NOTA_LC', 'NOTA_LC'],
    'MT': ['NU_NOTA_MT', 'NOTA_MT'],
    'RED': ['NU_NOTA_REDACAO', 'NU_NOTA_RED']
}

UF_REGION_MAP = {
    'RO': 'Norte', 'AC': 'Norte', 'AM': 'Norte', 'RR': 'Norte', 'PA': 'Norte', 'AP': 'Norte', 'TO': 'Norte',
    'MA': 'Nordeste', 'PI': 'Nordeste', 'CE': 'Nordeste', 'RN': 'Nordeste', 'PB': 'Nordeste', 
    'PE': 'Nordeste', 'AL': 'Nordeste', 'SE': 'Nordeste', 'BA': 'Nordeste',
    'MG': 'Sudeste', 'ES': 'Sudeste', 'RJ': 'Sudeste', 'SP': 'Sudeste',
    'PR': 'Sul', 'SC': 'Sul', 'RS': 'Sul',
    'MS': 'Centro-Oeste', 'MT': 'Centro-Oeste', 'GO': 'Centro-Oeste', 'DF': 'Centro-Oeste'
}

# --- UTILS (INPUT) ---
try:
    import msvcrt
    def input_timeout(prompt, timeout=10, default=''):
        sys.stdout.write(f"{prompt} [Automático em {timeout}s]: ")
        sys.stdout.flush()
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
        self.user_cols = user_cols

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
                print(f"   Arquivo alvo no ZIP: {target_filename}")
                
                with z.open(target_filename) as f:
                    # Detecção de Separador e Header
                    first_line = f.readline().decode('latin1')
                    sep = ';' if first_line.count(';') > first_line.count(',') else ','
                    f.seek(0)
                    header = pd.read_csv(f, sep=sep, encoding='latin1', nrows=0).columns.tolist()
                    
                    # Debug Columns
                    print(f"   Header detectado ({len(header)} colunas): {header[:5]} ...")
                    
                    col_map = {self.find_col_flexible(header, v): k for k, v in TARGET_COLS.items() if self.find_col_flexible(header, v)}
                    print(f"   Colunas mapeadas: {list(col_map.values())}")
                    
                    # Lógica de Modos (Agora com aviso explícito de falha)
                    modes = []
                    
                    # STRICT (Requer STATUS)
                    if self.filter_choice in ['STRICT', 'BOTH']:
                        if 'STATUS' in col_map.values():
                            modes.append('STRICT')
                        else:
                            print("   [ERRO] Pulo STRICT: Coluna de 'STATUS' (TP_ST_CONCLUSAO) não encontrada.")

                    # PROXY (Requer SCHOOL_ID)
                    if self.filter_choice in ['PROXY', 'BOTH']:
                        if 'SCHOOL_ID' in col_map.values():
                            modes.append('PROXY')
                        else:
                            print("   [ERRO] Pulo PROXY: Coluna de 'SCHOOL_ID' (CO_ESCOLA) não encontrada.")
                            print(f"          Procurei por: {TARGET_COLS['SCHOOL_ID']}")

                    # NONE (Requer nada específico além das notas)
                    if self.filter_choice == 'NONE': 
                        modes.append('NONE')

                    if not modes:
                        print(f"   [ABORTADO] Nenhuma coluna compatível encontrada para o filtro selecionado.")
                        return

                    for mode in modes:
                        print(f"   -> Executando Filtro: {mode}...")
                        f.seek(0)
                        reader = pd.read_csv(f, sep=sep, encoding='latin1', usecols=list(col_map.keys()), chunksize=500000)
                        agg_storage = []
                        score_cols = ['CN', 'CH', 'LC', 'MT', 'RED']

                        for chunk in reader:
                            chunk = chunk.rename(columns=col_map)
                            
                            # FILTROS
                            if mode == 'STRICT':
                                chunk = chunk[chunk['STATUS'] == 2].copy()
                            elif mode == 'PROXY':
                                # Garante que não é nulo e não é zero
                                chunk = chunk[chunk['SCHOOL_ID'].notna()]
                                chunk = chunk[chunk['SCHOOL_ID'] != 0].copy()
                            elif mode == 'NONE':
                                pass 
                            
                            if chunk.empty: continue
                            
                            valid_scores = [c for c in score_cols if c in chunk.columns]
                            chunk[valid_scores] = chunk[valid_scores].apply(pd.to_numeric, errors='coerce')
                            chunk['Média_Geral'] = chunk[valid_scores].mean(axis=1)
                            chunk['N_Alunos'] = 1 
                            
                            agg_chunk = chunk.groupby('UF')[valid_scores + ['Média_Geral', 'N_Alunos']].agg(['sum'])
                            agg_storage.append(agg_chunk)

                        if not agg_storage:
                            print(f"   [AVISO] Nenhum dado restou após filtragem ({mode}). Verifique se os dados contêm a informação necessária.")
                            continue

                        full_agg = pd.concat(agg_storage).groupby(level=0).sum()
                        final_df = pd.DataFrame(index=full_agg.index)
                        total_n = full_agg[('N_Alunos', 'sum')]
                        
                        for col in full_agg.columns.levels[0]:
                            final_df[col] = full_agg[(col, 'sum')] if col == 'N_Alunos' else full_agg[(col, 'sum')] / total_n

                        final_df = final_df.reset_index()
                        final_df['Região'] = final_df['UF'].map(UF_REGION_MAP)
                        final_df['Ano'] = self.year
                        
                        filter_tag = "ALL_CANDIDATES" if mode == 'NONE' else f"{mode}_3EM"
                        final_df['Filtro'] = filter_tag
                        
                        rename_dict = {
                            'CN': 'Ciências_Natureza', 'CH': 'Ciências_Humanas', 
                            'LC': 'Linguagens', 'MT': 'Matemática', 'RED': 'Redação'
                        }
                        final_df = final_df.rename(columns=rename_dict)
                        
                        all_cols_ptbr = ['Ano', 'Região', 'UF', 'Filtro', 'Média_Geral', 'N_Alunos'] + [rename_dict[c] for c in score_cols if c in rename_dict]
                        
                        if self.user_cols:
                            cols_to_keep = [c for c in all_cols_ptbr if c in self.user_cols]
                            if not cols_to_keep: cols_to_keep = all_cols_ptbr
                        else:
                            cols_to_keep = all_cols_ptbr

                        final_df = final_df[cols_to_keep]
                        if 'Média_Geral' in final_df.columns:
                            final_df = final_df.sort_values('Média_Geral', ascending=False)
                        
                        fname = f"enem_table_{self.year}_{filter_tag}"
                        final_df.to_csv(os.path.join(DATA_PROCESSED, f"{fname}.csv"), index=False)
                        final_df.to_excel(os.path.join(REPORT_XLSX, f"{fname}.xlsx"), index=False)
                        
                        n_count = int(total_n.sum())
                        print(f"      [OK] Arquivo gerado: {fname} | N: {n_count}")

        except Exception as e:
            print(f"   [ERRO CRÍTICO] {e}")

def main():
    os.system('cls' if os.name == 'nt' else 'clear')
    print("=== ENEM UNIFIED PIPELINE v4.3 ===")
    
    # 1. Anos
    raw_years = input_timeout(">> Anos (ex: 2015, 2022)", timeout=10, default="2015")
    try:
        years = [int(y.strip()) for y in raw_years.split(',') if y.strip()]
    except:
        years = [2015]
    
    # 2. Metodologia
    print("\nMETODOLOGIA:")
    print("1. STRICT (Concluintes) | 2. PROXY (Vínculo) | 3. AMBOS | 4. SEM FILTRO (Total)")
    
    raw_f = input_timeout(">> Selecione o Filtro", timeout=10, default="3")
    f_map = {'1': 'STRICT', '2': 'PROXY', '3': 'BOTH', '4': 'NONE'}
    selected_filter = f_map.get(raw_f, 'BOTH')

    # 3. Colunas
    print("\nCOLUNAS (Enter para TODAS):")
    raw_cols = input_timeout(">> Digite colunas", timeout=5, default="TODAS")
    user_cols_list = None if raw_cols == "TODAS" else [c.strip() for c in raw_cols.split(',')]
    
    print("-" * 60)

    for y in years:
        path = os.path.join(DATA_RAW, f"microdados_enem_{y}.zip")
        if os.path.exists(path):
            EnemPipeline(y, path, selected_filter, user_cols_list).process()
        else:
            print(f"[PULAR] Arquivo não encontrado: {path}")

    print("\n[CONCLUÍDO] Pipeline finalizado.")

if __name__ == "__main__":
    main()