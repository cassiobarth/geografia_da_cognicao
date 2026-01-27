"""
================================================================================
PROJECT:    Geography of Cognition: Poverty, Wealth, and Inequalities in Brazil
SCRIPT:     src/cog/saeb_unified_pipeline.py
VERSION:    14.7 (Production - Column Selection / Student Count / PT-BR)
DATE:       2026-01-26
--------------------------------------------------------------------------------
PRINCIPAL INVESTIGATOR:  Dr. José Aparecido da Silva
LEAD DATA SCIENTIST:     Me. Cássio Dalbem Barth
SOURCE:                  INEP Microdata (SAEB / Prova Brasil). Available at:
https://www.gov.br/inep/pt-br/acesso-a-informacao/dados-abertos/microdados/saeb
================================================================================

ABSTRACT:
    Unified ETL pipeline for processing SAEB school-level microdata.
    
    Key Features v14.7:
    - Interactive Column Selection (Matches ENEM v4.0 protocol).
    - Network Filtering (Public/Private/All).
    - Student Count (N) Extraction.
    - Full PT-BR Output.

RASTREABILITY SETTINGS:
    - INPUT_ROOT:  data/raw/saeb/
    - OUTPUT_CSV:  data/processed/saeb_table_[year]_[grade].csv
    - LOG_FILE:    logs/saeb_pipeline_[year].log

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
DATA_RAW = os.path.join(BASE_PATH, 'data', 'raw', 'saeb')
DATA_PROCESSED = os.path.join(BASE_PATH, 'data', 'processed')
REPORT_XLSX = os.path.join(BASE_PATH, 'reports', 'varcog', 'xlsx') 
LOG_DIR = os.path.join(BASE_PATH, 'logs')

for p in [DATA_RAW, DATA_PROCESSED, LOG_DIR, REPORT_XLSX]: 
    os.makedirs(p, exist_ok=True)

IBGE_TO_SIGLA = {
    11:'RO', 12:'AC', 13:'AM', 14:'RR', 15:'PA', 16:'AP', 17:'TO',
    21:'MA', 22:'PI', 23:'CE', 24:'RN', 25:'PB', 26:'PE', 27:'AL', 28:'SE', 29:'BA',
    31:'MG', 32:'ES', 33:'RJ', 35:'SP', 41:'PR', 42:'SC', 43:'RS',
    50:'MS', 51:'MT', 52:'GO', 53:'DF'
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

class SaebPipeline:
    def __init__(self, year, file_path, filter_network, user_cols=None):
        self.year = year
        self.file_path = file_path
        self.filter_network = filter_network
        self.user_cols = user_cols

    def get_quantity_column(self, header, grade):
        header_upper = {h.upper(): h for h in header}
        candidates = [
            f"NU_PRESENTES_{grade}", f"NU_PRESENTES_{grade}_LP", 
            f"QTD_ALUNOS_{grade}", f"N_ALUNOS_{grade}", "NU_PRESENTES"
        ]
        if grade == '3EM': candidates += ["NU_PRESENTES_EM", "NU_PRESENTES_EM_LP"]
        
        for cand in candidates:
            if cand in header_upper: return header_upper[cand]
        return None

    def find_grade_columns(self, header, grade):
        lp = next((h for h in header if ('MEDIA' in h.upper() or 'PROFICIENCIA' in h.upper()) and grade in h.upper() and ('LP' in h.upper() or 'LINGUA' in h.upper())), None)
        mt = next((h for h in header if ('MEDIA' in h.upper() or 'PROFICIENCIA' in h.upper()) and grade in h.upper() and ('MT' in h.upper() or 'MAT' in h.upper())), None)
        qty = self.get_quantity_column(header, grade)
        
        if not (lp and mt) and grade == '3EM':
            lp = next((h for h in header if 'MEDIA' in h.upper() and '_EM_' in h.upper() and 'LP' in h.upper()), None)
            mt = next((h for h in header if 'MEDIA' in h.upper() and '_EM_' in h.upper() and 'MT' in h.upper()), None)
            if not qty: qty = self.get_quantity_column(header, "EM")
        return lp, mt, qty

    def process(self):
        print(f"\n[INÍCIO] Processando SAEB {self.year}...")
        try:
            with zipfile.ZipFile(self.file_path, 'r') as z:
                target = next((f for f in z.namelist() if 'TS_ESCOLA' in f and f.endswith('.csv')), None)
                if not target:
                    print(f"   [ERRO] TS_ESCOLA não encontrado no ZIP.")
                    return

                with z.open(target) as f:
                    first_line = f.readline().decode('latin1')
                    sep = ';' if first_line.count(';') > first_line.count(',') else ','
                    f.seek(0)
                    header = pd.read_csv(f, sep=sep, encoding='latin1', nrows=0).columns.tolist()
                    
                    col_adm = next((h for h in header if any(x in h.upper() for x in ['ID_DEPENDENCIA_ADM', 'IN_PUBLICA', 'ID_REDE', 'TP_DEPENDENCIA'])), None)
                    col_uf = next((h for h in header if any(x in h.upper() for x in ['ID_UF', 'CO_UF', 'UF', 'SG_UF'])), None)

                    processed_grades = 0
                    for grade in ['9EF', '3EM']:
                        c_lp, c_mt, c_qty = self.find_grade_columns(header, grade)
                        if not (c_lp and c_mt): continue 

                        f.seek(0)
                        cols = [c for c in [col_uf, col_adm, c_lp, c_mt, c_qty] if c]
                        df = pd.read_csv(f, sep=sep, encoding='latin1', usecols=cols)

                        # 1. Filtro de Rede
                        if col_adm:
                            df['TEMP_ADM'] = pd.to_numeric(df[col_adm], errors='coerce')
                            df['Is_Public'] = df['TEMP_ADM'].apply(lambda x: 0 if x == 4 else 1)
                        else:
                            df['Is_Public'] = 1
                        
                        if self.filter_network == 'PUBLIC': df = df[df['Is_Public'] == 1]
                        elif self.filter_network == 'PRIVATE': df = df[df['Is_Public'] == 0]

                        # 2. Normalização
                        df['UF'] = df[col_uf].map(IBGE_TO_SIGLA) if pd.api.types.is_numeric_dtype(df[col_uf]) else df[col_uf]
                        
                        for c in [c_lp, c_mt]:
                            df[c] = pd.to_numeric(df[c].astype(str).str.replace(',', '.'), errors='coerce')
                        
                        df['N_Alunos'] = pd.to_numeric(df[c_qty], errors='coerce').fillna(0) if c_qty else 0
                        
                        # 3. Agregação
                        sub = df.dropna(subset=[c_lp, c_mt]).copy()
                        if sub.empty: continue

                        # Média Ponderada pelo N da Escola (Importante para SAEB)
                        # Nota: Se N_Alunos for 0 (dados faltantes), usa média simples
                        if sub['N_Alunos'].sum() > 0:
                            agg = sub.groupby('UF').apply(
                                lambda x: pd.Series({
                                    'Média_Port': np.average(x[c_lp], weights=x['N_Alunos']) if x['N_Alunos'].sum() > 0 else x[c_lp].mean(),
                                    'Média_Mat': np.average(x[c_mt], weights=x['N_Alunos']) if x['N_Alunos'].sum() > 0 else x[c_mt].mean(),
                                    'N_Alunos': x['N_Alunos'].sum()
                                })
                            ).reset_index()
                        else:
                            agg = sub.groupby('UF').agg({c_lp: 'mean', c_mt: 'mean', 'N_Alunos': 'sum'}).reset_index()
                            agg.rename(columns={c_lp: 'Média_Port', c_mt: 'Média_Mat'}, inplace=True)

                        agg['Média_Geral'] = (agg['Média_Port'] + agg['Média_Mat']) / 2
                        agg['Região'] = agg['UF'].map(UF_REGION_MAP)
                        agg['Ano'] = self.year
                        agg['Série'] = grade
                        agg['Rede'] = self.filter_network

                        # 4. Seleção de Colunas Dinâmica
                        all_cols = ['Ano', 'Região', 'UF', 'Rede', 'Série', 'Média_Geral', 'Média_Mat', 'Média_Port', 'N_Alunos']
                        
                        if self.user_cols:
                            cols_to_keep = [c for c in all_cols if c in self.user_cols]
                            if not cols_to_keep: cols_to_keep = all_cols
                        else:
                            cols_to_keep = all_cols

                        # Ordenação
                        final_df = agg[cols_to_keep]
                        if 'Média_Geral' in final_df.columns:
                            final_df = final_df.sort_values('Média_Geral', ascending=False)
                        
                        # 5. Output
                        base_name = f"saeb_table_{self.year}_{grade}"
                        final_df.to_csv(os.path.join(DATA_PROCESSED, f"{base_name}.csv"), index=False)
                        final_df.to_excel(os.path.join(REPORT_XLSX, f"{base_name}.xlsx"), index=False)
                        print(f"   -> Gerado: {base_name} | Alunos: {int(agg['N_Alunos'].sum())}")
                        processed_grades += 1
                    
                    if processed_grades == 0:
                        print("   [AVISO] Nenhuma série processada (verifique filtros ou arquivo).")

        except Exception as e:
            print(f"   [ERRO] {e}")

def main():
    os.system('cls' if os.name == 'nt' else 'clear')
    print("=== SAEB UNIFIED PIPELINE v14.7 ===")
    
    # 1. Anos
    raw_years = input_timeout(">> Anos (ex: 2015, 2023)", timeout=10, default="2015, 2017, 2019, 2021, 2023")
    try:
        years = [int(y.strip()) for y in raw_years.split(',') if y.strip()]
        if not years: raise ValueError
    except:
        years = [2015, 2017, 2019, 2021, 2023]

    # 2. Filtro de Rede
    print("\nREDE DE ENSINO:")
    print("1. PÚBLICA (Padrão) | 2. PRIVADA | 3. TOTAL")
    raw_f = input_timeout(">> Selecione a Rede", timeout=10, default="1")
    f_map = {'1': 'PUBLIC', '2': 'PRIVATE', '3': 'ALL'}
    selected_filter = f_map.get(raw_f, 'PUBLIC')

    # 3. Seleção de Colunas
    print("\nCOLUNAS DISPONÍVEIS:")
    print("[Ano, Região, UF, Rede, Série, Média_Geral, Média_Mat, Média_Port, N_Alunos]")
    
    raw_cols = input_timeout(">> Digite as colunas desejadas", timeout=10, default="TODAS")
    
    if raw_cols == "TODAS":
        user_cols_list = None
        print(f"\n[CONFIG] Anos: {years} | Rede: {selected_filter} | Colunas: TODAS")
    else:
        user_cols_list = [c.strip() for c in raw_cols.split(',')]
        print(f"\n[CONFIG] Anos: {years} | Rede: {selected_filter} | Colunas: {len(user_cols_list)} selecionadas")
    
    print("-" * 60)

    for y in years:
        path = os.path.join(DATA_RAW, f"microdados_saeb_{y}.zip")
        if os.path.exists(path):
            SaebPipeline(y, path, selected_filter, user_cols_list).process()
        else:
            # Tenta nome alternativo comum
            path_alt = os.path.join(DATA_RAW, f"TS_ESCOLA_{y}.zip")
            if os.path.exists(path_alt):
                SaebPipeline(y, path_alt, selected_filter, user_cols_list).process()
            else:
                print(f"[PULAR] Faltando: microdados_saeb_{y}.zip")

    print("\n[CONCLUÍDO] Processos SAEB finalizados.")

if __name__ == "__main__":
    main()