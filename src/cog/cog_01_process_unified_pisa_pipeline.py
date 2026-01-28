"""
================================================================================
PROJECT:         Geography of Cognition: Poverty, Wealth, and Inequalities in Brazil
SCRIPT:          src/cog/cog_04_process_pisa_unified.py
VERSION:         8.1 (Concept x Method Selection Logic)
DATE:            2026-01-27
--------------------------------------------------------------------------------
PRINCIPAL INVESTIGATOR:  Dr. José Aparecido da Silva
LEAD DATA SCIENTIST:     Me. Cássio Dalbem Barth
SOURCE:                  OECD PISA Microdata (2015, 2018, 2022)
PISA 2015: https://zenodo.org/records/13383223
PISA 2018: https://zenodo.org/records/13383115
PISA 2022: https://zenodo.org/records/13382904
================================================================================

ABSTRACT:
    Unified ETL pipeline for processing PISA student-level microdata.
    Handles architectural shifts across cycles (2015-2022).
    
    Key Features:
    - Calculates 'Cognitive_Global_Mean' (Math, Read, Science).
    - Computes BOTH Simple Mean (Raw) and Weighted Mean (OECD Standard).
    - Extracts 'N_Alunos' (Student Count) for sample robustness.
    - Standardized PT-BR output for academic reporting.

RASTREABILITY SETTINGS:
    - INPUT_ROOT:  data/raw/Pisa/
    - OUTPUT_CSV:  data/processed/pisa_table_[year]_[scope].csv
    - LOG_FILE:    logs/pisa_pipeline_[year].log

DEPENDENCIES:
    pandas, numpy, pyreadstat, openpyxl, re
================================================================================
"""

import pandas as pd
import numpy as np
import os
import sys
import time
import pyreadstat
from pathlib import Path
import warnings

warnings.filterwarnings("ignore")

# --- GLOBAL CONFIG ---
CURRENT_FILE = Path(__file__).resolve()
PROJECT_ROOT = CURRENT_FILE.parent.parent.parent
DATA_RAW_ROOT = PROJECT_ROOT / 'data' / 'raw' / 'Pisa'
REPORT_DIR = PROJECT_ROOT / 'reports' / 'varcog'
CSV_OUT_DIR = PROJECT_ROOT / 'data' / 'processed'
XLSX_OUT_DIR = REPORT_DIR / 'xlsx'
LOG_DIR = PROJECT_ROOT / 'logs'

for path in [CSV_OUT_DIR, XLSX_OUT_DIR, LOG_DIR]:
    path.mkdir(parents=True, exist_ok=True)

# --- WINDOWS TIMEOUT INPUT UTILITY (FIXED: getwch) ---
try:
    import msvcrt
    def input_timeout(prompt, timeout=10, default=''):
        print(f"{prompt} [Automático em {timeout}s]: ", end='', flush=True)
        start_time = time.time()
        input_chars = []
        while True:
            if msvcrt.kbhit():
                # Fix: Use getwch to avoid double echo on screen
                char = msvcrt.getwch()
                if char == '\r': # Enter key
                    print()
                    res = "".join(input_chars).strip()
                    return res if res else default
                input_chars.append(char)
                print(char, end='', flush=True) # Manual echo
            if (time.time() - start_time) > timeout:
                print(f"\n[TIMEOUT] Usando padrão: {default}")
                return default
            time.sleep(0.05)
except ImportError:
    def input_timeout(prompt, timeout=10, default=''):
        res = input(f"{prompt} [Enter para Padrão {default}]: ").strip()
        return res if res else default

class PisaUnifiedETL:
    def __init__(self, mode='BOTH', user_concepts=None):
        """
        :param mode: 'SIMPLE', 'WEIGHTED', 'BOTH'
        :param user_concepts: List of concept keys (e.g. ['Math', 'Global']) or None for ALL.
        """
        self.mode = mode.upper()
        self.user_concepts = user_concepts
        
        # Base Translations
        self.translate_map = {
            'Region': 'Região',
            'Math': 'Matemática', 'Math_Mean': 'Matemática',
            'Read': 'Leitura', 'Read_Mean': 'Leitura',
            'Science': 'Ciências', 'Science_Mean': 'Ciências',
            'Cognitive_Global_Mean': 'Média_Geral',
            'Student_Count': 'N_Alunos'
        }
        
        self.region_trans = {
            'North': 'Norte', 'Northeast': 'Nordeste', 
            'Southeast': 'Sudeste', 'South': 'Sul', 
            'Center-West': 'Centro-Oeste'
        }
        
        # Concept Mapping (Used for filtering)
        # Maps internal Concept Keys to PT-BR Column Substrings
        self.concept_matcher = {
            'Math': 'Matemática',
            'Read': 'Leitura',
            'Science': 'Ciências',
            'Global': 'Média_Geral',
            'Count': 'N_Alunos'
        }

    def _calc_weighted(self, df, group_col, val_cols):
        """Calculates weighted mean using W_FSTUWT (Robust to NaNs)."""
        try:
            results = []
            df['W_FSTUWT'] = pd.to_numeric(df['W_FSTUWT'], errors='coerce')
            
            for col in val_cols:
                df[col] = pd.to_numeric(df[col], errors='coerce')
                
                def weighted_avg(x):
                    valid = x[col].notna() & x['W_FSTUWT'].notna()
                    d = x.loc[valid, col]
                    w = x.loc[valid, 'W_FSTUWT']
                    if w.sum() > 0:
                        return np.average(d, weights=w)
                    return np.nan

                wm = df.groupby(group_col).apply(weighted_avg).reset_index(name=f'{col}_Ponderada')
                results.append(wm)
            
            final_w = results[0]
            for r in results[1:]: final_w = pd.merge(final_w, r, on=group_col)
            return final_w
        except Exception as e:
            # print(f"   [DEBUG] Weighted calc issue: {e}") 
            return None

    def _apply_standardization(self, df, year):
        """Applies translation and filters based on Concept x Method logic."""
        
        # 1. Translate Region
        if 'Region' in df.columns:
            df['Region'] = df['Region'].replace(self.region_trans)
        
        # 2. Rename Simple Cols
        df = df.rename(columns=self.translate_map)
        
        # 3. Rename Weighted Cols (Dynamic)
        new_cols = {}
        for col in df.columns:
            if '_Ponderada' in col:
                base = col.replace('_Ponderada', '')
                if base in self.translate_map:
                    new_cols[col] = f"{self.translate_map[base]}_Ponderada"
                elif f"{base}_Mean" in self.translate_map:
                     new_cols[col] = f"{self.translate_map[f'{base}_Mean']}_Ponderada"
        if new_cols: df = df.rename(columns=new_cols)

        df['Ano'] = int(year)
        
        # 4. Filter Logic (The Core Change)
        keep_cols = []
        all_cols = df.columns.tolist()
        base_mandatory = ['Ano', 'Região', 'UF', 'IBGE_CODE', 'N_Alunos', 'Student_Count']
        
        for col in all_cols:
            is_weighted = 'Ponderada' in col
            
            # A) Check Mandatory
            if col in base_mandatory:
                # Special check for N_Alunos if user didn't ask for it
                if col in ['N_Alunos', 'Student_Count'] and self.user_concepts and 'Count' not in self.user_concepts:
                     pass 
                keep_cols.append(col)
                continue
            
            # B) Check Concept (Subject)
            # If user_concepts is None, we keep all concepts.
            # If user_concepts has list, we check if column matches any selected concept.
            concept_match = True
            if self.user_concepts:
                concept_match = False
                for concept_key in self.user_concepts:
                    pt_term = self.concept_matcher.get(concept_key, '###')
                    if pt_term in col:
                        concept_match = True
                        break
            
            if not concept_match: continue # Skip this column

            # C) Check Method (Simple vs Weighted)
            method_match = False
            if self.mode == 'BOTH':
                method_match = True
            elif self.mode == 'WEIGHTED':
                if is_weighted: method_match = True
            elif self.mode == 'SIMPLE':
                if not is_weighted: method_match = True
            
            if method_match:
                keep_cols.append(col)
        
        # Deduplicate
        df = df[list(set(keep_cols))]

        # 5. Order
        priority = ['Ano', 'Região', 'UF', 'Média_Geral', 'Média_Geral_Ponderada', 'N_Alunos']
        remaining = sorted([c for c in df.columns if c not in priority])
        ordered = [c for c in priority if c in df.columns] + remaining
        
        final_df = df[ordered]
        
        # Sort Rows
        if 'Média_Geral' in final_df.columns:
            final_df = final_df.sort_values('Média_Geral', ascending=False)
        elif 'Média_Geral_Ponderada' in final_df.columns:
            final_df = final_df.sort_values('Média_Geral_Ponderada', ascending=False)
            
        return final_df
    
    def _save(self, df, fname):
        suffix = ""
        if self.mode == 'SIMPLE': suffix = "_simples"
        elif self.mode == 'WEIGHTED': suffix = "_ponderada"
        
        full_name = f"{fname}{suffix}"
        
        df.to_csv(CSV_OUT_DIR / f"{full_name}.csv", index=False)
        df.to_excel(XLSX_OUT_DIR / f"{full_name}.xlsx", index=False)
        print(f"   [OK] Gerado: {full_name}.xlsx | N: {int(df['N_Alunos'].sum())}")

    def run_2015(self):
        print(f"\n[INÍCIO] Processando PISA 2015 (Estados)...")
        base_path = DATA_RAW_ROOT / 'pisa_2015'
        if not base_path.exists(): print(f"[PULAR] Pasta não encontrada: {base_path}"); return

        sav_files = [f for f in os.listdir(base_path) if 'STU' in f and f.endswith('.sav')]
        if not sav_files: print("[ERRO] Arquivo .sav ausente."); return
        target_file = base_path / sav_files[0]

        # Static Mapping (2015 specific)
        NAME_TO_IBGE = {'RONDONIA': 11, 'RONDÔNIA': 11, 'ACRE': 12, 'AMAZONAS': 13, 'RORAIMA': 14, 'PARA': 15, 'PARÁ': 15, 'AMAPA': 16, 'AMAPÁ': 16, 'TOCANTINS': 17, 'MARANHAO': 21, 'MARANHÃO': 21, 'PIAUI': 22, 'PIAUÍ': 22, 'CEARA': 23, 'CEARÁ': 23, 'RIO GRANDE DO NORTE': 24, 'PARAIBA': 25, 'PARAÍBA': 25, 'PERNAMBUCO': 26, 'ALAGOAS': 27, 'SERGIPE': 28, 'BAHIA': 29, 'MINAS GERAIS': 31, 'ESPIRITO SANTO': 32, 'ESPÍRITO SANTO': 32, 'RIO DE JANEIRO': 33, 'SAO PAULO': 35, 'SÃO PAULO': 35, 'PARANA': 41, 'PARANÁ': 41, 'SANTA CATARINA': 42, 'RIO GRANDE DO SUL': 43, 'MATO GROSSO DO SUL': 50, 'MATO GROSSO': 51, 'GOIAS': 52, 'GOIÁS': 52, 'DISTRITO FEDERAL': 53}
        REGION_CODE_TO_NAME = {11:'North', 12:'North', 13:'North', 14:'North', 15:'North', 16:'North', 17:'North', 21:'Northeast', 22:'Northeast', 23:'Northeast', 24:'Northeast', 25:'Northeast', 26:'Northeast', 27:'Northeast', 28:'Northeast', 29:'Northeast', 31:'Southeast', 32:'Southeast', 33:'Southeast', 35:'Southeast', 41:'South', 42:'South', 43:'South', 50:'Center-West', 51:'Center-West', 52:'Center-West', 53:'Center-West'}
        IBGE_TO_SIGLA = {11:'RO', 12:'AC', 13:'AM', 14:'RR', 15:'PA', 16:'AP', 17:'TO', 21:'MA', 22:'PI', 23:'CE', 24:'RN', 25:'PB', 26:'PE', 27:'AL', 28:'SE', 29:'BA', 31:'MG', 32:'ES', 33:'RJ', 35:'SP', 41:'PR', 42:'SC', 43:'RS', 50:'MS', 51:'MT', 52:'GO', 53:'DF'}

        try:
            _, meta = pyreadstat.read_sav(str(target_file), metadataonly=True)
            region_col = next((c for c in ['STRATUM', 'REGION', 'CNT', 'ST004D01T'] if c in meta.column_names), None)
            scores = [c for c in meta.column_names if c.startswith('PV1') and any(x in c for x in ['MATH', 'READ', 'SCIE'])]
            use_cols = list(set([region_col] + scores + ['W_FSTUWT']))
            if 'CNT' in meta.column_names: use_cols.append('CNT')

            df, meta = pyreadstat.read_sav(str(target_file), usecols=use_cols)
            if 'CNT' in df.columns: df = df[df['CNT'] == 'BRA'].copy()

            if region_col in meta.variable_value_labels:
                labels = meta.variable_value_labels[region_col]
                df['STRATUM_TEXT'] = df[region_col].map(labels).fillna(df[region_col].astype(str))
            else: df['STRATUM_TEXT'] = df[region_col].astype(str)

            def resolve_ibge(txt):
                if not isinstance(txt, str): return None
                txt = txt.upper()
                for k in sorted(NAME_TO_IBGE.keys(), key=len, reverse=True):
                    if k in txt: return NAME_TO_IBGE[k]
                return None
            
            df['IBGE_CODE'] = df['STRATUM_TEXT'].apply(resolve_ibge)
            df = df.dropna(subset=['IBGE_CODE'])
            
            rename_pv = {'PV1MATH': 'Math', 'PV1READ': 'Read', 'PV1SCIE': 'Science'}
            df = df.rename(columns={k:v for k,v in rename_pv.items() if k in df.columns})

            summary = df['IBGE_CODE'].value_counts().reset_index()
            summary.columns = ['IBGE_CODE', 'Student_Count']
            
            if self.mode in ['SIMPLE', 'BOTH']:
                means = df.groupby('IBGE_CODE')[['Math', 'Read', 'Science']].mean().reset_index()
                summary = pd.merge(summary, means, on='IBGE_CODE')
                summary['Cognitive_Global_Mean'] = summary[['Math', 'Read', 'Science']].mean(axis=1)

            if self.mode in ['WEIGHTED', 'BOTH']:
                means_w = self._calc_weighted(df, 'IBGE_CODE', ['Math', 'Read', 'Science'])
                if means_w is not None:
                    summary = pd.merge(summary, means_w, on='IBGE_CODE')
                    summary['Cognitive_Global_Mean_Ponderada'] = summary[['Math_Ponderada', 'Read_Ponderada', 'Science_Ponderada']].mean(axis=1)

            summary['UF'] = summary['IBGE_CODE'].map(IBGE_TO_SIGLA)
            summary['Region'] = summary['IBGE_CODE'].map(REGION_CODE_TO_NAME)
            
            final_df = self._apply_standardization(summary, 2015)
            self._save(final_df, 'pisa_table_2015_states')

        except Exception as e: print(f"   [ERRO] {e}")

    def run_2018(self):
        print(f"\n[INÍCIO] Processando PISA 2018 (Regiões)...")
        RAW_FILE = DATA_RAW_ROOT / 'pisa_2018' / 'CY07_MSU_STU_QQQ.sav'
        if not RAW_FILE.exists(): print(f"[PULAR] Arquivo faltante."); return
        self._generic_regional_run(RAW_FILE, 2018)

    def run_2022(self):
        print(f"\n[INÍCIO] Processando PISA 2022 (Regiões)...")
        RAW_FILE = DATA_RAW_ROOT / 'pisa_2022' / 'CY08MSP_STU_QQQ.sav'
        if not RAW_FILE.exists(): print(f"[PULAR] Arquivo faltante."); return
        self._generic_regional_run(RAW_FILE, 2022)

    def _generic_regional_run(self, file_path, year):
        cols = ['CNT', 'STRATUM', 'PV1MATH', 'PV1READ', 'PV1SCIE', 'W_FSTUWT']
        try:
            df = pd.read_spss(str(file_path), usecols=cols)
            df = df[df['CNT'].astype(str).str.contains('BRA|Brazil|76', case=False, na=False)].copy()
            if df.empty: print("[ERRO] Dados Brasil vazios."); return

            def get_region(s, y):
                s = str(s).upper().strip()
                if y == 2018:
                    if s.startswith('BRA'):
                        code = s[3:5]
                        if code == '01': return 'North'
                        if code == '02': return 'Northeast'
                        if code == '03': return 'Southeast'
                        if code == '04': return 'South'
                        if code == '05': return 'Center-West'
                else: # 2022
                    if 'CENTRO' in s: return 'Center-West'
                    if 'NORDESTE' in s: return 'Northeast'
                    if 'SUDESTE' in s: return 'Southeast'
                    if 'NORTE' in s: return 'North'
                    if 'SUL' in s: return 'South'
                return 'UNKNOWN'

            df['Region'] = df['STRATUM'].apply(lambda x: get_region(x, year))
            df = df[df['Region'] != 'UNKNOWN']
            
            summary = df['Region'].value_counts().reset_index()
            summary.columns = ['Region', 'Student_Count']
            
            if self.mode in ['SIMPLE', 'BOTH']:
                means = df.groupby('Region')[['PV1MATH', 'PV1READ', 'PV1SCIE']].mean().reset_index()
                summary = pd.merge(summary, means, on='Region')
                summary['Cognitive_Global_Mean'] = (summary['PV1MATH'] + summary['PV1READ'] + summary['PV1SCIE']) / 3
            
            if self.mode in ['WEIGHTED', 'BOTH']:
                means_w = self._calc_weighted(df, 'Region', ['PV1MATH', 'PV1READ', 'PV1SCIE'])
                if means_w is not None:
                    summary = pd.merge(summary, means_w, on='Region')
                    summary['Cognitive_Global_Mean_Ponderada'] = (summary['PV1MATH_Ponderada'] + summary['PV1READ_Ponderada'] + summary['PV1SCIE_Ponderada']) / 3

            ren = {'PV1MATH': 'Math_Mean', 'PV1READ': 'Read_Mean', 'PV1SCIE': 'Science_Mean',
                   'PV1MATH_Ponderada': 'Matemática_Ponderada', 'PV1READ_Ponderada': 'Leitura_Ponderada', 'PV1SCIE_Ponderada': 'Ciências_Ponderada'}
            summary = summary.rename(columns=ren)
            
            final_df = self._apply_standardization(summary, year)
            self._save(final_df, f'pisa_table_{year}_regional')

        except Exception as e: print(f"   [ERRO] {e}")

def main():
    os.system('cls' if os.name == 'nt' else 'clear')
    print("=== PISA UNIFIED PIPELINE v8.1 ===")
    
    # 1. YEARS
    print("\n[1/3] SELEÇÃO DE ANOS")
    print("Disponíveis: 2015, 2018, 2022")
    raw_years = input_timeout(">> Digite anos (ex: 2015) ou ENTER para Todos", default="2015, 2018, 2022")
    try:
        years = [int(y.strip()) for y in raw_years.split(',') if y.strip()]
    except:
        years = [2015, 2018, 2022]

    # 2. CONCEPTS (Clean Menu)
    print("\n[2/3] SELEÇÃO DE INDICADORES (CONCEITOS)")
    print("O que você deseja analisar?")
    concept_map = {
        1: 'Math', 2: 'Read', 3: 'Science', 4: 'Global', 5: 'Count'
    }
    
    print("  [1] Matemática")
    print("  [2] Leitura")
    print("  [3] Ciências")
    print("  [4] Média Geral (Global)")
    print("  [5] N_Alunos (Contagem)")
        
    raw_cols = input_timeout(">> Digite os NÚMEROS (ex: 1, 4) ou ENTER para Todas", default="TODAS")
    
    user_concepts = None
    if raw_cols != "TODAS":
        try:
            ids = [int(x.strip()) for x in raw_cols.split(',') if x.strip().isdigit()]
            user_concepts = [concept_map[i] for i in ids if i in concept_map]
            print(f"   -> Indicadores Selecionados: {user_concepts}")
        except:
            print("   -> Entrada inválida. Usando TODAS.")

    # 3. METHOD
    print("\n[3/3] MÉTODO DE CÁLCULO")
    print("Como os indicadores devem ser calculados?")
    print("  [1] Apenas Média Simples (Legacy)")
    print("  [2] Apenas Média Ponderada (Recomendado)")
    print("  [3] Ambas (Comparativo)")
    op = input_timeout(">> Opção", default="3")
    
    mode_map = {'1': 'SIMPLE', '2': 'WEIGHTED', '3': 'BOTH'}
    selected_mode = mode_map.get(op, 'BOTH')

    print("-" * 60)
    print(f"[CONFIG] Anos: {years} | Modo: {selected_mode} | Indicadores: {'TODOS' if not user_concepts else len(user_concepts)}")
    print("-" * 60)

    etl = PisaUnifiedETL(mode=selected_mode, user_concepts=user_concepts)

    for year in years:
        if year == 2015: etl.run_2015()
        elif year == 2018: etl.run_2018()
        elif year == 2022: etl.run_2022()
        else: print(f"[AVISO] Ano {year} não suportado.")

    print("\n[CONCLUÍDO] PISA Finalizado.")

if __name__ == "__main__":
    main()