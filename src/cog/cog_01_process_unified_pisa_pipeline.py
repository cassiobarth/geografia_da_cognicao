"""
================================================================================
PROJECT:         Geography of Cognition: Poverty, Wealth, and Inequalities in Brazil
SCRIPT:          src/cog/cog_04_process_pisa_unified.py
VERSION:         7.5 (Production - Weighted Means / PT-BR / Standard Header)
DATE:            2026-01-26
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
from datetime import timedelta
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

class PisaUnifiedETL:
    def __init__(self, user_cols=None):
        self.user_cols = user_cols
        
        # Mapeamento Base PT-BR
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

    def _calc_weighted(self, df, group_col, val_cols):
        """Calcula média ponderada usando W_FSTUWT."""
        try:
            results = []
            for col in val_cols:
                wm = df.groupby(group_col).apply(
                    lambda x: np.average(x[col], weights=x['W_FSTUWT']) if x['W_FSTUWT'].sum() > 0 else np.nan
                ).reset_index(name=f'{col}_Ponderada')
                results.append(wm)
            
            final_w = results[0]
            for r in results[1:]:
                final_w = pd.merge(final_w, r, on=group_col)
            return final_w
        except Exception as e:
            print(f"   [AVISO] Falha no cálculo ponderado: {e}")
            return None

    def _apply_standardization(self, df, year):
        """Padronização final de colunas e tradução."""
        # 1. Tradução de Regiões
        if 'Region' in df.columns:
            df['Region'] = df['Region'].replace(self.region_trans)
        
        # 2. Renomeia Colunas Simples
        df = df.rename(columns=self.translate_map)
        
        # 3. Renomeia Colunas Ponderadas (Dinâmico)
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
        
        # 4. Filtro de Colunas do Usuário
        cols_available = [c for c in df.columns]
        if self.user_cols:
            final_cols = [c for c in cols_available if c in self.user_cols or c == 'Ano']
            if not final_cols: final_cols = cols_available
        else:
            final_cols = cols_available

        # 5. Ordenação Lógica
        priority = ['Ano', 'Região', 'UF', 'Média_Geral', 'Média_Geral_Ponderada', 'N_Alunos']
        ordered = [c for c in priority if c in final_cols] + [c for c in final_cols if c not in priority]
        
        final_df = df[ordered]
        if 'Média_Geral' in final_df.columns:
            final_df = final_df.sort_values('Média_Geral', ascending=False)
            
        return final_df

    def run_2015(self):
        print(f"\n[INÍCIO] Processando PISA 2015 (Estados)...")
        base_path = DATA_RAW_ROOT / 'pisa_2015'
        if not base_path.exists(): print(f"[PULAR] Pasta não encontrada: {base_path}"); return

        sav_files = [f for f in os.listdir(base_path) if 'STU' in f and f.endswith('.sav')]
        if not sav_files: print("[ERRO] Arquivo .sav ausente."); return
        target_file = base_path / sav_files[0]

        # Mapeamentos Estáticos (Lógica Original Preservada)
        NAME_TO_IBGE = {
            'RONDONIA': 11, 'RONDÔNIA': 11, 'ACRE': 12, 'AMAZONAS': 13, 'RORAIMA': 14,
            'PARA': 15, 'PARÁ': 15, 'AMAPA': 16, 'AMAPÁ': 16, 'TOCANTINS': 17,
            'MARANHAO': 21, 'MARANHÃO': 21, 'PIAUI': 22, 'PIAUÍ': 22, 'CEARA': 23, 'CEARÁ': 23,
            'RIO GRANDE DO NORTE': 24, 'PARAIBA': 25, 'PARAÍBA': 25, 'PERNAMBUCO': 26,
            'ALAGOAS': 27, 'SERGIPE': 28, 'BAHIA': 29,
            'MINAS GERAIS': 31, 'ESPIRITO SANTO': 32, 'ESPÍRITO SANTO': 32,
            'RIO DE JANEIRO': 33, 'SAO PAULO': 35, 'SÃO PAULO': 35,
            'PARANA': 41, 'PARANÁ': 41, 'SANTA CATARINA': 42, 'RIO GRANDE DO SUL': 43,
            'MATO GROSSO DO SUL': 50, 'MATO GROSSO': 51, 'GOIAS': 52, 'GOIÁS': 52, 'DISTRITO FEDERAL': 53
        }
        REGION_CODE_TO_NAME = {
            11:'North', 12:'North', 13:'North', 14:'North', 15:'North', 16:'North', 17:'North',
            21:'Northeast', 22:'Northeast', 23:'Northeast', 24:'Northeast', 25:'Northeast', 26:'Northeast', 27:'Northeast', 28:'Northeast', 29:'Northeast',
            31:'Southeast', 32:'Southeast', 33:'Southeast', 35:'Southeast',
            41:'South', 42:'South', 43:'South',
            50:'Center-West', 51:'Center-West', 52:'Center-West', 53:'Center-West'
        }
        IBGE_TO_SIGLA = {11:'RO', 12:'AC', 13:'AM', 14:'RR', 15:'PA', 16:'AP', 17:'TO', 21:'MA', 22:'PI', 23:'CE', 24:'RN', 25:'PB', 26:'PE', 27:'AL', 28:'SE', 29:'BA', 31:'MG', 32:'ES', 33:'RJ', 35:'SP', 41:'PR', 42:'SC', 43:'RS', 50:'MS', 51:'MT', 52:'GO', 53:'DF'}

        def resolve_ibge_from_text(text_label):
            if not isinstance(text_label, str): return None
            text_upper = text_label.upper()
            sorted_names = sorted(NAME_TO_IBGE.keys(), key=len, reverse=True)
            for name in sorted_names:
                if name in text_upper: return NAME_TO_IBGE[name]
            return None

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
            else:
                df['STRATUM_TEXT'] = df[region_col].astype(str)

            df['IBGE_CODE'] = df['STRATUM_TEXT'].apply(resolve_ibge_from_text)
            df = df.dropna(subset=['IBGE_CODE'])
            
            rename_pv = {'PV1MATH': 'Math', 'PV1READ': 'Read', 'PV1SCIE': 'Science'}
            df = df.rename(columns={k:v for k,v in rename_pv.items() if k in df.columns})

            # Cálculos
            means_simple = df.groupby('IBGE_CODE')[['Math', 'Read', 'Science']].mean().reset_index()
            means_weighted = self._calc_weighted(df, 'IBGE_CODE', ['Math', 'Read', 'Science'])
            counts = df['IBGE_CODE'].value_counts().reset_index()
            counts.columns = ['IBGE_CODE', 'Student_Count']
            
            summary = pd.merge(counts, means_simple, on='IBGE_CODE')
            if means_weighted is not None:
                summary = pd.merge(summary, means_weighted, on='IBGE_CODE')

            summary['UF'] = summary['IBGE_CODE'].map(IBGE_TO_SIGLA)
            summary['Region'] = summary['IBGE_CODE'].map(REGION_CODE_TO_NAME)
            
            summary['Cognitive_Global_Mean'] = summary[['Math', 'Read', 'Science']].mean(axis=1)
            if 'Math_Ponderada' in summary.columns:
                summary['Cognitive_Global_Mean_Ponderada'] = summary[['Math_Ponderada', 'Read_Ponderada', 'Science_Ponderada']].mean(axis=1)

            final_df = self._apply_standardization(summary, 2015)
            
            fname = "pisa_table_2015_states"
            final_df.to_csv(CSV_OUT_DIR / f"{fname}.csv", index=False)
            final_df.to_excel(XLSX_OUT_DIR / f"{fname}.xlsx", index=False)
            
            print(f"   [OK] Gerado: {fname} | N: {int(final_df['N_Alunos'].sum())}")

        except Exception as e:
            print(f"   [ERRO] {e}")

    def run_2018(self):
        print(f"\n[INÍCIO] Processando PISA 2018 (Regiões)...")
        RAW_FILE = DATA_RAW_ROOT / 'pisa_2018' / 'CY07_MSU_STU_QQQ.sav'
        if not RAW_FILE.exists(): print(f"[PULAR] Arquivo faltante."); return

        cols = ['CNT', 'STRATUM', 'PV1MATH', 'PV1READ', 'PV1SCIE', 'W_FSTUWT']
        try:
            df = pd.read_spss(str(RAW_FILE), usecols=cols, convert_categoricals=False)
            df = df[df['CNT'].astype(str).str.contains('BRA|Brazil|76', case=False, na=False)].copy()
            if df.empty: print("[ERRO] Dados Brasil vazios."); return

            def get_region_code_2018(stratum):
                s = str(stratum).upper().strip()
                if s.startswith('BRA'):
                    code = s[3:5] 
                    if code == '01': return 'North'
                    if code == '02': return 'Northeast'
                    if code == '03': return 'Southeast'
                    if code == '04': return 'South'
                    if code == '05': return 'Center-West'
                return 'UNKNOWN'

            df['Region'] = df['STRATUM'].apply(get_region_code_2018)
            df = df[df['Region'] != 'UNKNOWN']
            
            means_simple = df.groupby('Region')[['PV1MATH', 'PV1READ', 'PV1SCIE']].mean().reset_index()
            means_weighted = self._calc_weighted(df, 'Region', ['PV1MATH', 'PV1READ', 'PV1SCIE'])
            counts = df['Region'].value_counts().reset_index()
            counts.columns = ['Region', 'Student_Count']
            
            res = pd.merge(counts, means_simple, on='Region')
            if means_weighted is not None:
                res = pd.merge(res, means_weighted, on='Region')

            res['Cognitive_Global_Mean'] = (res['PV1MATH'] + res['PV1READ'] + res['PV1SCIE']) / 3
            if 'PV1MATH_Ponderada' in res.columns:
                 res['Cognitive_Global_Mean_Ponderada'] = (res['PV1MATH_Ponderada'] + res['PV1READ_Ponderada'] + res['PV1SCIE_Ponderada']) / 3

            res = res.rename(columns={'PV1MATH': 'Math_Mean', 'PV1READ': 'Read_Mean', 'PV1SCIE': 'Science_Mean'})
            final_df = self._apply_standardization(res, 2018)
            
            # Ajuste fino de nomes
            map_extra = {'PV1MATH_Ponderada': 'Matemática_Ponderada', 'PV1READ_Ponderada': 'Leitura_Ponderada', 'PV1SCIE_Ponderada': 'Ciências_Ponderada'}
            final_df = final_df.rename(columns=map_extra)

            fname = "pisa_table_2018_regional"
            final_df.to_csv(CSV_OUT_DIR / f"{fname}.csv", index=False)
            final_df.to_excel(XLSX_OUT_DIR / f"{fname}.xlsx", index=False)
            print(f"   [OK] Gerado: {fname} | N: {int(final_df['N_Alunos'].sum())}")

        except Exception as e:
            print(f"   [ERRO] {e}")

    def run_2022(self):
        print(f"\n[INÍCIO] Processando PISA 2022 (Regiões)...")
        RAW_FILE = DATA_RAW_ROOT / 'pisa_2022' / 'CY08MSP_STU_QQQ.sav'
        if not RAW_FILE.exists(): print(f"[PULAR] Arquivo faltante."); return

        cols = ['CNT', 'STRATUM', 'PV1MATH', 'PV1READ', 'PV1SCIE', 'W_FSTUWT']
        try:
            df = pd.read_spss(str(RAW_FILE), usecols=cols)
            df = df[df['CNT'].astype(str).str.contains('Brazil|BRA', case=False, na=False)].copy()
            if df.empty: print("[ERRO] Dados Brasil vazios."); return

            def get_region_2022(stratum):
                s = str(stratum).upper()
                if 'CENTRO-OESTE' in s or 'CENTRO OESTE' in s: return 'Center-West'
                if 'NORDESTE' in s: return 'Northeast'
                if 'SUDESTE' in s: return 'Southeast'
                if 'NORTE' in s: return 'North'
                if 'SUL' in s: return 'South'
                return 'UNKNOWN'

            df['Region'] = df['STRATUM'].apply(get_region_2022)
            df = df[df['Region'] != 'UNKNOWN']
            
            means_simple = df.groupby('Region')[['PV1MATH', 'PV1READ', 'PV1SCIE']].mean().reset_index()
            means_weighted = self._calc_weighted(df, 'Region', ['PV1MATH', 'PV1READ', 'PV1SCIE'])
            counts = df['Region'].value_counts().reset_index()
            counts.columns = ['Region', 'Student_Count']
            
            res = pd.merge(counts, means_simple, on='Region')
            if means_weighted is not None:
                res = pd.merge(res, means_weighted, on='Region')

            res['Cognitive_Global_Mean'] = (res['PV1MATH'] + res['PV1READ'] + res['PV1SCIE']) / 3
            if 'PV1MATH_Ponderada' in res.columns:
                 res['Cognitive_Global_Mean_Ponderada'] = (res['PV1MATH_Ponderada'] + res['PV1READ_Ponderada'] + res['PV1SCIE_Ponderada']) / 3

            res = res.rename(columns={'PV1MATH': 'Math_Mean', 'PV1READ': 'Read_Mean', 'PV1SCIE': 'Science_Mean'})
            final_df = self._apply_standardization(res, 2022)

            map_extra = {'PV1MATH_Ponderada': 'Matemática_Ponderada', 'PV1READ_Ponderada': 'Leitura_Ponderada', 'PV1SCIE_Ponderada': 'Ciências_Ponderada'}
            final_df = final_df.rename(columns=map_extra)
            
            fname = "pisa_table_2022_regional"
            final_df.to_csv(CSV_OUT_DIR / f"{fname}.csv", index=False)
            final_df.to_excel(XLSX_OUT_DIR / f"{fname}.xlsx", index=False)
            print(f"   [OK] Gerado: {fname} | N: {int(final_df['N_Alunos'].sum())}")

        except Exception as e:
            print(f"   [ERRO] {e}")

def main():
    os.system('cls' if os.name == 'nt' else 'clear')
    print("=== PISA UNIFIED PIPELINE v7.5 ===")
    
    raw_years = input_timeout(">> Anos (ex: 2015, 2022)", timeout=10, default="2015, 2018, 2022")
    try:
        years = [int(y.strip()) for y in raw_years.split(',') if y.strip()]
    except:
        years = [2015, 2018, 2022]

    print("\nCOLUNAS (PT-BR):")
    print("Padrão: [Ano, Região, UF, N_Alunos, Média_Geral, Média_Geral_Ponderada...]")
    raw_cols = input_timeout(">> Selecione colunas ou ENTER para TODAS", timeout=10, default="TODAS")
    
    if raw_cols == "TODAS":
        user_cols_list = None
        print(f"\n[CONFIG] Anos: {years} | Colunas: TODAS (Com Ponderação)")
    else:
        user_cols_list = [c.strip() for c in raw_cols.split(',')]
        print(f"\n[CONFIG] Anos: {years} | Colunas: {len(user_cols_list)} selecionadas")

    print("-" * 60)

    etl = PisaUnifiedETL(user_cols=user_cols_list)

    for year in years:
        if year == 2015: etl.run_2015()
        elif year == 2018: etl.run_2018()
        elif year == 2022: etl.run_2022()
        else: print(f"[AVISO] Ano {year} não suportado.")

    print("\n[CONCLUÍDO] PISA Finalizado.")

if __name__ == "__main__":
    main()