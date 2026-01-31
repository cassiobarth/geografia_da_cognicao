"""
================================================================================
PROJECT:    Geography of Cognition: Poverty, Wealth, and Inequalities in Brazil
SCRIPT:     src/ind/ind_01_sisvan_nutricao.py
VERSION:    1.2 (Added Timeout Selection: 2024 Default vs 2022-2024 Mean)
DATE:       2026-01-30
--------------------------------------------------------------------------------
PRINCIPAL INVESTIGATOR:  Dr. José Aparecido da Silva
LEAD DEVELOPER:          Specialist in Applied Statistics
SOURCE:                  SISVAN (Sistema de Vigilância Alimentar e Nutricional)
================================================================================

ABSTRACT:
    Automated ETL pipeline for Nutritional Status indicators (SISVAN).
    Processes "Stunting" (Height-for-Age) and "Wasting" (Weight-for-Height).
    Handles raw .xls files (often HTML tables) exported from SISVAN Web.
    Computes prevalence of nutritional deficits for children 0-5 years.

DATA SOURCE:
    - SISVAN Web Reports (Relatórios Públicos).
    - Period: 2022, 2023, 2024.
    - Files: sisvan_estatura_{year}.xls / sisvan_peso_{year}.xls

OUTPUTS:
    1. CSV: data/processed/indicadores/ind01_nutricao.csv
    2. XLSX: reports/indicadores/xlsx/ind01_nutricao.xlsx
    3. PLOTS: Bar charts for Stunting and Wasting prevalence.

DEPENDENCIES:
    pandas, matplotlib, seaborn, xlrd (optional), lxml (for html), threading, time
================================================================================
"""
import pandas as pd
import numpy as np
import sys
import os
import glob
import time
import threading
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path

# --- Path Configuration ---
BASE_DIR = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(BASE_DIR))

# Directories
RAW_DIR = BASE_DIR / "data" / "raw" / "indicadores" / "ind01_bio"
OUT_CSV_DIR = BASE_DIR / "data" / "processed" / "indicadores"
OUT_XLSX_DIR = BASE_DIR / "reports" / "indicadores" / "xlsx"
OUT_PLOT_DIR = BASE_DIR / "reports" / "indicadores" / "graficos"

for d in [OUT_CSV_DIR, OUT_XLSX_DIR, OUT_PLOT_DIR]:
    d.mkdir(parents=True, exist_ok=True)

# State Mapping
UF_TO_SIGLA = {
    'Rondônia': 'RO', 'Acre': 'AC', 'Amazonas': 'AM', 'Roraima': 'RR', 'Pará': 'PA', 'Amapá': 'AP', 'Tocantins': 'TO',
    'Maranhão': 'MA', 'Piauí': 'PI', 'Ceará': 'CE', 'Rio Grande do Norte': 'RN', 'Paraíba': 'PB', 'Pernambuco': 'PE', 
    'Alagoas': 'AL', 'Sergipe': 'SE', 'Bahia': 'BA',
    'Minas Gerais': 'MG', 'Espírito Santo': 'ES', 'Rio de Janeiro': 'RJ', 'São Paulo': 'SP',
    'Paraná': 'PR', 'Santa Catarina': 'SC', 'Rio Grande do Sul': 'RS',
    'Mato Grosso do Sul': 'MS', 'Mato Grosso': 'MT', 'Goiás': 'GO', 'Distrito Federal': 'DF'
}

UF_TO_REGIAO = {
    'AC': 'Norte', 'AP': 'Norte', 'AM': 'Norte', 'PA': 'Norte', 'RO': 'Norte', 'RR': 'Norte', 'TO': 'Norte',
    'AL': 'Nordeste', 'BA': 'Nordeste', 'CE': 'Nordeste', 'MA': 'Nordeste', 'PB': 'Nordeste', 'PE': 'Nordeste', 'PI': 'Nordeste', 'RN': 'Nordeste', 'SE': 'Nordeste',
    'DF': 'Centro-Oeste', 'GO': 'Centro-Oeste', 'MT': 'Centro-Oeste', 'MS': 'Centro-Oeste',
    'ES': 'Sudeste', 'MG': 'Sudeste', 'RJ': 'Sudeste', 'SP': 'Sudeste',
    'PR': 'Sul', 'RS': 'Sul', 'SC': 'Sul'
}

def robust_read_sisvan(filepath):
    """
    Attempts to read SISVAN .xls files.
    Strategy 1: Read as HTML (common for SISVAN exports).
    Strategy 2: Read as Standard Excel.
    """
    print(f"[LOADING] {filepath.name}...")
    df = None
    
    # Strategy 1: HTML
    try:
        tables = pd.read_html(str(filepath), flavor='bs4', decimal=',', thousands='.')
        if tables:
            for t in tables:
                if t.astype(str).apply(lambda x: x.str.contains('UF|Região|Total', case=False)).any().any():
                    df = t
                    break
            if df is None: df = tables[0]
    except Exception:
        pass 

    # Strategy 2: Excel
    if df is None:
        try:
            df = pd.read_excel(filepath)
        except Exception:
            pass

    if df is None:
        print(f"[ERROR] Could not read {filepath.name}. Format unrecognized.")
        return None

    return clean_sisvan_dataframe(df)

def clean_sisvan_dataframe(df):
    """Cleaning logic specific to SISVAN layout."""
    df = df.astype(str)
    
    # Find header
    header_idx = -1
    for i, row in df.iterrows():
        row_str = " ".join(row.values).lower()
        if 'uf' in row_str and ('total' in row_str or 'quantidade' in row_str):
            header_idx = i
            break
    
    if header_idx != -1:
        df.columns = df.iloc[header_idx]
        df = df.iloc[header_idx+1:]
    
    # Rename columns to avoid duplicates
    cols = df.columns.tolist()
    new_cols = [f"COL_{i}" for i in range(len(cols))]
    df.columns = new_cols
    
    # Identify UF Column
    uf_col_idx = -1
    for i, col in enumerate(df.columns):
        sample = df[col].iloc[0:10].tolist()
        matches = sum(1 for x in sample if str(x).strip() in UF_TO_SIGLA.keys())
        if matches > 0:
            uf_col_idx = i
            break
            
    if uf_col_idx == -1: return None

    # Filter rows
    df['STATE_NAME'] = df.iloc[:, uf_col_idx].str.strip()
    df = df[df['STATE_NAME'].isin(UF_TO_SIGLA.keys())].copy()
    df['UF'] = df['STATE_NAME'].map(UF_TO_SIGLA)
    
    # Extract Percentages
    # Assuming SISVAN Standard: Col X (UF), X+1 (?), X+2 (% Muito Baixo), X+4 (% Baixo)
    def clean_pct(val):
        try:
            val = str(val).replace('%', '').replace(',', '.')
            return float(val)
        except:
            return 0.0

    try:
        idx_start = uf_col_idx + 1
        col_mb_perc = df.columns[idx_start + 1] # Usually Col 4 relative to start
        col_b_perc = df.columns[idx_start + 3]  # Usually Col 6 relative to start
        
        # Check if these columns actually contain numbers roughly
        df['DEFICIT_PERC'] = df[col_mb_perc].apply(clean_pct) + df[col_b_perc].apply(clean_pct)
        return df[['UF', 'DEFICIT_PERC']]
    except:
        return None

def process_years(metric_type, target_years):
    """
    metric_type: 'estatura' or 'peso'
    target_years: list of integers [2024] or [2022, 2023, 2024]
    """
    print(f"\n--- PROCESSING {metric_type.upper()} ---")
    dfs = []
    
    for year in target_years:
        # Search for file: sisvan_estatura_2024.xls (or xlsx)
        pattern = str(RAW_DIR / f"sisvan_{metric_type}_{year}*.xls*")
        files = glob.glob(pattern)
        
        if not files:
            print(f"  [WARN] No file found for {year}")
            continue
        
        fpath = Path(files[0])
        df_year = robust_read_sisvan(fpath)
        
        if df_year is not None:
            col_name = f"{metric_type.upper()}_{year}"
            df_year = df_year.rename(columns={'DEFICIT_PERC': col_name})
            dfs.append(df_year)
            print(f"  [OK] Loaded {year}")

    if not dfs:
        print("[ERROR] No data loaded.")
        return None

    # Merge
    df_final = dfs[0]
    for df_next in dfs[1:]:
        df_final = pd.merge(df_final, df_next, on='UF', how='outer')
        
    # Calculate Mean (or just keep the single year value)
    num_cols = [c for c in df_final.columns if metric_type.upper() in c]
    df_final[f'MEAN_{metric_type.upper()}'] = df_final[num_cols].mean(axis=1).round(2)
    
    return df_final

def generate_plots(df, suffix_title):
    sns.set_theme(style="whitegrid")
    
    # 1. Stunting Plot
    if 'MEAN_ESTATURA' in df.columns:
        plt.figure(figsize=(12, 6))
        df_sorted = df.sort_values('MEAN_ESTATURA', ascending=False)
        colors = df_sorted['REGIAO'].map({
            'Norte': 'green', 'Nordeste': 'red', 'Sudeste': 'blue', 
            'Sul': 'cyan', 'Centro-Oeste': 'orange'
        }).fillna('gray')
        sns.barplot(x='UF', y='MEAN_ESTATURA', data=df_sorted, palette=colors.values)
        plt.title(f'IND-01: Deficit de Altura (Stunting) 0-5 anos - {suffix_title}')
        plt.ylabel('Prevalencia (%)')
        plt.xlabel('UF')
        plt.savefig(OUT_PLOT_DIR / "ind01_stunting_bar.png", dpi=300, bbox_inches='tight')
        plt.close()

    # 2. Wasting Plot
    if 'MEAN_PESO' in df.columns:
        plt.figure(figsize=(12, 6))
        df_sorted = df.sort_values('MEAN_PESO', ascending=False)
        colors = df_sorted['REGIAO'].map({'Norte': 'green', 'Nordeste': 'red', 'Sudeste': 'blue', 'Sul': 'cyan', 'Centro-Oeste': 'orange'}).fillna('gray')
        sns.barplot(x='UF', y='MEAN_PESO', data=df_sorted, palette=colors.values)
        plt.title(f'IND-01: Deficit de Peso (Wasting) 0-5 anos - {suffix_title}')
        plt.ylabel('Prevalencia (%)')
        plt.xlabel('UF')
        plt.savefig(OUT_PLOT_DIR / "ind01_wasting_bar.png", dpi=300, bbox_inches='tight')
        plt.close()

def run(years_list, title_suffix):
    print(f"--- SISVAN ETL STARTED (Target: {title_suffix}) ---")
    
    df_stunting = process_years('estatura', years_list)
    df_wasting = process_years('peso', years_list)
    
    if df_stunting is None and df_wasting is None:
        print("[FATAL] Could not process any files.")
        return

    if df_stunting is not None and df_wasting is not None:
        df_final = pd.merge(df_stunting, df_wasting, on='UF', how='outer')
    elif df_stunting is not None:
        df_final = df_stunting
    else:
        df_final = df_wasting

    df_final['REGIAO'] = df_final['UF'].map(UF_TO_REGIAO)
    cols = ['UF', 'REGIAO'] + [c for c in df_final.columns if c not in ['UF', 'REGIAO']]
    df_final = df_final[cols]

    csv_path = OUT_CSV_DIR / "ind01_nutricao.csv"
    xlsx_path = OUT_XLSX_DIR / "ind01_nutricao.xlsx"
    
    df_final.to_csv(csv_path, index=False, sep=';', encoding='utf-8')
    df_final.to_excel(xlsx_path, index=False)
    print(f"[SUCCESS] Saved to {csv_path}")
    
    generate_plots(df_final, title_suffix)

if __name__ == "__main__":
    print("--- SELECAO DE PERIODO (SISVAN) ---")
    print("1 - Apenas 2024 (Padrao)")
    print("2 - Media 2022-2024 (Historico)")
    print("0 - Cancelar")
    print("\nVoce tem 10 segundos para escolher... (Padrao: 1 - 2024)")
    
    choice = '1' # Padrao
    
    def get_input():
        global choice
        try:
            user_input = input("Opcao (0/1/2): ").strip()
            if user_input:
                choice = user_input
        except EOFError:
            pass

    input_thread = threading.Thread(target=get_input)
    input_thread.daemon = True
    input_thread.start()

    timeout = 10
    start_time = time.time()
    
    while input_thread.is_alive():
        elapsed = time.time() - start_time
        remaining = int(timeout - elapsed)
        if remaining <= 0:
            print("\nTempo esgotado! Assumindo opcao padrao (1 - 2024).")
            break
        time.sleep(0.1)

    # Configurar variaveis baseadas na escolha
    selected_years = [2024]
    plot_title = "Ano 2024"

    if choice == '0':
        print("Operacao cancelada.")
        sys.exit(0)
    elif choice == '2':
        selected_years = [2022, 2023, 2024]
        plot_title = "Media 2022-2024"
    elif choice == '1':
        selected_years = [2024]
        plot_title = "Ano 2024"
    
    print(f"\nIniciando processamento para: {plot_title}")
    run(selected_years, plot_title)