"""
================================================================================
PROJECT:    Geography of Cognition: Poverty, Wealth, and Inequalities in Brazil
SCRIPT:     src/ind/ind_01_snis_saneamento.py
VERSION:    1.5 (Added Cancel Option & Smart Timeout)
DATE:       2026-01-30
--------------------------------------------------------------------------------
PRINCIPAL INVESTIGATOR:  Dr. José Aparecido da Silva
LEAD DEVELOPER:          Specialist in Applied Statistics
SOURCE:                  SNIS (Sistema Nacional de Informações sobre Saneamento)
================================================================================

ABSTRACT:
    Automated ETL pipeline for the National Saneamento Information System (SNIS).
    Extracts and aggregates municipal data to state-level (UF) indicators.
    Now includes interactive options to generate specific visualization types.

DATA SOURCE:
    - SNIS 2022 Raw Municipal Dataset (Official Brazilian Government data).
    - URL: http://app4.mdr.gov.br/serieHistorica/ (Selected: Municípios / 2022)

OUTPUTS:
    1. CSV: data/processed/indicadores/ind01_saneamento.csv
    2. XLSX: reports/indicadores/xlsx/ind01_saneamento.xlsx
    3. PLOTS: 
        - reports/indicadores/graficos/ind01_saneamento_quadrant.png
        - reports/indicadores/graficos/ind01_agua_bar.png
        - reports/indicadores/graficos/ind01_esgoto_bar.png

TABLE CONTENT:
    - SG_UF_PROVA: State abbreviation (Federation Unit).
    - AGUA_ATENDIMENTO_PERC: Mean percentage of population with water access.
    - ESGOTO_ATENDIMENTO_PERC: Mean percentage of population with sewage collection.

VISUALIZATION:
    - Bivariate Scatter Plot (Quadrant Analysis).
    - Ranked Bar Charts (Water & Sewage coverage by State).

DEPENDENCIES:
    pandas, matplotlib, seaborn, openpyxl, io, sys, threading, time
================================================================================
"""
import pandas as pd
import os
import io
import sys
import time
import threading
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path

# --- Path Configuration ---
BASE_DIR = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(BASE_DIR))

# Input/Output Paths
RAW_DIR = BASE_DIR / "data" / "raw" / "indicadores" / "ind01_bio"
OUT_CSV_DIR = BASE_DIR / "data" / "processed" / "indicadores"
OUT_XLSX_DIR = BASE_DIR / "reports" / "indicadores" / "xlsx"
OUT_PLOT_DIR = BASE_DIR / "reports" / "indicadores" / "graficos"

# INPUT FILENAME
INPUT_FILE = RAW_DIR / "snis_municipios_2022.csv"

# Valid Federation Unit acronyms
SIGLAS_UF = [
    'AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA', 'MT', 'MS', 
    'MG', 'PA', 'PB', 'PR', 'PE', 'PI', 'RJ', 'RN', 'RS', 'RO', 'RR', 'SC', 
    'SP', 'SE', 'TO'
]

# State Name to Acronym Mapping
DE_PARA_UF = {
    'Acre': 'AC', 'Alagoas': 'AL', 'Amapá': 'AP', 'Amazonas': 'AM', 'Bahia': 'BA', 
    'Ceará': 'CE', 'Distrito Federal': 'DF', 'Espírito Santo': 'ES', 'Goiás': 'GO', 
    'Maranhão': 'MA', 'Mato Grosso': 'MT', 'Mato Grosso do Sul': 'MS', 'Minas Gerais': 'MG', 
    'Pará': 'PA', 'Paraíba': 'PB', 'Paraná': 'PR', 'Pernambuco': 'PE', 'Piauí': 'PI', 
    'Rio de Janeiro': 'RJ', 'Rio Grande do Norte': 'RN', 'Rio Grande do Sul': 'RS', 
    'Rondônia': 'RO', 'Roraima': 'RR', 'Santa Catarina': 'SC', 'São Paulo': 'SP', 
    'Sergipe': 'SE', 'Tocantins': 'TO'
}

# Region Mapping for Colors
UF_TO_REGIAO = {
    'AC': 'Norte', 'AP': 'Norte', 'AM': 'Norte', 'PA': 'Norte', 'RO': 'Norte', 'RR': 'Norte', 'TO': 'Norte',
    'AL': 'Nordeste', 'BA': 'Nordeste', 'CE': 'Nordeste', 'MA': 'Nordeste', 'PB': 'Nordeste', 'PE': 'Nordeste', 'PI': 'Nordeste', 'RN': 'Nordeste', 'SE': 'Nordeste',
    'DF': 'Centro-Oeste', 'GO': 'Centro-Oeste', 'MT': 'Centro-Oeste', 'MS': 'Centro-Oeste',
    'ES': 'Sudeste', 'MG': 'Sudeste', 'RJ': 'Sudeste', 'SP': 'Sudeste',
    'PR': 'Sul', 'RS': 'Sul', 'SC': 'Sul'
}

def generate_quadrant_plot(df):
    """Generates the Scatter Plot (Water x Sewage)."""
    sns.set_theme(style="whitegrid")
    plt.figure(figsize=(14, 10))
    
    plot = sns.scatterplot(
        data=df, 
        x='AGUA_ATENDIMENTO_PERC', 
        y='ESGOTO_ATENDIMENTO_PERC',
        s=100,
        color='teal',
        edgecolor='black'
    )
    
    for i in range(df.shape[0]):
        plt.text(
            df.AGUA_ATENDIMENTO_PERC[i] + 0.7, 
            df.ESGOTO_ATENDIMENTO_PERC[i], 
            df.SG_UF_PROVA[i], 
            fontsize=10, 
            fontweight='bold'
        )
    
    plt.title('IND-01: Infraestrutura Sanitaria - Agua vs Esgoto (2022)', fontsize=16, pad=20)
    plt.xlabel('Cobertura de Abastecimento de Agua (%)', fontsize=12)
    plt.ylabel('Cobertura de Coleta de Esgoto (%)', fontsize=12)
    
    plt.axhline(90, color='red', linestyle='--', alpha=0.5, label='Meta Marco Legal')
    plt.axvline(99, color='blue', linestyle='--', alpha=0.5)
    plt.legend()
    
    plot_path = OUT_PLOT_DIR / 'ind01_saneamento_quadrant.png'
    plt.savefig(plot_path, dpi=300, bbox_inches='tight')
    print(f"[GRAPHIC] Quadrant plot saved to: {plot_path}")
    plt.close()

def generate_bar_plot(df, column, title, filename, color_map):
    """Generates a ranked Bar Plot for a specific indicator."""
    sns.set_theme(style="whitegrid")
    plt.figure(figsize=(12, 6))
    
    # Sort data
    df_sorted = df.sort_values(column, ascending=True)
    
    # Map colors based on Region
    df_sorted['REGIAO'] = df_sorted['SG_UF_PROVA'].map(UF_TO_REGIAO)
    colors = df_sorted['REGIAO'].map(color_map).fillna('gray')

    plt.bar(df_sorted['SG_UF_PROVA'], df_sorted[column], color=colors)
    
    plt.title(title, fontsize=14)
    plt.ylabel('Atendimento (%)')
    plt.xlabel('Unidade da Federacao')
    plt.ylim(0, 100)
    
    # Legend
    from matplotlib.patches import Patch
    legend_elements = [Patch(facecolor=c, label=r) for r, c in color_map.items()]
    plt.legend(handles=legend_elements, title="Regiao")
    
    plot_path = OUT_PLOT_DIR / filename
    plt.savefig(plot_path, dpi=300, bbox_inches='tight')
    print(f"[GRAPHIC] Bar plot saved to: {plot_path}")
    plt.close()

def extract_snis_data(selected_plots='all'):
    print("Initiating SNIS Data Extraction...")
    
    if not INPUT_FILE.exists():
        print(f"[CRITICAL ERROR] Input file not found: {INPUT_FILE}")
        return

    try:
        # 1. Read File (Handling encoding)
        encoding_detected = 'utf-16-le'
        try:
            with open(INPUT_FILE, 'r', encoding='utf-16-le') as f: f.read(100)
        except:
            encoding_detected = 'utf-8'

        buffer_clean = io.StringIO()
        with open(INPUT_FILE, 'r', encoding=encoding_detected, errors='replace') as f:
            for line in f:
                line = line.strip()
                if not line: continue
                if line.endswith(';'): line = line[:-1]
                buffer_clean.write(line + '\n')
        
        buffer_clean.seek(0)
        df = pd.read_csv(buffer_clean, sep=';', on_bad_lines='warn')
        df.columns = [str(c).strip().replace('"', '') for c in df.columns]

        # 2. Identify Columns
        col_uf = next((c for c in df.columns if c in ['Estado', 'UF', 'Sigla']), None)
        col_water = next((c for c in df.columns if 'IN055' in c), None)
        col_sewage = next((c for c in df.columns if 'IN056' in c), None)

        if not all([col_uf, col_water, col_sewage]):
            print(f"ERROR: Columns missing. Found: UF={col_uf}, Water={col_water}, Sewage={col_sewage}")
            return

        # 3. Clean and Filter
        df[col_uf] = df[col_uf].astype(str).str.strip().str.replace('"', '')
        df['SG_UF_PROVA'] = df[col_uf].map(DE_PARA_UF).fillna(df[col_uf])
        df = df[df['SG_UF_PROVA'].isin(SIGLAS_UF)].copy()

        # Convert numbers
        for col in [col_water, col_sewage]:
            df[col] = df[col].astype(str).str.replace('.', '', regex=False)
            df[col] = df[col].str.replace(',', '.', regex=False)
            df[col] = pd.to_numeric(df[col], errors='coerce')

        # 4. Aggregate by State
        df_final = df.groupby('SG_UF_PROVA').agg({
            col_water: 'mean',
            col_sewage: 'mean'
        }).reset_index()

        df_final.columns = ['SG_UF_PROVA', 'AGUA_ATENDIMENTO_PERC', 'ESGOTO_ATENDIMENTO_PERC']
        df_final = df_final.round(2)

        # 5. Export Data
        for d in [OUT_CSV_DIR, OUT_XLSX_DIR, OUT_PLOT_DIR]:
            d.mkdir(parents=True, exist_ok=True)
            
        file_xlsx = OUT_XLSX_DIR / 'ind01_saneamento.xlsx'
        file_csv = OUT_CSV_DIR / 'ind01_saneamento.csv'
        
        df_final.to_excel(file_xlsx, index=False, sheet_name='SNIS_2022')
        df_final.to_csv(file_csv, index=False, sep=';', encoding='utf-8-sig')
        print(f"[SUCCESS] Data saved to {file_csv}")

        # 6. Generate Plots
        region_colors = {
            'Norte': 'green', 'Nordeste': 'red', 'Sudeste': 'blue', 
            'Sul': 'cyan', 'Centro-Oeste': 'orange'
        }

        if selected_plots in ['all', 'quadrant']:
            generate_quadrant_plot(df_final)
        
        if selected_plots in ['all', 'barras']:
            generate_bar_plot(
                df_final, 
                'AGUA_ATENDIMENTO_PERC', 
                'IND-01: Cobertura de Agua (SNIS 2022) por Estado', 
                'ind01_agua_bar.png',
                region_colors
            )
            generate_bar_plot(
                df_final, 
                'ESGOTO_ATENDIMENTO_PERC', 
                'IND-01: Cobertura de Esgoto (SNIS 2022) por Estado', 
                'ind01_esgoto_bar.png',
                region_colors
            )

    except Exception as e:
        print(f"FATAL SCRIPT ERROR: {e}")

if __name__ == "__main__":
    print("--- SNIS GENERATOR ---")
    print("Selecione os graficos desejados:")
    print("1 - Apenas Dispersao (Agua x Esgoto)")
    print("2 - Apenas Barras (Ranking por Estado)")
    print("3 - Todos (Padrao)")
    print("0 - Cancelar e Sair")
    
    # --- SNIPPET: INPUT COM TIMEOUT ---
    print("\nVoce tem 10 segundos para escolher... (Padrao: 3)")
    
    choice = '3' # Valor padrao se o tempo acabar
    
    def get_input():
        global choice
        try:
            user_input = input("Opcao (0/1/2/3): ").strip()
            if user_input:
                choice = user_input
        except EOFError:
            pass

    # Cria uma thread para esperar o input
    input_thread = threading.Thread(target=get_input)
    input_thread.daemon = True
    input_thread.start()

    # Conta o tempo
    timeout = 10
    start_time = time.time()
    
    while input_thread.is_alive():
        elapsed = time.time() - start_time
        remaining = int(timeout - elapsed)
        if remaining <= 0:
            print("\nTempo esgotado! Assumindo opcao padrao (3 - Todos).")
            break
        # Mostra contador na mesma linha (se quiser) ou apenas espera
        time.sleep(0.1)
    
    # Se a thread ainda estiver viva depois do timeout, nao podemos "matar" input(),
    # mas o script segue usando o valor padrao 'choice'.
    # --- FIM DO SNIPPET ---

    print(f"\nOpcao selecionada: {choice}")

    if choice == '0':
        print("Operacao cancelada pelo usuario.")
        sys.exit(0)
    
    selection = 'all'
    if choice == '1': selection = 'quadrant'
    elif choice == '2': selection = 'barras'
    
    extract_snis_data(selected_plots=selection)