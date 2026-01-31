"""
================================================================================
PROJECT:    Geography of Cognition: Poverty, Wealth, and Inequalities in Brazil
SCRIPT:     src/ind/ind_01_datasus_mortalidade.py
VERSION:    1.1 (Fix: Handles TabNet 'Region/UF' hierarchy format with dots)
DATE:       2026-01-30
--------------------------------------------------------------------------------
PRINCIPAL INVESTIGATOR:  Dr. José Aparecido da Silva
LEAD DEVELOPER:          Specialist in Applied Statistics
SOURCE:                  DATASUS (SIM - Mortalidade & SINASC - Nascidos Vivos)
================================================================================

ABSTRACT:
    Automated ETL pipeline to calculate Infant Mortality Rate (TMI).
    Cross-references mortality data (SIM) with birth data (SINASC) by Residence.
    Computes annual rates (2022-2024) and a consolidated recent mean.

DATA SOURCE:
    - SIM (Obitos): http://tabnet.datasus.gov.br/cgi/deftohtm.exe?sim/cnv/inf10uf.def
    - SINASC (Nascimentos): http://tabnet.datasus.gov.br/cgi/deftohtm.exe?sinasc/cnv/nvuf.def
    - Period: 2022, 2023, 2024.

INPUTS:
    1. data/raw/indicadores/ind01_bio/obitos_infantis_residencia_2022_2023_2024.csv
    2. data/raw/indicadores/ind01_bio/nascimentos_infantis_residencia_2022_2023_2024.csv

OUTPUTS:
    1. CSV: data/processed/indicadores/ind01_mortalidade.csv
    2. XLSX: reports/indicadores/xlsx/ind01_mortalidade.xlsx
    3. PLOT: reports/indicadores/graficos/ind01_mortalidade_bar.png

TABLE CONTENT:
    - UF: Federation Unit.
    - REGIAO: Geo-political Region (Norte, Sul, etc.).
    - TMI_2022, TMI_2023, TMI_2024: Annual Infant Mortality Rate (per 1,000 live births).
    - TMI_MEDIA_RECENTE: Mean rate of the analyzed period.

DEPENDENCIES:
    pandas, matplotlib, seaborn, openpyxl, sys, pathlib
================================================================================
"""
import pandas as pd
import numpy as np
import sys
import os
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

# Create directories if they don't exist
for d in [OUT_CSV_DIR, OUT_XLSX_DIR, OUT_PLOT_DIR]:
    d.mkdir(parents=True, exist_ok=True)

# Mapping Name -> Acronym (Essential for TabNet hierarchical format)
NAME_TO_UF = {
    'Rondônia': 'RO', 'Acre': 'AC', 'Amazonas': 'AM', 'Roraima': 'RR', 'Pará': 'PA', 'Amapá': 'AP', 'Tocantins': 'TO',
    'Maranhão': 'MA', 'Piauí': 'PI', 'Ceará': 'CE', 'Rio Grande do Norte': 'RN', 'Paraíba': 'PB', 'Pernambuco': 'PE', 
    'Alagoas': 'AL', 'Sergipe': 'SE', 'Bahia': 'BA',
    'Minas Gerais': 'MG', 'Espírito Santo': 'ES', 'Rio de Janeiro': 'RJ', 'São Paulo': 'SP',
    'Paraná': 'PR', 'Santa Catarina': 'SC', 'Rio Grande do Sul': 'RS',
    'Mato Grosso do Sul': 'MS', 'Mato Grosso': 'MT', 'Goiás': 'GO', 'Distrito Federal': 'DF'
}

# Mapping Acronym -> Region
UF_TO_REGIAO = {
    'AC': 'Norte', 'AP': 'Norte', 'AM': 'Norte', 'PA': 'Norte', 'RO': 'Norte', 'RR': 'Norte', 'TO': 'Norte',
    'AL': 'Nordeste', 'BA': 'Nordeste', 'CE': 'Nordeste', 'MA': 'Nordeste', 'PB': 'Nordeste', 'PE': 'Nordeste', 'PI': 'Nordeste', 'RN': 'Nordeste', 'SE': 'Nordeste',
    'DF': 'Centro-Oeste', 'GO': 'Centro-Oeste', 'MT': 'Centro-Oeste', 'MS': 'Centro-Oeste',
    'ES': 'Sudeste', 'MG': 'Sudeste', 'RJ': 'Sudeste', 'SP': 'Sudeste',
    'PR': 'Sul', 'RS': 'Sul', 'SC': 'Sul'
}

def clean_datasus_hierarchical(filepath):
    """
    Reads TabNet CSV in 'Region/UF' format (lines starting with '.. StateName').
    """
    print(f"[PROCESSING] Reading file: {filepath.name}")
    try:
        # Skip initial header rows (usually 3 for TabNet)
        df = pd.read_csv(filepath, sep=';', encoding='latin1', skiprows=3, engine='python')
    except Exception as e:
        print(f"[ERROR] Reading file: {e}")
        return None, []

    # Identify the first column
    col_nome = df.columns[0]
    
    # Filter lines that look like states: "  .. StateName"
    # Logic: String must contain '..'
    df = df[df[col_nome].astype(str).str.contains(r'\.\.', na=False)].copy()

    # Clean the name: Remove '..' and whitespace
    df['STATE_NAME'] = df[col_nome].astype(str).str.replace(r'\.\.', '', regex=True).str.strip()
    
    # Map to Acronym
    df['UF'] = df['STATE_NAME'].map(NAME_TO_UF)
    
    # Check for unmapped states
    missing = df[df['UF'].isna()]['STATE_NAME'].unique()
    if len(missing) > 0:
        print(f"[WARNING] Unmapped states found: {missing}")

    # Identify Year Columns
    cols_anos = [c for c in df.columns if c.strip().isdigit() and len(c.strip()) == 4]
    
    if not cols_anos:
        print(f"[WARNING] No year columns found in {filepath.name}")
        return df, []

    # Clean numeric values
    for ano in cols_anos:
        df[ano] = (
            df[ano].astype(str)
            .str.replace('-', '0')
            .str.replace(',', '.')
            .astype(float)
        )
    
    return df[['UF'] + cols_anos], cols_anos

def generate_visuals(df):
    """Generates a bar chart for the recent mean TMI."""
    sns.set_theme(style="whitegrid")
    plt.figure(figsize=(12, 6))
    
    # Sort for better visualization
    df_sorted = df.sort_values('TMI_MEDIA_RECENTE', ascending=True)
    
    colors = df_sorted['REGIAO'].map({
        'Norte': 'green', 'Nordeste': 'red', 'Sudeste': 'blue', 
        'Sul': 'cyan', 'Centro-Oeste': 'orange'
    }).fillna('gray')

    plt.bar(df_sorted['UF'], df_sorted['TMI_MEDIA_RECENTE'], color=colors)
    
    plt.title('IND-01: Taxa de Mortalidade Infantil Media (2022-2024) por UF', fontsize=14)
    plt.ylabel('Obitos por 1.000 Nascidos Vivos')
    plt.xlabel('Unidade da Federacao')
    
    # Create custom legend
    from matplotlib.patches import Patch
    legend_elements = [Patch(facecolor=c, label=r) for r, c in 
                       {'Norte':'green', 'Nordeste':'red', 'Sudeste':'blue', 'Sul':'cyan', 'Centro-Oeste':'orange'}.items()]
    plt.legend(handles=legend_elements, title="Regiao")
    
    plot_path = OUT_PLOT_DIR / "ind01_mortalidade_bar.png"
    plt.savefig(plot_path, dpi=300, bbox_inches='tight')
    print(f"[GRAPHIC] Visualization saved to: {plot_path}")

def run():
    print("--- IND-01: INFANT MORTALITY ETL (2022-2024) ---")
    
    # Input filenames
    f_obitos = RAW_DIR / "obitos_infantis_residencia_2022_2023_2024.csv"
    f_nascidos = RAW_DIR / "nascimentos_infantis_residencia_2022_2023_2024.csv"

    if not f_obitos.exists() or not f_nascidos.exists():
        print(f"[CRITICAL ERROR] Input files not found in {RAW_DIR}")
        return

    # 1. Load and Clean
    df_obitos, anos_obitos = clean_datasus_hierarchical(f_obitos)
    df_nasc, anos_nasc = clean_datasus_hierarchical(f_nascidos)

    if df_obitos is None or df_nasc is None: return

    # 2. Identify Common Years
    anos_comuns = sorted(list(set(anos_obitos) & set(anos_nasc)))
    print(f"[INFO] Years identified: {anos_comuns}")

    # 3. Merge
    df_final = pd.merge(df_obitos, df_nasc, on='UF', suffixes=('_OBITO', '_NASC'))

    # 4. Calculate Rates
    cols_taxa = []
    for ano in anos_comuns:
        col_res = f'TMI_{ano}'
        df_final[col_res] = np.where(
            df_final[f'{ano}_NASC'] > 0,
            (df_final[f'{ano}_OBITO'] / df_final[f'{ano}_NASC']) * 1000,
            0
        )
        df_final[col_res] = df_final[col_res].round(2)
        cols_taxa.append(col_res)

    # 5. Calculate Mean
    df_final['TMI_MEDIA_RECENTE'] = df_final[cols_taxa].mean(axis=1).round(2)

    # 6. Add Region
    df_final['REGIAO'] = df_final['UF'].map(UF_TO_REGIAO)

    # 7. Export
    cols_finais = ['UF', 'REGIAO', 'TMI_MEDIA_RECENTE'] + cols_taxa
    df_export = df_final[cols_finais].sort_values('TMI_MEDIA_RECENTE', ascending=False)

    if len(df_export) != 27:
        print(f"[WARNING] Result has {len(df_export)} UFs (Expected 27).")

    outfile_csv = OUT_CSV_DIR / "ind01_mortalidade.csv"
    df_export.to_csv(outfile_csv, index=False, sep=',', encoding='utf-8')
    
    outfile_xlsx = OUT_XLSX_DIR / "ind01_mortalidade.xlsx"
    df_export.to_excel(outfile_xlsx, index=False, sheet_name='Mortalidade_22_24')

    print(f"[SUCCESS] Data saved to: {outfile_csv}")
    print(f"[SUCCESS] Excel saved to: {outfile_xlsx}")
    
    generate_visuals(df_export)

if __name__ == "__main__":
    run()