"""
PROJECT:     Cognitive Capital Analysis - Brazil
SCRIPT:      analise_kendall_2015_v2.py
SOURCE:      INEP (SAEB/ENEM) & OCDE (PISA)
ROLE:        Automated Statistical Reporting
DATE:        2026-01-13 (v2.0 - Path Auto-Discovery)

DESCRIPTION:
    Calculates Kendall's Coefficient of Concordance (W) for the 2015 wave.
    Generates tabular reports (CSV/XLSX) and visualization figures.
    
    Inputs (XLSX format):
      - 2015_pisa_states.xlsx
      - 2015_saeb_table_9EF.xlsx
      - 2015_enem_table_3EM.xlsx
    
    Outputs:
      - tabela_kendall_2015.xlsx
      - tabela_kendall_2015.csv
      - grafico_fluxo_rankings_2015.png
      - grafico_dispersao_bolhas_2015.png
"""
    
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pandas.plotting import parallel_coordinates
import os
import sys

# ==============================================================================
# CONFIGURAÇÃO DE DIRETÓRIOS (Ref: src/cog)
# ==============================================================================
# Caminhos de Saída (Output)
DIRS = {
    "csv":      "../../data/processed",
    "xlsx":     "../../reports/varcog/xlsx",
    "graficos": "../../reports/varcog/graficos"
}

# Caminhos de Busca para Entrada (Input)
SEARCH_PATHS = [
    "../../reports/varcog/xlsx",  
    ".",                          
    "../../../data/processed"    
]

FILES = {
    "pisa": "2015_pisa_states.xlsx",
    "saeb": "2015_saeb_table_9EF.xlsx",
    "enem": "2015_enem_table_3EM.xlsx"
}

OUTPUT_PREFIX = "resultado_kendall_2015"

# ==============================================================================
# FUNÇÕES AUXILIARES
# ==============================================================================
def ensure_directories():
    """Garante que as pastas de saída existem."""
    for key, path in DIRS.items():
        if not os.path.exists(path):
            try:
                os.makedirs(path)
                print(f"[INFO] Pasta criada: {path}")
            except OSError:
                print(f"[AVISO] Não foi possível criar/acessar: {path}")

def find_file_path():
    """Tenta encontrar onde os arquivos Excel de entrada estão."""
    print(">> Procurando arquivos de entrada...")
    for path in SEARCH_PATHS:
        full_path = os.path.abspath(path)
        if os.path.exists(full_path):
            test_file = os.path.join(full_path, FILES["pisa"])
            if os.path.exists(test_file):
                print(f"   [INPUT ENCONTRADO] {full_path}")
                return full_path
    return None

def find_col(df, candidates):
    for col in candidates:
        if col in df.columns: return col
    return None

def load_and_prep_data(base_path):
    print(">> Carregando dados...")
    try:
        df_pisa = pd.read_excel(os.path.join(base_path, FILES["pisa"]))
        df_saeb = pd.read_excel(os.path.join(base_path, FILES["saeb"]))
        df_enem = pd.read_excel(os.path.join(base_path, FILES["enem"]))

        col_pisa = find_col(df_pisa, ["Cognitive_Global_Mean", "Mean_Global", "PISA_Score", "Media_Global"])
        col_saeb = find_col(df_saeb, ["SAEB_General", "MEDIA_9EF_LP_MT", "SAEB_Score", "Media_Padronizada"])
        col_enem = find_col(df_enem, ["Mean_General", "ENEM_General", "ENEM_Score", "Média_Geral"])

        if not all([col_pisa, col_saeb, col_enem]):
            print("[ERRO] Colunas não identificadas.")
            return None

        df_pisa = df_pisa[['UF', col_pisa]].rename(columns={col_pisa: 'PISA_2015'})
        df_saeb = df_saeb[['UF', col_saeb]].rename(columns={col_saeb: 'SAEB_9EF'})
        df_enem = df_enem[['UF', col_enem]].rename(columns={col_enem: 'ENEM_3EM'})

        merged = df_pisa.merge(df_saeb, on='UF').merge(df_enem, on='UF')
        return merged
    except Exception as e:
        print(f"[ERRO CRÍTICO] {e}")
        return None

def calculate_w(df):
    df['Rank_PISA'] = df['PISA_2015'].rank(ascending=False)
    df['Rank_SAEB'] = df['SAEB_9EF'].rank(ascending=False)
    df['Rank_ENEM'] = df['ENEM_3EM'].rank(ascending=False)

    m = 3
    n = len(df)
    df['Sum_Ranks'] = df['Rank_PISA'] + df['Rank_SAEB'] + df['Rank_ENEM']
    mean_sum = m * (n + 1) / 2
    df['Squared_Deviation'] = (df['Sum_Ranks'] - mean_sum) ** 2
    S = df['Squared_Deviation'].sum()
    W = (12 * S) / (m**2 * (n**3 - n))
    
    return df, W

def generate_plots(df, W):
    print(">> Gerando Gráficos...")
    sns.set(style="whitegrid")

    # --- GRÁFICO 1: COORDENADAS PARALELAS ---
    plt.figure(figsize=(14, 8))
    plot_df = df[['UF', 'Rank_PISA', 'Rank_SAEB', 'Rank_ENEM', 'Sum_Ranks']].copy()
    
    conditions = [
        (plot_df['Sum_Ranks'] <= 20),
        (plot_df['Sum_Ranks'] > 20) & (plot_df['Sum_Ranks'] <= 60),
        (plot_df['Sum_Ranks'] > 60)
    ]
    choices = ['Topo', 'Médio', 'Base']
    plot_df['Performance'] = np.select(conditions, choices, default='Médio')
    custom_colors = ['#2ca02c', '#1f77b4', '#d62728']
    
    parallel_coordinates(plot_df[['Rank_PISA', 'Rank_SAEB', 'Rank_ENEM', 'Performance']], 
                         'Performance', color=custom_colors, alpha=0.7, linewidth=2.5)
    
    plt.gca().invert_yaxis()
    plt.title(f'Consistência Hierárquica dos Estados (2015)\nCoeficiente de Kendall (W) = {W:.3f}', fontsize=16)
    plt.ylabel('Posição no Ranking', fontsize=12)
    plt.legend(loc='upper right')
    plt.tight_layout()
    
    # Salvar na pasta de GRÁFICOS
    save_path1 = os.path.join(DIRS["graficos"], f'{OUTPUT_PREFIX}_fluxo_rankings.png')
    plt.savefig(save_path1, dpi=300)
    plt.close()

    # --- GRÁFICO 2: DISPERSÃO ---
    plt.figure(figsize=(12, 10))
    pisa_min, pisa_max = df['PISA_2015'].min(), df['PISA_2015'].max()
    sizes = 100 + (df['PISA_2015'] - pisa_min) / (pisa_max - pisa_min) * 500
    
    scatter = plt.scatter(df['SAEB_9EF'], df['ENEM_3EM'], s=sizes, c=df['Sum_Ranks'], 
                          cmap='viridis_r', alpha=0.85, edgecolors='gray')
    
    for i, row in df.iterrows():
        plt.text(row['SAEB_9EF']+0.3, row['ENEM_3EM']+0.3, row['UF'], fontsize=9, fontweight='bold')

    z = np.polyfit(df['SAEB_9EF'], df['ENEM_3EM'], 1)
    plt.plot(df['SAEB_9EF'], np.poly1d(z)(df['SAEB_9EF']), "r--", alpha=0.4)
    plt.colorbar(scatter, label='Soma dos Rankings')
    plt.title('Dispersão Triangulada: SAEB x ENEM x PISA (2015)', fontsize=16)
    plt.xlabel('Nota Média SAEB (9º Ano)', fontsize=12)
    plt.ylabel('Nota Média ENEM (3º Ano)', fontsize=12)
    plt.tight_layout()
    
    # Salvar na pasta de GRÁFICOS
    save_path2 = os.path.join(DIRS["graficos"], f'{OUTPUT_PREFIX}_dispersao.png')
    plt.savefig(save_path2, dpi=300)
    plt.close()

# ==============================================================================
# MAIN
# ==============================================================================
if __name__ == "__main__":
    print("-" * 60)
    print("SCRIPT DE ANÁLISE KENDALL 2015 (OUTPUT DIRECIONADO)")
    print("-" * 60)

    ensure_directories()
    input_path = find_file_path()
    
    if input_path:
        df = load_and_prep_data(input_path)
        if df is not None:
            df_final, W = calculate_w(df)
            print(f"\n>>> RESULTADO FINAL: W = {W:.4f} <<<\n")
            
            export_df = df_final[[
                'UF', 'PISA_2015', 'Rank_PISA', 'SAEB_9EF', 'Rank_SAEB', 
                'ENEM_3EM', 'Rank_ENEM', 'Sum_Ranks', 'Squared_Deviation'
            ]].sort_values('Sum_Ranks')
            
            # Exportar CSV para DATA/PROCESSED
            csv_target = os.path.join(DIRS["csv"], f'{OUTPUT_PREFIX}_tabela.csv')
            export_df.to_csv(csv_target, index=False)
            print(f">> CSV salvo em: {csv_target}")

            # Exportar XLSX para REPORTS/VARCOG/XLSX
            xlsx_target = os.path.join(DIRS["xlsx"], f'{OUTPUT_PREFIX}_tabela.xlsx')
            export_df.to_excel(xlsx_target, index=False)
            print(f">> XLSX salvo em: {xlsx_target}")

            # Gerar Gráficos em REPORTS/VARCOG/GRAFICOS
            generate_plots(df_final, W)
            print(f">> Gráficos salvos em: {DIRS['graficos']}")
            
            print("-" * 60)
            print("Sucesso.")
    else:
        print("[ERRO] Arquivos de entrada não encontrados.")