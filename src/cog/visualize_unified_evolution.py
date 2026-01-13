"""
================================================================================
SCRIPT:        src/cog/visualize_unified_evolution.py
DESCRIPTION:   Gera um gráfico único consolidando as 3 ondas (2015, 2018, 2022).
               Eixo X: Nota ENEM | Eixo Y: Nota PISA
               
               SOLUÇÃO VISUAL (V1.4 - FIX):
               - Fix Leitura: Lê as abas corretas (_Region_Data).
               - Fix Plot: Usa scatterplot + regplot para suportar 'style' e regressão.
               
OUTPUT:        reports/varcog/graficos/evolution_pisa_enem_unified.png
================================================================================
"""

import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from pathlib import Path

# --- CONFIGURAÇÃO ---
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
INPUT_FILE = PROJECT_ROOT / 'reports' / 'varcog' / 'xlsx' / 'triangulation_waves_consolidated.xlsx'
IMG_DIR = PROJECT_ROOT / 'reports' / 'varcog' / 'graficos'
IMG_DIR.mkdir(parents=True, exist_ok=True)

# Mapa de Siglas (Para Rótulos Curtos)
REGION_ABBR = {
    'North': 'N', 'Northeast': 'NE', 'Center-West': 'CW', 
    'Southeast': 'SE', 'South': 'S',
    'Norte': 'N', 'Nordeste': 'NE', 'Centro-Oeste': 'CO', 'Sudeste': 'SE', 'Sul': 'S'
}

def load_and_prep_wave(file_path, sheet_name, year):
    """Carrega, padroniza e agrega dados de uma onda."""
    try:
        # Tenta ler o Excel. Se a aba não existir, retorna None silenciosamente para tentar a próxima opção
        try:
            df = pd.read_excel(file_path, sheet_name=sheet_name)
        except ValueError:
            return None

        # Identificar colunas dinamicamente
        pisa_col = next((c for c in df.columns if 'PISA' in c and 'Score' in c), None)
        enem_col = next((c for c in df.columns if 'ENEM' in c and 'Score' in c), None)
        
        if not pisa_col or not enem_col:
            return None

        # Padronizar nomes
        df = df.rename(columns={'KEY': 'Region', pisa_col: 'PISA_Score', enem_col: 'ENEM_Score'})
        
        df['Year'] = year
        return df[['Region', 'PISA_Score', 'ENEM_Score', 'Year']]
        
    except Exception as e:
        print(f"[ERRO] Falha técnica em {year}: {e}")
        return None

def run_plot():
    print("="*60)
    print("      GRÁFICO UNIFICADO: EVOLUÇÃO PISA x ENEM (V1.4 Fix)")
    print("="*60)

    if not INPUT_FILE.exists():
        print(f"[ERRO] Arquivo não encontrado: {INPUT_FILE}")
        return

    # 1. Carregar as 3 Ondas (Prioriza abas regionais geradas pela v1.4 da triangulação)
    df_15 = load_and_prep_wave(INPUT_FILE, '2015_Region_Data', '2015')
    if df_15 is None: df_15 = load_and_prep_wave(INPUT_FILE, '2015_Data', '2015') # Fallback

    df_18 = load_and_prep_wave(INPUT_FILE, '2018_Region_Data', '2018')
    if df_18 is None: df_18 = load_and_prep_wave(INPUT_FILE, '2018_Data', '2018')

    df_22 = load_and_prep_wave(INPUT_FILE, '2022_Region_Data', '2022')
    if df_22 is None: df_22 = load_and_prep_wave(INPUT_FILE, '2022_Data', '2022')
    
    if any(d is None for d in [df_15, df_18, df_22]):
        print("[ABORTAR] Abas do Excel não encontradas. Verifique se rodou a triangulação.")
        return

    # 2. Consolidar
    master = pd.concat([df_15, df_18, df_22], ignore_index=True)
    
    # 3. Aplicar Siglas
    master['Region_Label'] = master['Region'].map(lambda x: REGION_ABBR.get(x, str(x)[:3]))
    
    # 4. Plotagem Manual (Scatter + Regressão separados)
    plt.figure(figsize=(12, 9))
    sns.set_style("whitegrid")
    
    # A. Linhas de Tendência (Regplot não suporta hue nativo para regressão apenas, então fazemos loop)
    colors = {'2015': '#1f77b4', '2018': '#ff7f0e', '2022': '#2ca02c'} # Azul, Laranja, Verde
    
    for year in ['2015', '2018', '2022']:
        subset = master[master['Year'] == year]
        sns.regplot(
            data=subset, x='ENEM_Score', y='PISA_Score',
            scatter=False, # Não desenha pontos aqui, só a linha
            color=colors[year],
            label=f'Tendência {year}',
            ci=None
        )

    # B. Pontos (Scatterplot suporta style!)
    sns.scatterplot(
        data=master,
        x='ENEM_Score', y='PISA_Score',
        hue='Year',
        style='Region_Label', # Formas diferentes por região
        palette=colors,
        s=200, # Tamanho grande
        alpha=0.9
    )

    # Ajustes de Título e Eixos
    plt.title("Sincronia Cognitiva: Evolução PISA x ENEM (2015-2022)", fontsize=16, pad=20)
    plt.xlabel("Desempenho ENEM (Média Regional)", fontsize=13)
    plt.ylabel("Desempenho PISA (Média Regional)", fontsize=13)
    
    # Rótulos nos Pontos
    for i, row in master.iterrows():
        plt.text(
            row['ENEM_Score'] + 0.8, 
            row['PISA_Score'] + 0.8, 
            row['Region_Label'], 
            fontsize=10, 
            weight='bold',
            color='#333333'
        )

    # Legenda Inteligente
    plt.legend(bbox_to_anchor=(1.02, 1), loc='upper left', borderaxespad=0, title="Legenda")

    save_path = IMG_DIR / 'evolution_pisa_enem_unified.png'
    plt.savefig(save_path, dpi=300, bbox_inches='tight')
    
    print(f"[SUCESSO] Gráfico salvo em: {save_path}")

if __name__ == "__main__":
    run_plot()