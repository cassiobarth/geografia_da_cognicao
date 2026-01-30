"""
================================================================================
PROJECT:       COGNITIVE CAPITAL ANALYSIS - BRAZIL
SCRIPT:        src/cog/visualize_waves_scatter.py
VERSION:       1.1 (Fix: Auto-Column Detection & Fallbacks)
DESCRIPTION:   Gera Scatter Plots (Gráficos de Dispersão) para as 3 Ondas.
               Visualiza a correlação entre Avaliação Internacional (PISA)
               e Avaliações Nacionais (ENEM/SAEB).
               
               - Correção: Detecta nomes de colunas (UF vs SG_UF_PROVA) automaticamente.
               - Fallback: Usa SAEB 9EF se 3EM não estiver disponível (caso de 2015).
OUTPUT:        reports/varcog/graficos/scatter_wave_{YEAR}_{SOURCE}.png
================================================================================
"""

import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np
from pathlib import Path

# --- CONFIGURAÇÃO ---
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DATA_PROC = PROJECT_ROOT / 'data' / 'processed'
REPORTS_XLSX = PROJECT_ROOT / 'reports' / 'varcog' / 'xlsx'
IMG_DIR = PROJECT_ROOT / 'reports' / 'varcog' / 'graficos'
IMG_DIR.mkdir(parents=True, exist_ok=True)

# Mapeamento UF -> Região
UF_TO_REGION = {
    'AC':'North', 'AL':'Northeast', 'AP':'North', 'AM':'North', 'BA':'Northeast', 'CE':'Northeast', 
    'DF':'Center-West', 'ES':'Southeast', 'GO':'Center-West', 'MA':'Northeast', 'MT':'Center-West', 
    'MS':'Center-West', 'MG':'Southeast', 'PA':'North', 'PB':'Northeast', 'PR':'South', 
    'PE':'Northeast', 'PI':'Northeast', 'RJ':'Southeast', 'RN':'Northeast', 'RS':'South', 
    'RO':'North', 'RR':'North', 'SC':'South', 'SP':'Southeast', 'SE':'Northeast', 'TO':'North'
}

# Configuração Visual
sns.set_style("whitegrid")
plt.rcParams.update({'font.size': 12})

def load_file_smart(path_obj):
    """Carrega arquivo com fallback (ex: 3EM -> 9EF)."""
    if path_obj.exists():
        return pd.read_csv(path_obj) if path_obj.suffix == '.csv' else pd.read_excel(path_obj)
    
    # Fallback SAEB
    if 'saeb' in str(path_obj).lower() and '3EM' in str(path_obj):
        alt_path = Path(str(path_obj).replace('3EM', '9EF'))
        if alt_path.exists():
            print(f"   [INFO] Fallback: Usando {alt_path.name}")
            return pd.read_csv(alt_path) if alt_path.suffix == '.csv' else pd.read_excel(alt_path)
    return None

def normalize_cols(df, prefix):
    """Padroniza colunas (KEY e Score) baseado no prefixo (PISA/ENEM/SAEB)."""
    
    # Lista de sinônimos possíveis
    key_syns = ['UF', 'SG_UF_PROVA', 'SG_UF', 'Region', 'REGION', 'Estado']
    
    # Sinônimos de Score por fonte
    if prefix == 'PISA':
        score_syns = ['Cognitive_Global_Mean', 'Mean_General']
    elif prefix == 'ENEM':
        score_syns = ['Mean_General', 'Enem_Global_Mean']
    elif prefix == 'SAEB':
        score_syns = ['SAEB_General', 'MEDIA_MT_LP', 'MEDIA_TOTAL', 'Global_Mean']
    else:
        score_syns = ['Score', 'Mean']

    # 1. Encontrar Chave (UF/Region)
    found_key = next((c for c in key_syns if c in df.columns), None)
    if not found_key:
        print(f"   [AVISO] Chave não encontrada em {prefix}. Cols: {list(df.columns)}")
        return None

    # 2. Encontrar Score
    found_score = next((c for c in score_syns if c in df.columns), None)
    if not found_score:
        # Tenta calcular se tiver componentes
        if 'Math_Mean' in df.columns and 'Language_Mean' in df.columns:
            df['Calculated_Score'] = (df['Math_Mean'] + df['Language_Mean']) / 2
            found_score = 'Calculated_Score'
        elif 'MEDIA_MT' in df.columns and 'MEDIA_LP' in df.columns:
            df['Calculated_Score'] = (df['MEDIA_MT'] + df['MEDIA_LP']) / 2
            found_score = 'Calculated_Score'
        else:
            print(f"   [AVISO] Score não encontrado em {prefix}. Cols: {list(df.columns)}")
            return None

    # Renomear
    return df.rename(columns={found_key: 'KEY', found_score: f'{prefix}_Score'})[['KEY', f'{prefix}_Score']]

def aggregate_to_region(df):
    """Converte dados estaduais para regionais (Média)."""
    if df is None or df.empty: return df
    
    # Verifica se KEY é UF
    sample = str(df['KEY'].iloc[0])
    if len(sample) == 2 and sample in UF_TO_REGION:
        df['Region'] = df['KEY'].map(UF_TO_REGION)
        # Calcula média das colunas de score
        score_col = [c for c in df.columns if 'Score' in c][0]
        return df.groupby('Region')[[score_col]].mean().reset_index().rename(columns={'Region': 'KEY'})
    return df

def get_data_for_wave(year):
    """Prepara o DataFrame consolidado para o ano específico."""
    
    granularity = 'State' if year == '2015' else 'Region'
    
    # 1. PISA
    pisa_file = 'pisa_2015_states.csv' if year == '2015' else f'pisa_{year}_regional_summary.csv'
    pisa_raw = load_file_smart(DATA_PROC / pisa_file)
    
    if pisa_raw is None:
        return pd.DataFrame(), granularity
        
    df_main = normalize_cols(pisa_raw, 'PISA')
    if df_main is None: return pd.DataFrame(), granularity

    # 2. ENEM
    enem_path = DATA_PROC / f'enem_table_{year}_3EM.csv'
    enem_raw = load_file_smart(enem_path)
    
    if enem_raw is not None:
        df_enem = normalize_cols(enem_raw, 'ENEM')
        if df_enem is not None:
            if granularity == 'Region': df_enem = aggregate_to_region(df_enem)
            df_main = pd.merge(df_main, df_enem, on='KEY', how='inner')
    else:
        print(f"   [SKIP] ENEM {year} não encontrado.")

    # 3. SAEB
    saeb_year = '2015' if year == '2015' else ('2017' if year == '2018' else '2023')
    saeb_path = REPORTS_XLSX / f'saeb_table_{saeb_year}_3EM.xlsx'
    saeb_raw = load_file_smart(saeb_path)
    
    if saeb_raw is not None:
        df_saeb = normalize_cols(saeb_raw, 'SAEB')
        if df_saeb is not None:
            if granularity == 'Region': df_saeb = aggregate_to_region(df_saeb)
            df_main = pd.merge(df_main, df_saeb, on='KEY', how='inner')
    else:
        print(f"   [SKIP] SAEB {saeb_year} não encontrado.")

    return df_main, granularity

def plot_scatter(df, x_col, y_col, label_col, title, filename):
    """Gera e salva o gráfico."""
    if len(df) < 2:
        print(f"   [SKIP] Poucos pontos para plotar {filename} (N={len(df)})")
        return

    plt.figure(figsize=(10, 7))
    
    # Scatter Plot com Regressão
    sns.regplot(data=df, x=x_col, y=y_col, ci=95, scatter_kws={'s': 100, 'alpha':0.7}, line_kws={'color':'red'})
    
    # Labels nos pontos
    for i in range(df.shape[0]):
        plt.text(
            df[x_col].iloc[i], 
            df[y_col].iloc[i]+(df[y_col].max()*0.01), 
            str(df[label_col].iloc[i]), 
            fontsize=9,
            weight='bold',
            color='black'
        )

    # Correlação
    corr = df[[x_col, y_col]].corr().iloc[0,1]
    
    plt.title(f"{title}\nCorrelação de Pearson (r): {corr:.3f}", fontsize=14)
    plt.xlabel(f"Pontuação Nacional ({x_col.replace('_Score', '')})")
    plt.ylabel("Pontuação Internacional (PISA)")
    
    plt.tight_layout()
    save_path = IMG_DIR / filename
    plt.savefig(save_path, dpi=300)
    plt.close()
    print(f"   [PLOT] Salvo: {save_path.name}")

def run_visuals():
    print("="*60)
    print("      GERADOR DE GRÁFICOS: ONDAS COGNITIVAS (V1.1)")
    print("="*60)
    
    waves = ['2015', '2018', '2022']
    
    for year in waves:
        print(f"\n--- Gerando Onda {year} ---")
        df, gran = get_data_for_wave(year)
        
        if df.empty:
            print(f"[SKIP] Sem dados consolidados para {year}")
            continue
            
        print(f"   Dados: {len(df)} pontos ({gran})")
            
        # Plot PISA x ENEM
        if 'ENEM_Score' in df.columns:
            plot_scatter(
                df, 'ENEM_Score', 'PISA_Score', 'KEY',
                f"Sincronia Cognitiva: PISA vs ENEM ({year})",
                f"scatter_wave_{year}_pisa_enem.png"
            )
            
        # Plot PISA x SAEB
        if 'SAEB_Score' in df.columns:
            plot_scatter(
                df, 'SAEB_Score', 'PISA_Score', 'KEY',
                f"Validação Sistêmica: PISA vs SAEB ({year})",
                f"scatter_wave_{year}_pisa_saeb.png"
            )

    print("\n" + "="*60)
    print(f"[SUCESSO] Gráficos salvos em: {IMG_DIR}")

if __name__ == "__main__":
    run_visuals()