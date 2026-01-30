"""
================================================================================
PROJECT:       COGNITIVE CAPITAL ANALYSIS - BRAZIL
SCRIPT:        src/cog/analysis_correlations_waves.py
DESCRIPTION:   Gera matrizes de correlação (Pearson) para cada onda do PISA
               (2015, 2018, 2022).
               
               OBS: 2015 usa granularidade Estadual (N=27).
                    2018/22 usam granularidade Regional (N=5).
================================================================================
"""

import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import os
import sys
from pathlib import Path

# --- CONFIGURAÇÃO DE CAMINHOS ---
CURRENT_FILE = Path(__file__).resolve()
# Ajuste baseado na sua estrutura: src/cog -> src -> project_root
PROJECT_ROOT = CURRENT_FILE.parent.parent.parent
DATA_DIR = PROJECT_ROOT / 'data' / 'processed'
REPORT_DIR = PROJECT_ROOT / 'reports' / 'varcog'
IMG_DIR = REPORT_DIR / 'graficos'  # Ajustado para sua pasta 'graficos' existente
XLSX_DIR = REPORT_DIR / 'xlsx'

for p in [IMG_DIR, XLSX_DIR]:
    p.mkdir(parents=True, exist_ok=True)

# Mapeamento dos Arquivos (Baseado no seu 'ls -R')
FILES = {
    '2015': {'file': 'pisa_2015_states.csv', 'label': 'Estados (N=27)'},
    '2018': {'file': 'pisa_2018_regional_summary.csv', 'label': 'Regiões (N=5)'},
    '2022': {'file': 'pisa_2022_regional_summary.csv', 'label': 'Regiões (N=5)'}
}

def analyze_correlations():
    print("="*60)
    print("      ANÁLISE DE CORRELAÇÃO POR ONDAS (PISA)")
    print("="*60)

    out_excel = XLSX_DIR / 'pisa_correlations_waves.xlsx'
    writer = pd.ExcelWriter(out_excel, engine='openpyxl')
    
    for year, info in FILES.items():
        filename = info['file']
        label_desc = info['label']
        fpath = DATA_DIR / filename
        
        if not fpath.exists():
            print(f"[SKIP] Arquivo não encontrado para {year}: {filename}")
            continue
            
        print(f"\n--- Processando Onda {year} [{label_desc}] ---")
        try:
            df = pd.read_csv(fpath)
            
            # Filtra colunas numéricas de interesse (Notas e Contagens)
            # Ignora colunas categóricas (Region, UF)
            cols = [c for c in df.columns if any(x in c for x in ['Math', 'Read', 'Science', 'Mean', 'Grade', 'Score'])]
            
            # Remove colunas que sejam ID ou códigos se houver
            cols = [c for c in cols if 'IBGE' not in c and 'Code' not in c]

            if len(cols) < 2:
                print(f"   [AVISO] Poucas colunas numéricas para correlação: {cols}")
                continue

            # Matriz de Correlação
            corr = df[cols].corr(method='pearson')
            
            # Print no Console
            print(corr.round(3))
            
            # Salvar Excel
            corr.to_excel(writer, sheet_name=f'{year}_{label_desc.split()[0]}')
            
            # Gerar Heatmap
            plt.figure(figsize=(10, 8))
            sns.heatmap(corr, annot=True, cmap='RdBu_r', vmin=-1, vmax=1, fmt=".2f")
            plt.title(f"Correlação PISA {year} - {label_desc}")
            plt.tight_layout()
            
            img_name = f'heatmap_corr_pisa_{year}.png'
            plt.savefig(IMG_DIR / img_name)
            plt.close()
            print(f"   [PLOT] Salvo: {img_name}")
            
        except Exception as e:
            print(f"   [ERRO] Falha ao processar {year}: {e}")

    writer.close()
    print("\n" + "="*60)
    print(f"[SUCESSO] Relatório salvo em:\n{out_excel}")
    print("="*60)

if __name__ == "__main__":
    analyze_correlations()