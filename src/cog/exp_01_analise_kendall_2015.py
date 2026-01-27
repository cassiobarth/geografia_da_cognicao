"""
================================================================================
PROJECT:     GEOGRAPHY OF COGNITION: POVERTY, WEALTH, AND INEQUALITIES IN BRAZIL
SCRIPT:      src/cog/exp_01_analise_kendall_2015.py
VERSION:     3.1 (Official Project Name Update)
DATE:        2026-01-26
--------------------------------------------------------------------------------
DESCRIPTION:
    Executes the full Kendall's W analysis for the 2015 cycle.
    
    FEATURES:
    1. Auto-Radar: Scans project directories to find PISA/SAEB/ENEM files.
    2. Smart-Merge: Automatically detects score columns (Mean/Média/Nota).
    3. Output: Generates CSV, XLSX reports and High-Res Plots.

USAGE:
    python src/cog/exp_01_analise_kendall_2015.py
================================================================================
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pandas.plotting import parallel_coordinates
import os
import sys
from pathlib import Path

# --- CONFIGURAÇÃO ---
TARGET_YEAR = "2015"

# Diretórios de Saída (Output)
BASE_DIR = Path(__file__).resolve().parent.parent.parent # Raiz do Projeto
DIRS = {
    "csv":      BASE_DIR / "data" / "processed",
    "xlsx":     BASE_DIR / "reports" / "varcog" / "xlsx",
    "graficos": BASE_DIR / "reports" / "varcog" / "graficos"
}

# Garante que pastas existem
for p in DIRS.values():
    p.mkdir(parents=True, exist_ok=True)

# ==============================================================================
# 1. MOTOR DE BUSCA (RADAR)
# ==============================================================================
def scan_files():
    """Varre o projeto em busca dos arquivos de input (PISA/SAEB/ENEM)."""
    print(f"\n[RADAR] Iniciando varredura por arquivos de {TARGET_YEAR}...")
    candidates = {"pisa": [], "saeb": [], "enem": []}
    
    # Ignora pastas de sistema/ambientes
    ignore = ["venv", ".git", "__pycache__", "lib", "site-packages"]

    for root, dirs, files in os.walk(BASE_DIR):
        if any(x in root for x in ignore): continue
        
        for file in files:
            fname = file.lower()
            # Filtro básico: deve ser xlsx/csv, ter o ano alvo e não ser temporário
            if (fname.endswith(".xlsx") or fname.endswith(".csv")) and TARGET_YEAR in fname and not fname.startswith("~$"):
                full_path = os.path.join(root, file)
                
                if "pisa" in fname: candidates["pisa"].append(full_path)
                elif "saeb" in fname: candidates["saeb"].append(full_path)
                elif "enem" in fname: candidates["enem"].append(full_path)
    
    # Seleciona o melhor candidato (Prioriza XLSX processado)
    def pick_best(file_list):
        if not file_list: return None
        # Ordena: Processed > Raw, XLSX > CSV
        file_list.sort(key=lambda x: (0 if "processed" in x else 1, 0 if x.endswith("xlsx") else 1))
        return file_list[0]

    final_files = {
        "pisa": pick_best(candidates["pisa"]),
        "saeb": pick_best(candidates["saeb"]),
        "enem": pick_best(candidates["enem"])
    }

    # Relatório do Radar
    print("-" * 50)
    for k, v in final_files.items():
        status = "OK" if v else "MISSING"
        fname = os.path.basename(v) if v else "---"
        print(f"[{status}] {k.upper()}: {fname}")
    print("-" * 50)
    
    return final_files

# ==============================================================================
# 2. CARREGAMENTO INTELIGENTE
# ==============================================================================
def smart_load(files_dict):
    if not all(files_dict.values()):
        print("[ERRO] Faltam arquivos para a análise. Verifique o Radar acima.")
        return None

    try:
        # Carrega
        def load(path): return pd.read_excel(path) if path.endswith("xlsx") else pd.read_csv(path)
        df_p = load(files_dict["pisa"])
        df_s = load(files_dict["saeb"])
        df_e = load(files_dict["enem"])

        # Detecta Colunas de Nota
        def get_col(df):
            # Prioridade para nomes conhecidos
            targets = ["Média_Geral", "Mean_General", "Cognitive_Global_Mean", "SAEB_General", "ENEM_General", "Média", "Mean"]
            for t in targets:
                if t in df.columns: return t
            # Fallback genérico
            for c in df.columns:
                if "media" in c.lower() or "mean" in c.lower(): return c
            return None

        cp, cs, ce = get_col(df_p), get_col(df_s), get_col(df_e)
        
        if not all([cp, cs, ce]):
            print(f"[ERRO] Colunas não identificadas: PISA={cp}, SAEB={cs}, ENEM={ce}")
            return None
        
        print(f"[INFO] Colunas Usadas: PISA[{cp}] | SAEB[{cs}] | ENEM[{ce}]")

        # Padroniza e Merge
        d1 = df_p[['UF', cp]].rename(columns={cp: 'PISA'})
        d2 = df_s[['UF', cs]].rename(columns={cs: 'SAEB'})
        d3 = df_e[['UF', ce]].rename(columns={ce: 'ENEM'})

        merged = d1.merge(d2, on='UF').merge(d3, on='UF')
        return merged

    except Exception as e:
        print(f"[ERRO CRÍTICO] {e}")
        return None

# ==============================================================================
# 3. ANÁLISE ESTATÍSTICA
# ==============================================================================
def run_analysis(df):
    # Rankings (1 = Melhor nota)
    df['R_PISA'] = df['PISA'].rank(ascending=False)
    df['R_SAEB'] = df['SAEB'].rank(ascending=False)
    df['R_ENEM'] = df['ENEM'].rank(ascending=False)

    # Kendall's W
    m = 3; n = len(df)
    df['S'] = df['R_PISA'] + df['R_SAEB'] + df['R_ENEM']
    S_var = ((df['S'] - (m*(n+1)/2))**2).sum()
    W = (12 * S_var) / (m**2 * (n**3 - n))
    
    return df, W

# ==============================================================================
# 4. VISUALIZAÇÃO
# ==============================================================================
def plot_results(df, W):
    print(">> Gerando gráficos...")
    sns.set(style="whitegrid")

    # --- A. Parallel Coordinates (Fluxo) ---
    plt.figure(figsize=(12, 6))
    plot_df = df.copy()
    
    # Grupos por tercil
    q1, q2 = plot_df['S'].quantile([0.33, 0.66])
    conditions = [
        (plot_df['S'] <= q1),
        (plot_df['S'] > q1) & (plot_df['S'] <= q2),
        (plot_df['S'] > q2)
    ]
    plot_df['Grupo'] = np.select(conditions, ['Alta Performance', 'Média', 'Baixa'], default='Média')
    plot_df = plot_df.sort_values('S') # Ordena para plotar bonito

    parallel_coordinates(plot_df[['R_PISA', 'R_SAEB', 'R_ENEM', 'Grupo']], 'Grupo', 
                         color=['#2ca02c', '#1f77b4', '#d62728'], alpha=0.8, linewidth=2.5)
    
    plt.gca().invert_yaxis()
    plt.title(f'Consistência Hierárquica dos Estados (2015)\nKendall W = {W:.4f}', fontsize=14)
    plt.ylabel('Ranking (Posição)')
    plt.legend(bbox_to_anchor=(1.02, 1), loc='upper left')
    plt.tight_layout()
    plt.savefig(DIRS["graficos"] / "kendall_2015_fluxo.png", dpi=300)
    plt.close()

    # --- B. Dispersão (Bolhas) ---
    plt.figure(figsize=(10, 8))
    # Tamanho da bolha = Nota PISA
    sizes = 100 + (df['PISA'] - df['PISA'].min()) / (df['PISA'].max() - df['PISA'].min()) * 800
    
    plt.scatter(df['SAEB'], df['ENEM'], s=sizes, c=df['S'], cmap='RdYlGn_r', alpha=0.75, edgecolors='gray')
    
    for _, row in df.iterrows():
        plt.text(row['SAEB'], row['ENEM'], row['UF'], fontsize=9, ha='center', va='center', fontweight='bold')
        
    plt.title('Triangulação: SAEB vs ENEM (Tamanho = PISA)', fontsize=14)
    plt.xlabel('Nota SAEB (9º EF)')
    plt.ylabel('Nota ENEM (3º EM)')
    plt.colorbar(label='Soma dos Rankings (Menor = Melhor)')
    plt.tight_layout()
    plt.savefig(DIRS["graficos"] / "kendall_2015_dispersao.png", dpi=300)
    plt.close()

# ==============================================================================
# MAIN
# ==============================================================================
if __name__ == "__main__":
    files = scan_files()
    df_merged = smart_load(files)
    
    if df_merged is not None:
        df_final, W = run_analysis(df_merged)
        
        print(f"\n>>> KENDALL W (2015) = {W:.4f} <<<\n")
        
        # Salva Tabelas
        df_final.to_csv(DIRS["csv"] / "kendall_final_2015.csv", index=False)
        df_final.to_excel(DIRS["xlsx"] / "kendall_final_2015.xlsx", index=False)
        print(f"[OK] Tabelas salvas em: {DIRS['xlsx']}")
        
        # Salva Gráficos
        plot_results(df_final, W)
        print(f"[OK] Gráficos salvos em: {DIRS['graficos']}")