"""
================================================================================
PROJECT:     GEOGRAPHY OF COGNITION: POVERTY, WEALTH, AND INEQUALITIES IN BRAZIL
SCRIPT:      src/cog/exp_01_analise_kendall_2015.py
VERSION:     3.4 (Fix: Compatibilidade com arquivos de sufixo ETL v7.7)
DATE:        2026-01-26
--------------------------------------------------------------------------------
DESCRIPTION:
    Executes Kendall's W analysis (2015).
    
    UPDATE (v3.4):
    - File Discovery: Prioritizes files matching the selected mode suffix 
      (e.g., '_ponderada') to ensure correct dataset loading.
================================================================================
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pandas.plotting import parallel_coordinates
import os
import sys
import time
from pathlib import Path

# --- CONFIGURAÇÃO ---
TARGET_YEAR = "2015"
BASE_DIR = Path(__file__).resolve().parent.parent.parent 
DIRS = {
    "csv":      BASE_DIR / "data" / "processed",
    "xlsx":     BASE_DIR / "reports" / "varcog" / "xlsx",
    "graficos": BASE_DIR / "reports" / "varcog" / "graficos"
}
for p in DIRS.values(): p.mkdir(parents=True, exist_ok=True)

# --- UTILS ---
try:
    import msvcrt
    def input_timeout(prompt, timeout=10, default=''):
        print(f"{prompt} [Automático em {timeout}s]: ", end='', flush=True)
        start = time.time()
        chars = []
        while True:
            if msvcrt.kbhit():
                c = msvcrt.getwche()
                if c == '\r': print(); return "".join(chars) or default
                chars.append(c)
            if time.time() - start > timeout: print(f"\n[TIMEOUT] Usando: {default}"); return default
except:
    def input_timeout(prompt, timeout=10, default=''):
        return input(f"{prompt} [Default: {default}]: ") or default

# 1. RADAR (AJUSTADO v3.4)
def scan_files(target_suffix):
    print(f"\n[RADAR] Localizando arquivos {TARGET_YEAR} (Prioridade: '{target_suffix}')...")
    candidates = {"pisa": [], "saeb": [], "enem": []}
    ignore = ["venv", ".git", "__pycache__"]
    
    for root, dirs, files in os.walk(BASE_DIR):
        if any(x in root for x in ignore): continue
        for file in files:
            fname = file.lower()
            if (fname.endswith(".xlsx") or fname.endswith(".csv")) and TARGET_YEAR in fname and not fname.startswith("~$"):
                path = os.path.join(root, file)
                if "pisa" in fname: candidates["pisa"].append(path)
                elif "saeb" in fname: candidates["saeb"].append(path)
                elif "enem" in fname: candidates["enem"].append(path)
    
    # Função de escolha inteligente:
    # 1. Prioriza arquivo que tenha o sufixo desejado (ex: _ponderada)
    # 2. Prioriza pasta 'processed'
    # 3. Prioriza .xlsx
    def pick(lst):
        if not lst: return None
        lst.sort(key=lambda x: (
            0 if target_suffix in os.path.basename(x).lower() else 1, 
            0 if "processed" in x else 1, 
            0 if x.endswith("xlsx") else 1
        ))
        return lst[0]

    final = {k: pick(v) for k, v in candidates.items()}
    for k, v in final.items(): 
        name = os.path.basename(v) if v else '---'
        print(f"[{'OK' if v else 'MISSING'}] {k.upper()}: {name}")
    return final

# 2. SMART LOAD (FLEXÍVEL)
def smart_load(files_dict, use_weighted=True):
    if not all(files_dict.values()): return None
    try:
        def load(p): return pd.read_excel(p) if p.endswith("xlsx") else pd.read_csv(p)
        df_p, df_s, df_e = load(files_dict["pisa"]), load(files_dict["saeb"]), load(files_dict["enem"])

        def get_col(df, context):
            cols = df.columns
            # Listas de Candidatos
            weighted_tags = ["Ponderada", "Weighted", "w_mean", "W_FSTUWT"]
            simple_tags = ["Cognitive_Global_Mean", "Média_Geral", "Mean_General", "SAEB_General", "ENEM_General"]
            
            # Lógica de Seleção
            if use_weighted:
                for c in cols:
                    if any(t in c for t in weighted_tags):
                        print(f"   [{context}] Ponderada encontrada: {c}")
                        return c
                print(f"   [{context}] Aviso: Ponderada não achada. Usando melhor disponível.")
            
            # Fallback ou Modo Simples
            for c in cols:
                if not use_weighted and any(t in c for t in weighted_tags): continue
                if any(t in c for t in simple_tags):
                    print(f"   [{context}] Usando Padrão/Simples: {c}")
                    return c
            
            # Último recurso
            for c in cols:
                if "media" in c.lower() or "mean" in c.lower(): return c
            return None

        print(f"\n>> Selecionando colunas (Modo: {'PONDERADA' if use_weighted else 'SIMPLES'})...")
        cp = get_col(df_p, "PISA")
        cs = get_col(df_s, "SAEB")
        ce = get_col(df_e, "ENEM")
        
        if not all([cp, cs, ce]): return None

        d1 = df_p[['UF', cp]].rename(columns={cp: 'PISA'})
        d2 = df_s[['UF', cs]].rename(columns={cs: 'SAEB'})
        d3 = df_e[['UF', ce]].rename(columns={ce: 'ENEM'})
        return d1.merge(d2, on='UF').merge(d3, on='UF')
    except Exception as e:
        print(f"[ERRO] {e}"); return None

# 3. ANÁLISE
def run_analysis(df):
    df['R_PISA'] = df['PISA'].rank(ascending=False)
    df['R_SAEB'] = df['SAEB'].rank(ascending=False)
    df['R_ENEM'] = df['ENEM'].rank(ascending=False)
    m = 3; n = len(df)
    df['S'] = df['R_PISA'] + df['R_SAEB'] + df['R_ENEM']
    S_var = ((df['S'] - (m*(n+1)/2))**2).sum()
    W = (12 * S_var) / (m**2 * (n**3 - n))
    return df, W

# 4. PLOTS
def plot_results(df, W, suffix):
    print(">> Gerando gráficos...")
    sns.set(style="whitegrid")
    
    tipo = "Ponderada" if "ponderada" in suffix else "Simples"

    # Plot 1: Fluxo
    plt.figure(figsize=(12, 6))
    plot_df = df.copy().sort_values('S')
    q1, q2 = plot_df['S'].quantile([0.33, 0.66])
    plot_df['Grupo'] = np.select(
        [(plot_df['S'] <= q1), (plot_df['S'] > q1) & (plot_df['S'] <= q2)], 
        ['Alta', 'Média'], default='Baixa'
    )
    parallel_coordinates(plot_df[['R_PISA', 'R_SAEB', 'R_ENEM', 'Grupo']], 'Grupo', 
                         color=['#2ca02c', '#1f77b4', '#d62728'], alpha=0.8)
    plt.gca().invert_yaxis()
    plt.title(f'Consistência Hierárquica ({tipo}) - W={W:.4f}', fontsize=14)
    plt.tight_layout()
    plt.savefig(DIRS["graficos"] / f"kendall_2015_fluxo{suffix}.png", dpi=300)
    plt.close()

    # Plot 2: Dispersão
    plt.figure(figsize=(10, 8))
    sizes = 100 + (df['PISA'] - df['PISA'].min()) / (df['PISA'].max() - df['PISA'].min()) * 800
    plt.scatter(df['SAEB'], df['ENEM'], s=sizes, c=df['S'], cmap='RdYlGn_r', alpha=0.75, edgecolors='gray')
    for _, row in df.iterrows():
        plt.text(row['SAEB'], row['ENEM'], row['UF'], fontsize=9, ha='center', va='center', fontweight='bold')
    plt.title(f'Triangulação ({tipo}): SAEB vs ENEM (Tamanho = PISA)', fontsize=14)
    plt.xlabel('Nota SAEB')
    plt.ylabel('Nota ENEM')
    plt.tight_layout()
    plt.savefig(DIRS["graficos"] / f"kendall_2015_dispersao{suffix}.png", dpi=300)
    plt.close()

# MAIN
if __name__ == "__main__":
    os.system('cls' if os.name == 'nt' else 'clear')
    print("=== ANÁLISE KENDALL 2015 (v3.4) ===")
    
    # MENU DE ESCOLHA
    print("\nEscolha o método de cálculo:")
    print("1 - Média Simples (Aritmética)")
    print("2 - Média Ponderada (Recomendado OCDE)")
    choice = input_timeout(">> Opção", default="2")
    
    is_weighted = (choice == "2")
    
    # Define o sufixo para buscar arquivos E para salvar
    target_suffix = "_ponderada" if is_weighted else "_simples"
    
    # Passa o sufixo para o scan
    files = scan_files(target_suffix)
    
    df_merged = smart_load(files, use_weighted=is_weighted)
    
    if df_merged is not None:
        df_final, W = run_analysis(df_merged)
        
        print(f"\n{'='*40}")
        print(f"RESULTADO FINAL ({'PONDERADA' if is_weighted else 'SIMPLES'}): W = {W:.4f}")
        print(f"{'='*40}\n")
        
        f_csv = DIRS["csv"] / f"kendall_final_2015{target_suffix}.csv"
        f_xlsx = DIRS["xlsx"] / f"kendall_final_2015{target_suffix}.xlsx"
        
        df_final.to_csv(f_csv, index=False)
        df_final.to_excel(f_xlsx, index=False)
        
        plot_results(df_final, W, target_suffix)
        print(f"[SUCESSO] Arquivos gerados com sufixo '{target_suffix}'")