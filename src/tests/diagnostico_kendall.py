"""
================================================================================
SCRIPT:      src/cog/diagnostico_kendall.py
DESCRICAO:   Varredura profunda de diretórios para localizar inputs perdidos
             e executar a análise de Kendall (2015) automaticamente.
AUTOR:       Adaptado para Dr. José Aparecido
================================================================================
"""
import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pandas.plotting import parallel_coordinates

# --- CONFIGURAÇÃO ---
TARGET_YEAR = "2015"
KEYWORDS = {
    "pisa": ["pisa", "states", "uf"], # Palavras-chave para identificar arquivo PISA
    "saeb": ["saeb", "9ef"],          # Palavras-chave para SAEB
    "enem": ["enem", "3em", "total"]  # Palavras-chave para ENEM
}

# Colunas de Nota (Tentativas em ordem de prioridade)
SCORE_COLS = [
    "Média_Geral", "Mean_General", "Cognitive_Global_Mean", "SAEB_General", 
    "ENEM_General", "Media_Geral", "Média", "Mean"
]

def scan_files(start_dir="."):
    """Varre recursivamente buscando arquivos candidatos."""
    print(f"\n[RADAR] Iniciando varredura a partir de: {os.path.abspath(start_dir)}")
    candidates = {"pisa": [], "saeb": [], "enem": []}
    
    # Sobe 3 níveis para garantir que pega desde a raiz do projeto
    root_dir = os.path.abspath(os.path.join(start_dir, "../../.."))
    print(f"[RADAR] Raiz estimada do projeto: {root_dir}")
    
    found_any = False
    
    for root, dirs, files in os.walk(root_dir):
        # Ignora pastas de ambiente virtual ou git
        if "venv" in root or ".git" in root or "__pycache__" in root: continue
        
        for file in files:
            fname = file.lower()
            if (fname.endswith(".xlsx") or fname.endswith(".csv")) and TARGET_YEAR in fname:
                full_path = os.path.join(root, file)
                
                # Classifica
                if "pisa" in fname:
                    candidates["pisa"].append(full_path)
                    print(f"  -> Achei PISA: {file}")
                    found_any = True
                elif "saeb" in fname:
                    candidates["saeb"].append(full_path)
                    print(f"  -> Achei SAEB: {file}")
                    found_any = True
                elif "enem" in fname:
                    candidates["enem"].append(full_path)
                    print(f"  -> Achei ENEM: {file}")
                    found_any = True
                    
    if not found_any:
        print("[RADAR] Nenhum arquivo com '2015' encontrado. Verifique se rodou as extrações.")
    return candidates

def load_best_candidate(candidates, type_key):
    """Escolhe o melhor arquivo (prioriza .xlsx e nomes com 'table' ou 'processed')"""
    opts = candidates[type_key]
    if not opts: return None
    
    # Prioridade: XLSX > CSV
    # Prioridade: Pasta 'processed' ou 'reports' > Pasta 'raw'
    opts.sort(key=lambda x: (0 if "processed" in x else 1, 0 if x.endswith("xlsx") else 1))
    
    chosen = opts[0]
    print(f"\n[CARREGANDO] {type_key.upper()}: {os.path.basename(chosen)}")
    
    try:
        if chosen.endswith(".csv"): df = pd.read_csv(chosen)
        else: df = pd.read_excel(chosen)
        return df
    except Exception as e:
        print(f"[ERRO] Falha ao abrir {chosen}: {e}")
        return None

def find_score_column(df, type_key):
    """Tenta adivinhar a coluna de nota."""
    cols = df.columns.tolist()
    # 1. Tenta match exato na lista SCORE_COLS
    for candidate in SCORE_COLS:
        if candidate in cols: return candidate
    
    # 2. Tenta substring (ex: 'Média_Geral_Ponderada')
    for col in cols:
        if "media" in col.lower() or "mean" in col.lower() or "nota" in col.lower():
            # Evita colunas de erro ou desvio
            if "erro" not in col.lower() and "dev" not in col.lower():
                return col
    return None

def run_kendall_analysis(df_pisa, df_saeb, df_enem):
    print("\n[ANÁLISE] Iniciando Cálculo de Kendall...")
    
    # Identifica colunas
    col_p = find_score_column(df_pisa, "pisa")
    col_s = find_score_column(df_saeb, "saeb")
    col_e = find_score_column(df_enem, "enem")
    
    print(f"  - Coluna PISA: {col_p}")
    print(f"  - Coluna SAEB: {col_s}")
    print(f"  - Coluna ENEM: {col_e}")
    
    if not all([col_p, col_s, col_e]):
        print("[ERRO] Não foi possível identificar as colunas de nota automaticamente.")
        return

    # Padronização
    d1 = df_pisa[['UF', col_p]].rename(columns={col_p: 'PISA'})
    d2 = df_saeb[['UF', col_s]].rename(columns={col_s: 'SAEB'})
    d3 = df_enem[['UF', col_e]].rename(columns={col_e: 'ENEM'})
    
    # Merge
    merged = d1.merge(d2, on='UF').merge(d3, on='UF')
    print(f"  - Estados combinados (Intersection): {len(merged)}")
    
    if len(merged) < 5:
        print("[ERRO] Poucos estados após o merge. Verifique siglas das UFs.")
        return

    # Cálculo
    merged['R_PISA'] = merged['PISA'].rank(ascending=False)
    merged['R_SAEB'] = merged['SAEB'].rank(ascending=False)
    merged['R_ENEM'] = merged['ENEM'].rank(ascending=False)
    
    m = 3; n = len(merged)
    merged['S'] = merged['R_PISA'] + merged['R_SAEB'] + merged['R_ENEM']
    S_var = ((merged['S'] - (m*(n+1)/2))**2).sum()
    W = (12 * S_var) / (m**2 * (n**3 - n))
    
    print(f"\n{'='*40}")
    print(f"RESULTADO FINAL (KENDALL W): {W:.5f}")
    print(f"{'='*40}")
    
    # Output Simples
    out_path = "kendall_final_2015.csv"
    merged.to_csv(out_path, index=False)
    print(f"[OK] Tabela salva em {os.path.abspath(out_path)}")

# --- MAIN FLOW ---
if __name__ == "__main__":
    candidates = scan_files()
    
    if candidates["pisa"] and candidates["saeb"] and candidates["enem"]:
        print("\n[SUCESSO] Arquivos necessários localizados.")
        df_p = load_best_candidate(candidates, "pisa")
        df_s = load_best_candidate(candidates, "saeb")
        df_e = load_best_candidate(candidates, "enem")
        
        if all([df_p is not None, df_s is not None, df_e is not None]):
            run_kendall_analysis(df_p, df_s, df_e)
    else:
        print("\n[FALHA] Faltam arquivos para completar a tríade.")
        print(f"PISA encontrado? {'SIM' if candidates['pisa'] else 'NÃO'}")
        print(f"SAEB encontrado? {'SIM' if candidates['saeb'] else 'NÃO'}")
        print(f"ENEM encontrado? {'SIM' if candidates['enem'] else 'NÃO'}")