"""
================================================================================
PROJECT:    Geography of Cognition: Poverty, Wealth, and Inequalities in Brazil
SCRIPT:     src/ind/lib/01_sisvan_clean.py
VERSION:    2.3 (Focado em ficheiros .xls)
DATE:       2026-01-31
--------------------------------------------------------------------------------
PROCESS:
    1. Procura estritamente por ficheiros com extensão .xls.
    2. Lê o conteúdo via motor HTML (comum no SISVAN) ou Excel binário.
    3. Filtra apenas as 27 Unidades Federativas.
    4. Gera CSVs limpos para processamento posterior.
================================================================================
"""
import pandas as pd
import os
import glob
from pathlib import Path

# --- Configuração de Caminhos ---
# Localização: src/ind/lib/01_sisvan_clean.py (4 níveis até a raiz)
BASE_DIR = Path(__file__).resolve().parent.parent.parent.parent
RAW_DIR = BASE_DIR / "data" / "raw" / "indicadores" / "ind01_bio"
PROCESSED_DIR = BASE_DIR / "data" / "processed" / "indicadores"

# Garante que a pasta de destino existe
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

# Lista oficial de siglas para filtragem
VALID_UFS = [
    'AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA', 
    'MT', 'MS', 'MG', 'PA', 'PB', 'PR', 'PE', 'PI', 'RJ', 'RN', 
    'RS', 'RO', 'RR', 'SC', 'SP', 'SE', 'TO'
]

def clean_sisvan_xls(file_path, output_name):
    print(f"Processando: {file_path.name}...")
    df = None

    # TENTATIVA 1: Ler como HTML (Arquivos do SISVAN costumam ser HTML interno)
    try:
        # O motor 'bs4' é o mais robusto para os exports do SISVAN
        tables = pd.read_html(str(file_path), encoding='utf-8', decimal=',', thousands='.')
        
        # Procura a tabela que contém a célula "UF" (exata)
        for t in tables:
            if t.astype(str).apply(lambda x: x.str.strip().eq('UF')).any().any():
                df = t
                break
    except Exception:
        pass

    # TENTATIVA 2: Ler como Excel binário real (se a tentativa HTML falhar)
    if df is None:
        try:
            df = pd.read_excel(file_path)
        except Exception as e:
            print(f" [ERRO] Não foi possível ler o ficheiro {file_path.name}: {e}")
            return

    # --- Extração e Limpeza ---
    df = df.astype(str)
    
    # 1. Localiza a linha do cabeçalho real
    header_idx = -1
    for i, row in df.iterrows():
        row_list = [str(x).strip().upper() for x in row.values]
        if 'UF' in row_list and 'REGIÃO' in row_list:
            header_idx = i
            break
            
    if header_idx == -1:
        print(f" [ERRO] Cabeçalho 'UF' não localizado em {file_path.name}")
        return

    # Define cabeçalho e limpa linhas inúteis
    df.columns = df.iloc[header_idx]
    df = df.iloc[header_idx+1:].reset_index(drop=True)
    
    # 2. Renomeia colunas para evitar duplicatas (SISVAN usa muitos nomes iguais)
    df.columns = [f"COL_{i}_{str(col).strip()}" for i, col in enumerate(df.columns)]
    
    # 3. Identifica a coluna que contém as siglas das UFs
    uf_col = None
    for col in df.columns:
        if df[col].head(30).apply(lambda x: str(x).strip().upper() in VALID_UFS).any():
            uf_col = col
            break
    
    if uf_col is None:
        print(f" [ERRO] Coluna de siglas de estado não encontrada.")
        return

    # 4. Filtra apenas as linhas das UFs e limpa caracteres
    df['UF_SIGLA'] = df[uf_col].apply(lambda x: str(x).strip().upper())
    df_clean = df[df['UF_SIGLA'].isin(VALID_UFS)].copy()
    
    for col in df_clean.columns:
        if col != 'UF_SIGLA':
            df_clean[col] = df_clean[col].str.replace('%', '', regex=False).replace('nan', '', regex=False)
    
    # Salva o resultado
    output_path = PROCESSED_DIR / output_name
    df_clean.to_csv(output_path, index=False, sep=';', encoding='utf-8')
    print(f" [SUCESSO] Guardado em: {output_name}")

def run():
    # Padrões fixos para ficheiros .xls de 2024
    jobs = [
        ("RelatorioEstadoNutricional_altura_x_idade_2024.xls", "sisvan_estatura_2024_clean.csv"),
        ("RelatorioEstadoNutricional_peso_x_altura_2024.xls", "sisvan_peso_2024_clean.csv"),
        ("RelatorioEstadoNutricional_imc_x_idade_2024.xls", "sisvan_imc_2024_clean.csv")
    ]
    
    print("--- INICIANDO LIMPEZA SISVAN (.XLS) ---")
    
    for filename, out_name in jobs:
        target = RAW_DIR / filename
        if target.exists():
            clean_sisvan_xls(target, out_name)
        else:
            # Tenta uma busca via glob caso o nome tenha variações mínimas
            found = glob.glob(str(RAW_DIR / f"*{filename}*"))
            if found:
                clean_sisvan_xls(Path(found[0]), out_name)
            else:
                print(f" [FALHA] Ficheiro não encontrado: {filename}")

if __name__ == "__main__":
    run()