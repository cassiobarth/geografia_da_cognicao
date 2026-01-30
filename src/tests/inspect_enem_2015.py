"""
================================================================================
SCRIPT:     src/tests/inspect_enem_2015_columns.py
DESCRIÇÃO:  Diagnóstico de DNA do arquivo ENEM 2015.
            Lista TODAS as colunas para encontrar nomes divergentes.
================================================================================
"""

import pandas as pd
import zipfile
import os

# --- CONFIGURAÇÃO ---
BASE_PATH = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
FILE_PATH = os.path.join(BASE_PATH, 'data', 'raw', 'enem', 'microdados_enem_2015.zip')

def inspect_header():
    print(f"--- INSPEÇÃO DE ARQUIVO ENEM 2015 ---")
    print(f"Alvo: {FILE_PATH}")
    
    if not os.path.exists(FILE_PATH):
        print("[ERRO] Arquivo não encontrado!")
        return

    try:
        with zipfile.ZipFile(FILE_PATH, 'r') as z:
            # Pega o maior arquivo CSV dentro do ZIP
            csv_files = [f for f in z.namelist() if f.lower().endswith('.csv')]
            target_file = sorted(csv_files, key=lambda x: z.getinfo(x).file_size, reverse=True)[0]
            
            print(f"Arquivo CSV Interno: {target_file}")
            
            with z.open(target_file) as f:
                # Detecta separador na primeira linha
                first_line = f.readline().decode('latin1')
                sep = ';' if first_line.count(';') > first_line.count(',') else ','
                print(f"Separador detectado: '{sep}'")
                
                # Volta para o início e lê apenas o cabeçalho
                f.seek(0)
                df_header = pd.read_csv(f, sep=sep, encoding='latin1', nrows=0)
                cols = df_header.columns.tolist()
                
                print(f"\nTotal de Colunas: {len(cols)}")
                
                # 1. Busca por Colunas de Escola (Para o Proxy)
                print("\n--- CANDIDATAS A CÓDIGO DE ESCOLA ---")
                school_candidates = [c for c in cols if any(x in c.upper() for x in ['ESCOLA', 'ENTIDADE', 'COD', 'ID_'])]
                if school_candidates:
                    for c in school_candidates: print(f" -> {c}")
                else:
                    print(" [ALERTA] Nenhuma coluna óbvia de escola encontrada!")

                # 2. Busca por Status (Para o Strict)
                print("\n--- CANDIDATAS A STATUS DE CONCLUSÃO ---")
                status_candidates = [c for c in cols if any(x in c.upper() for x in ['SITUACAO', 'CONCLUSAO', 'STATUS', 'TP_ST'])]
                for c in status_candidates: print(f" -> {c}")

                # 3. Lista Completa (Ordenada)
                print("\n--- LISTA COMPLETA DE COLUNAS (A-Z) ---")
                for c in sorted(cols):
                    print(f"[{c}]", end=", ")
                print("\n")

    except Exception as e:
        print(f"[ERRO CRÍTICO] {e}")

if __name__ == "__main__":
    inspect_header()