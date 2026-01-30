import pyreadstat
import pandas as pd
import os

def inspecionar_colunas():
    # Ajuste de caminho (igual ao anterior)
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    input_file = os.path.join(base_dir, 'data', 'raw', 'pisa_2022', 'CY08MSP_STU_QQQ.sav')

    print(f"Lendo metadados de: {input_file}")
    
    if not os.path.exists(input_file):
        print("Arquivo não encontrado!")
        return

    # 1. Ler apenas metadados para listar colunas suspeitas
    _, meta = pyreadstat.read_sav(input_file, metadataonly=True)
    
    suspeitas = [c for c in meta.column_names if any(x in c.upper() for x in ['SUB', 'REG', 'STATE', 'UF', 'PROV'])]
    print(f"\nColunas candidatas a ter a UF: {suspeitas}")

    # 2. Ler as primeiras linhas dessas colunas para o Brasil
    print("\nVerificando conteúdo das colunas para o Brasil (BRA)...")
    cols_to_load = ['CNT', 'STRATUM'] + suspeitas
    
    # Prevenção de erro se alguma coluna não existir
    cols_to_load = [c for c in cols_to_load if c in meta.column_names]

    df, _ = pyreadstat.read_sav(input_file, usecols=cols_to_load, disable_datetime_conversion=True)
    df_bra = df[df['CNT'] == 'BRA'].head(20) # Pega 20 alunos

    print("\n--- AMOSTRA DE DADOS (BRA) ---")
    print(df_bra.to_string())

    # 3. Tentar ver valores únicos de SUBNATIO (se existir)
    if 'SUBNATIO' in df_bra.columns:
        print("\n--- VALORES ÚNICOS EM SUBNATIO ---")
        print(df[df['CNT'] == 'BRA']['SUBNATIO'].unique())

if __name__ == "__main__":
    inspecionar_colunas()