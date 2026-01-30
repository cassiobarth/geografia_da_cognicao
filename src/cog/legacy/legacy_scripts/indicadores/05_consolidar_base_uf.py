"""
PROJETO: ANALISE DO REPERTORIO COGNITIVO NO BRASIL POR UF
ARQUIVO: 05_consolidar_base_uf.py
OBJETIVO: Unificacao de todos os 11 indicadores em um unico dataset.
"""

import pandas as pd
import os
from functools import reduce

# --- Configuracao ---
script_dir = os.path.dirname(os.path.abspath(__file__))
base_dir = os.path.dirname(os.path.dirname(script_dir))
input_dir = os.path.join(base_dir, 'analise_exploratoria', 'ind_se', 'xlsx')
output_dir = os.path.join(base_dir, 'data', 'processed')

def executar_health_check_final(df):
    print("\n[Health Check] Base Consolidada")
    erros = []

    if len(df) != 27:
        erros.append(f"Erro Critico: Base final com {len(df)} UFs. Esperado: 27.")

    if df.isnull().any().any():
        print("   Aviso: Existem valores nulos na base consolidada.")
    
    print(f"   Dimensoes finais: {df.shape[0]} linhas x {df.shape[1]} colunas")

    if not erros:
        print("Validacao Aprovada: Base Mestra Integra.")
        return True
    else:
        for erro in erros: print(f"FAIL: {erro}")
        return False

def consolidar_indicadores():
    print("Iniciando consolidacao da Base Mestra (11 Indicadores)...")

    # Lista ajustada para SUA metodologia original (SIOPE, PIB CAPITA, PNAD)
    arquivos_esperados = [
        'dados_saneamento_snis_2022.xlsx',
        'dados_qualificacao_docente_2022.xlsx',
        'dados_educacao_ibge_2022.xlsx',
        'dados_fluxo_inep_2022.xlsx',
        'dados_gini_ibge_2022.xlsx',
        'dados_idh_atlas_2021.xlsx',
        'dados_ingles_ef_2022.xlsx',
        'dados_internet_pnad_2022.xlsx',
        'dados_investimento_siope_2022.xlsx', # <--- Nome corrigido (SIOPE)
        '04_extrair_pib_capita_2022_2023.py',         # <--- Nome corrigido (PIB CAPITA)
        'dados_rendimento_ibge_2022.xlsx'
    ]

    dfs = []
    
    for arq in arquivos_esperados:
        caminho = os.path.join(input_dir, arq)
        if os.path.exists(caminho):
            print(f"   Lendo: {arq}")
            try:
                df_temp = pd.read_excel(caminho)
                dfs.append(df_temp)
            except Exception as e:
                print(f"ERRO ao ler {arq}: {e}")
        else:
            print(f"AVISO: Arquivo nao encontrado (pulei): {arq}")

    if not dfs:
        print("Nenhum dado para consolidar.")
        return

    print("   Unificando datasets...")
    df_consolidado = reduce(lambda left, right: pd.merge(left, right, on='SG_UF_PROVA', how='outer'), dfs)
    df_consolidado = df_consolidado.sort_values('SG_UF_PROVA').reset_index(drop=True)

    if executar_health_check_final(df_consolidado):
        os.makedirs(output_dir, exist_ok=True)
        
        path_xlsx = os.path.join(output_dir, 'base_mestra_indicadores_completa.xlsx')
        path_csv = os.path.join(output_dir, 'base_mestra_indicadores_completa.csv')
        
        df_consolidado.to_excel(path_xlsx, index=False, sheet_name='BASE_COMPLETA')
        df_consolidado.to_csv(path_csv, index=False, sep=';', encoding='utf-8-sig')
        
        print(f"\nCONSOLIDACAO CONCLUIDA!")
        print(f"   Arquivo Final: {path_xlsx}")

if __name__ == "__main__":
    consolidar_indicadores()