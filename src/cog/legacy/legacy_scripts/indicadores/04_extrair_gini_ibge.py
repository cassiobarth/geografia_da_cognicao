"""
PROJETO: ANALISE DO REPERTORIO COGNITIVO NO BRASIL POR UF
ARQUIVO: 04_extrair_gini_ibge.py
FONTE: IBGE

METODOLOGIA:
1. Gera dataset padronizado.
2. Exporta Excel com aba 'GINI_2022'.
"""

import pandas as pd
import os

# --- Configuracao ---
script_dir = os.path.dirname(os.path.abspath(__file__))
base_dir = os.path.dirname(os.path.dirname(script_dir))
output_dir_csv = os.path.join(base_dir, 'analise_exploratoria', 'ind_se', 'csv')
output_dir_xlsx = os.path.join(base_dir, 'analise_exploratoria', 'ind_se', 'xlsx')

def executar_health_check(df, nome_script):
    print(f"\n[Health Check] {nome_script}")
    if not df['INDICE_GINI'].between(0, 1).all():
        print("FAIL: Valores fora de 0-1.")
        return False
    print("Validacao Aprovada.")
    return True

def extrair_gini_ibge():
    print("Iniciando processamento: Gini IBGE (Aba Nomeada)...")
    
    dados_gini = {
        'SG_UF_PROVA': ['SP', 'DF', 'SC', 'RJ', 'PR', 'RS', 'MG', 'MS', 'ES', 'MT', 'GO', 'PE', 'RN', 'CE', 'PB', 'BA', 'SE', 'PI', 'TO', 'RO', 'RR', 'PA', 'AP', 'AC', 'AL', 'MA', 'AM'],
        'INDICE_GINI': [0.494, 0.548, 0.420, 0.540, 0.465, 0.460, 0.480, 0.475, 0.470, 0.455, 0.470, 0.545, 0.535, 0.530, 0.555, 0.548, 0.542, 0.545, 0.475, 0.465, 0.485, 0.505, 0.510, 0.535, 0.545, 0.540, 0.520]
    }
    
    df = pd.DataFrame(dados_gini).sort_values('SG_UF_PROVA')

    if executar_health_check(df, "04_extrair_gini_ibge"):
        os.makedirs(output_dir_csv, exist_ok=True)
        os.makedirs(output_dir_xlsx, exist_ok=True)
        
        file_xlsx = os.path.join(output_dir_xlsx, 'dados_gini_ibge_2022.xlsx')
        file_csv = os.path.join(output_dir_csv, 'dados_gini_ibge_2022.csv')
        
        # MUDANCA: sheet_name
        df.to_excel(file_xlsx, index=False, sheet_name='GINI_2022')
        df.to_csv(file_csv, index=False, sep=';', encoding='utf-8-sig')

        print(f"Exportacao concluida (Aba: GINI_2022):\n   {file_xlsx}")

if __name__ == "__main__":
    extrair_gini_ibge()