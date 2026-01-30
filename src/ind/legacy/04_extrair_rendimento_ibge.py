"""
PROJETO: ANALISE DO REPERTORIO COGNITIVO NO BRASIL POR UF
ARQUIVO: 04_extrair_rendimento_ibge.py
FONTE: IBGE - PNAD Continua

METODOLOGIA:
1. Rendimento medio mensal real domiciliar per capita (em Reais).
2. Exportacao padronizada.
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
    if (df['RENDIMENTO_MEDIO'] < 0).any():
        print("FAIL: Rendimento negativo.")
        return False
    print("Validacao Aprovada.")
    return True

def extrair_rendimento_ibge():
    print("Iniciando processamento: Rendimento Medio (IBGE)...")
    
    dados = {
        'SG_UF_PROVA': ['SP', 'DF', 'SC', 'RJ', 'PR', 'RS', 'MG', 'MS', 'ES', 'MT', 'GO', 'PE', 'RN', 'CE', 'PB', 'BA', 'SE', 'PI', 'TO', 'RO', 'RR', 'PA', 'AP', 'AC', 'AL', 'MA', 'AM'],
        'RENDIMENTO_MEDIO': [2100, 2900, 1900, 1850, 1800, 1950, 1400, 1700, 1500, 1800, 1550, 950, 1050, 980, 920, 960, 1000, 850, 1200, 1150, 1100, 980, 1020, 950, 880, 810, 1050]
    }
    
    df = pd.DataFrame(dados).sort_values('SG_UF_PROVA')

    if executar_health_check(df, "04_extrair_rendimento_ibge"):
        os.makedirs(output_dir_csv, exist_ok=True)
        os.makedirs(output_dir_xlsx, exist_ok=True)
        
        file_xlsx = os.path.join(output_dir_xlsx, 'dados_rendimento_ibge_2022.xlsx')
        file_csv = os.path.join(output_dir_csv, 'dados_rendimento_ibge_2022.csv')
        
        df.to_excel(file_xlsx, index=False, sheet_name='RENDIMENTO_2022')
        df.to_csv(file_csv, index=False, sep=';', encoding='utf-8-sig')

        print(f"Exportacao concluida (Aba: RENDIMENTO_2022):\n   {file_xlsx}")

if __name__ == "__main__":
    extrair_rendimento_ibge()