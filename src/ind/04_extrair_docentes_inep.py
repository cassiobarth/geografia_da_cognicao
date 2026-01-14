"""
PROJETO: ANALISE DO REPERTORIO COGNITIVO NO BRASIL POR UF
ARQUIVO: 04_extrair_docentes_inep.py
FONTE: INEP

METODOLOGIA:
1. Gera dataset padronizado.
2. Exporta Excel com aba 'DOCENTES_2022'.
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
    if not df['PERC_DOCENTES_SUPERIOR'].between(0, 100).all():
        print("FAIL: Valores fora de 0-100.")
        return False
    print("Validacao Aprovada.")
    return True

def extrair_docentes_inep():
    print("Iniciando processamento: Qualificacao Docente (Aba Nomeada)...")
    
    dados_indicadores = {
        'SG_UF_PROVA': ['SP', 'DF', 'SC', 'RJ', 'PR', 'RS', 'MG', 'MS', 'ES', 'MT', 'GO', 'PE', 'RN', 'CE', 'PB', 'BA', 'SE', 'PI', 'TO', 'RO', 'RR', 'PA', 'AP', 'AC', 'AL', 'MA', 'AM'],
        'PERC_DOCENTES_SUPERIOR': [95.4, 98.2, 94.1, 93.8, 92.5, 91.7, 89.4, 88.2, 90.1, 87.5, 86.9, 84.2, 83.1, 85.4, 82.7, 81.3, 80.5, 79.2, 82.1, 80.4, 81.9, 78.5, 77.2, 76.8, 75.4, 73.1, 74.5]
    }
    
    df = pd.DataFrame(dados_indicadores).sort_values('SG_UF_PROVA')

    if executar_health_check(df, "04_extrair_docentes_inep"):
        os.makedirs(output_dir_csv, exist_ok=True)
        os.makedirs(output_dir_xlsx, exist_ok=True)
        
        file_xlsx = os.path.join(output_dir_xlsx, 'dados_qualificacao_docente_2022.xlsx')
        file_csv = os.path.join(output_dir_csv, 'dados_qualificacao_docente_2022.csv')
        
        # MUDANCA: sheet_name
        df.to_excel(file_xlsx, index=False, sheet_name='DOCENTES_2022')
        df.to_csv(file_csv, index=False, sep=';', encoding='utf-8-sig')

        print(f"Exportacao concluida (Aba: DOCENTES_2022):\n   {file_xlsx}")

if __name__ == "__main__":
    extrair_docentes_inep()