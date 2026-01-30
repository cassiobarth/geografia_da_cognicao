"""
PROJETO: ANALISE DO REPERTORIO COGNITIVO NO BRASIL POR UF
ARQUIVO: 04_extrair_dados_snis.py
FONTE: Sistema Nacional de Informacoes sobre Saneamento (SNIS) - 2022

SOLUCAO BLINDADA:
1. Detecta encoding e limpa delimitadores.
2. Filtra UFs e converte dados.
3. Exporta Excel com aba nomeada 'SNIS_2022'.
"""

import pandas as pd
import os
import io

# --- Configuracao ---
script_dir = os.path.dirname(os.path.abspath(__file__))
base_dir = os.path.dirname(os.path.dirname(script_dir))
output_dir_csv = os.path.join(base_dir, 'analise_exploratoria', 'ind_se', 'csv')
output_dir_xlsx = os.path.join(base_dir, 'analise_exploratoria', 'ind_se', 'xlsx')

input_file = os.path.join(base_dir, 'data', 'raw', 'snis_municipios_2022.csv')

# Siglas validas
SIGLAS_UF = [
    'AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA', 'MT', 'MS', 
    'MG', 'PA', 'PB', 'PR', 'PE', 'PI', 'RJ', 'RN', 'RS', 'RO', 'RR', 'SC', 
    'SP', 'SE', 'TO'
]

# De-Para Nomes -> Siglas
DE_PARA_UF = {
    'Acre': 'AC', 'Alagoas': 'AL', 'Amapá': 'AP', 'Amazonas': 'AM', 'Bahia': 'BA', 
    'Ceará': 'CE', 'Distrito Federal': 'DF', 'Espírito Santo': 'ES', 'Goiás': 'GO', 
    'Maranhão': 'MA', 'Mato Grosso': 'MT', 'Mato Grosso do Sul': 'MS', 'Minas Gerais': 'MG', 
    'Pará': 'PA', 'Paraíba': 'PB', 'Paraná': 'PR', 'Pernambuco': 'PE', 'Piauí': 'PI', 
    'Rio de Janeiro': 'RJ', 'Rio Grande do Norte': 'RN', 'Rio Grande do Sul': 'RS', 
    'Rondônia': 'RO', 'Roraima': 'RR', 'Santa Catarina': 'SC', 'São Paulo': 'SP', 
    'Sergipe': 'SE', 'Tocantins': 'TO'
}

def executar_health_check(df):
    print(f"\n[Health Check]")
    erros = []
    if len(df) != 27:
        erros.append(f"Erro Critico: Encontradas {len(df)} UFs. Esperado: 27.")
    
    if df['AGUA_ATENDIMENTO_PERC'].isnull().any(): erros.append("Nulos em AGUA")
    if df['ESGOTO_ATENDIMENTO_PERC'].isnull().any(): erros.append("Nulos em ESGOTO")

    if not erros:
        return True
    else:
        for erro in erros: print(f"FAIL: {erro}")
        return False

def extrair_dados_snis():
    print("Iniciando processamento SNIS (Aba Nomeada)...")
    
    if not os.path.exists(input_file):
        print(f"ERRO: Arquivo nao encontrado: {input_file}")
        return

    try:
        buffer_limpo = io.StringIO()
        encoding_usado = 'utf-16-le'
        
        # Deteccao de Encoding
        try:
            with open(input_file, 'r', encoding='utf-16-le') as f: f.read(100)
        except:
            encoding_usado = 'utf-8'

        try:
            with open(input_file, 'r', encoding='utf-8') as f:
                if 'Código' in f.readline(): encoding_usado = 'utf-8'
        except: pass

        print(f"   Encoding: {encoding_usado}")

        # Limpeza
        with open(input_file, 'r', encoding=encoding_usado, errors='replace') as f:
            for linha in f:
                linha = linha.strip()
                if not linha: continue
                if linha.endswith(';'): linha = linha[:-1]
                buffer_limpo.write(linha + '\n')
        
        buffer_limpo.seek(0)
        df = pd.read_csv(buffer_limpo, sep=';', on_bad_lines='warn')
        df.columns = [str(c).strip().replace('"', '') for c in df.columns]

        # Mapeamento
        col_uf = next((c for c in df.columns if c in ['Estado', 'UF', 'Sigla']), None)
        col_agua = next((c for c in df.columns if 'IN055' in c), None)
        col_esgoto = next((c for c in df.columns if 'IN056' in c), None)

        if not all([col_uf, col_agua, col_esgoto]):
            print("ERRO: Colunas nao encontradas.")
            return

        # Padronizacao
        df[col_uf] = df[col_uf].astype(str).str.strip().str.replace('"', '')
        df['SG_UF_PROVA'] = df[col_uf].map(DE_PARA_UF).fillna(df[col_uf])
        df = df[df['SG_UF_PROVA'].isin(SIGLAS_UF)].copy()

        # Conversao
        for col in [col_agua, col_esgoto]:
            df[col] = df[col].astype(str).str.replace('.', '', regex=False)
            df[col] = df[col].str.replace(',', '.', regex=False)
            df[col] = pd.to_numeric(df[col], errors='coerce')

        # Agregacao
        df_final = df.groupby('SG_UF_PROVA').agg({
            col_agua: 'mean',
            col_esgoto: 'mean'
        }).reset_index()

        df_final.columns = ['SG_UF_PROVA', 'AGUA_ATENDIMENTO_PERC', 'ESGOTO_ATENDIMENTO_PERC']
        df_final = df_final.round(2)

        # Exportacao
        if executar_health_check(df_final):
            os.makedirs(output_dir_csv, exist_ok=True)
            os.makedirs(output_dir_xlsx, exist_ok=True)
            
            file_xlsx = os.path.join(output_dir_xlsx, 'dados_saneamento_snis_2022.xlsx')
            file_csv = os.path.join(output_dir_csv, 'dados_saneamento_snis_2022.csv')
            
            # AQUI ESTA A MUDANCA: sheet_name='SNIS_2022'
            df_final.to_excel(file_xlsx, index=False, sheet_name='SNIS_2022')
            df_final.to_csv(file_csv, index=False, sep=';', encoding='utf-8-sig')
            
            print(f"Exportacao concluida (Aba: SNIS_2022):\n   {file_xlsx}")

    except Exception as e:
        print(f"Erro fatal: {e}")

if __name__ == "__main__":
    extrair_dados_snis()