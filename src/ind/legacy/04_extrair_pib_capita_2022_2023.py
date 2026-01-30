"""
PROJETO: ANALISE DO REPERTORIO COGNITIVO NO BRASIL POR UF
ARQUIVO: 04_extrair_pib_capita_2022_2023.py
FONTE: IBGE

CORREÇÃO APLICADA: Renomeação de colunas por índice (posição) para evitar KeyError.
"""

import pandas as pd
import os

# --- AJUSTE DE CAMINHOS ---
# Usa o caminho relativo à pasta onde o script é executado, mas garante robustez
diretorio_atual = os.getcwd()
caminho_arquivo = os.path.join(diretorio_atual, 'data', 'raw', 'IBGE_Tabela5938.xlsx')

# Verifica se o arquivo existe
if not os.path.exists(caminho_arquivo):
    print(f"[ERRO] Arquivo não encontrado em: {caminho_arquivo}")
    # Tenta procurar na pasta raw direta caso a estrutura seja diferente
    caminho_arquivo_alt = os.path.join(diretorio_atual, 'raw', 'IBGE_Tabela5938.xlsx')
    if os.path.exists(caminho_arquivo_alt):
        caminho_arquivo = caminho_arquivo_alt
        print(f"[AVISO] Arquivo encontrado no caminho alternativo: {caminho_arquivo}")
    else:
        exit() # Encerra se não achar

print(f">>> Lendo arquivo: {caminho_arquivo}")

# 1. Leitura do Arquivo
# header=3 geralmente pega a linha onde estão os anos 2022 e 2023
df = pd.read_excel(caminho_arquivo, header=3)

# --- CORREÇÃO DO ERRO ---
# Em vez de df.rename com nomes específicos que podem variar (int vs str),
# vamos forçar o nome das 3 primeiras colunas que sabemos que são UF, 2022 e 2023.
print("Colunas originais encontradas:", df.columns.tolist())

# Renomeia as 3 primeiras colunas pela posição (Índice 0, 1 e 2)
# Isso resolve o problema se o Excel leu "2022" como texto ou número
colunas_novas = list(df.columns)
colunas_novas[0] = 'UF'       # Primeira coluna é sempre o Estado
colunas_novas[1] = 'PIB_2022' # Segunda coluna
colunas_novas[2] = 'PIB_2023' # Terceira coluna
df.columns = colunas_novas

# Limpeza: Remover linhas onde o nome da UF ou o PIB_2022 estão vazios
df_clean = df.dropna(subset=['UF', 'PIB_2022']).copy()

# Garantir que os valores são numéricos
cols_numericas = ['PIB_2022', 'PIB_2023']
for col in cols_numericas:
    df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce')

# 2. Calcular a Média do PIB (Total)
df_clean['Media_PIB_Mil_Reais'] = df_clean[['PIB_2022', 'PIB_2023']].mean(axis=1)

# 3. Dicionário de População (Censo 2022 - IBGE)
pop_2022 = {
    'São Paulo': 44411238, 'Minas Gerais': 20539989, 'Rio de Janeiro': 16055174,
    'Bahia': 14141626, 'Paraná': 11444380, 'Rio Grande do Sul': 10882965,
    'Pernambuco': 9058931, 'Ceará': 8794957, 'Pará': 8120131,
    'Santa Catarina': 7610361, 'Goiás': 7056495, 'Maranhão': 6776699,
    'Paraíba': 3974687, 'Amazonas': 3941613, 'Espírito Santo': 3833712,
    'Mato Grosso': 3658649, 'Rio Grande do Norte': 3302729, 'Piauí': 3271199,
    'Alagoas': 3127683, 'Distrito Federal': 2817381, 'Mato Grosso do Sul': 2757013,
    'Sergipe': 2210004, 'Rondônia': 1581196, 'Tocantins': 1511460,
    'Acre': 830018, 'Amapá': 733759, 'Roraima': 636707
}

# Normalizar nomes das UFs (remover espaços em branco extras que vêm do Excel)
df_clean['UF'] = df_clean['UF'].astype(str).str.strip()

# Mapear população
df_clean['Populacao_2022'] = df_clean['UF'].map(pop_2022)

# Verificar se algum estado não foi encontrado (útil para debug)
nulos = df_clean[df_clean['Populacao_2022'].isna()]
if not nulos.empty:
    print("\n[ATENÇÃO] Não foi possível encontrar população para as seguintes linhas (verifique nomes):")
    print(nulos['UF'].unique())
    # Remove linhas que não são estados (ex: totais ou lixo do excel)
    df_clean = df_clean.dropna(subset=['Populacao_2022'])

# 4. Calcular PIB per Capita
df_clean['PIB_Per_Capita_Reais'] = (df_clean['Media_PIB_Mil_Reais'] * 1000) / df_clean['Populacao_2022']

# Selecionar e ordenar colunas finais
df_final = df_clean[['UF', 'Media_PIB_Mil_Reais', 'Populacao_2022', 'PIB_Per_Capita_Reais']].sort_values('PIB_Per_Capita_Reais', ascending=False)

# 5. Salvar arquivos na pasta raw
caminho_csv = os.path.join(diretorio_atual, 'data', 'raw', 'media_pib_per_capita.csv')
caminho_xlsx = os.path.join(diretorio_atual, 'data', 'raw', 'media_pib_per_capita.xlsx')

# Garantir que a pasta de saída existe
os.makedirs(os.path.dirname(caminho_csv), exist_ok=True)

df_final.to_csv(caminho_csv, index=False, encoding='utf-8-sig', sep=';', decimal=',')
df_final.to_excel(caminho_xlsx, index=False)

print(f"\nArquivos gerados com sucesso:\nCSV: {caminho_csv}\nXLSX: {caminho_xlsx}")
print(df_final.head())