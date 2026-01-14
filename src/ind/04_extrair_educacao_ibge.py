"""
PROJETO: ANALISE DO REPERTORIO COGNITIVO NO BRASIL POR UF
ARQUIVO: 01_processar_indicadores_basicos.py
FONTE: IBGE (Tabela 5938 - Contas Regionais e Censo Demográfico 2022)

METODOLOGIA:
1. Carrega dados brutos de PIB (XLSX) e remove cabeçalhos/rodapés.
2. Cruza com dados estáticos do Censo 2022 (População e Área).
3. Calcula indicadores derivados:
   - Média do PIB (2022-2023).
   - PIB per Capita.
   - Densidade Demográfica (hab/km²).
4. Exporta tabela consolidada em CSV e Excel para pasta local.
"""

import pandas as pd
import os

def processar_dados_ibge():
    # --- CONFIGURAÇÃO DE CAMINHOS ---
    caminho_entrada = r'data\raw\IBGE_Tabela5938.xlsx'
    caminho_saida_csv = r'data\raw\indicadores_basicos_uf.csv'
    caminho_saida_xlsx = r'data\raw\indicadores_basicos_uf.xlsx'

    # Verifica existência do arquivo
    if not os.path.exists(caminho_entrada):
        print(f"[ERRO] Arquivo não encontrado: {caminho_entrada}")
        return

    print(">>> Iniciando processamento de dados...")

    # --- 1. EXTRAÇÃO E LIMPEZA (PIB) ---
    # O cabeçalho real dos anos está na linha 4 (índice 3)
    df = pd.read_excel(caminho_entrada, header=3)
    
    # Renomear colunas essenciais
    df.rename(columns={'Unnamed: 0': 'UF', 2022: 'PIB_2022', 2023: 'PIB_2023'}, inplace=True)

    # Filtrar apenas linhas válidas (onde PIB_2022 não é nulo)
    df_clean = df.dropna(subset=['PIB_2022']).copy()
    
    # Garantir tipagem numérica
    for col in ['PIB_2022', 'PIB_2023']:
        df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce')

    # Calcular média dos dois anos (em Mil Reais)
    df_clean['Media_PIB_Mil_Reais'] = df_clean[['PIB_2022', 'PIB_2023']].mean(axis=1)

    # --- 2. ENRIQUECIMENTO (DADOS DO CENSO 2022) ---
    # Dicionário manual para garantir integridade sem dependência de outros arquivos externos agora
    dados_censo = {
        # UF: [População, Área km²]
        'Acre': [830018, 164123],
        'Alagoas': [3127683, 27778],
        'Amapá': [733759, 142828],
        'Amazonas': [3941613, 1559146],
        'Bahia': [14141626, 564733],
        'Ceará': [8794957, 148920],
        'Distrito Federal': [2817381, 5760],
        'Espírito Santo': [3833712, 46095],
        'Goiás': [7056495, 340111],
        'Maranhão': [6776699, 329628],
        'Mato Grosso': [3658649, 903366],
        'Mato Grosso do Sul': [2757013, 357145],
        'Minas Gerais': [20539989, 586522],
        'Pará': [8120131, 1247954],
        'Paraíba': [3974687, 56469],
        'Paraná': [11444380, 199307],
        'Pernambuco': [9058931, 98148],
        'Piauí': [3271199, 251577],
        'Rio de Janeiro': [16055174, 43780],
        'Rio Grande do Norte': [3302729, 52811],
        'Rio Grande do Sul': [10882965, 281730],
        'Rondônia': [1581196, 237590],
        'Roraima': [636707, 224300],
        'Santa Catarina': [7610361, 95736],
        'São Paulo': [44411238, 248222],
        'Sergipe': [2210004, 21915],
        'Tocantins': [1511460, 277720]
    }

    # Normalizar nomes das UFs (remover espaços extras)
    df_clean['UF'] = df_clean['UF'].str.strip()

    # Mapear dados do dicionário
    df_clean['Populacao_2022'] = df_clean['UF'].map(lambda x: dados_censo.get(x, [None, None])[0])
    df_clean['Area_km2'] = df_clean['UF'].map(lambda x: dados_censo.get(x, [None, None])[1])

    # --- 3. CÁLCULO DE INDICADORES ---
    
    # PIB per Capita: (Média PIB * 1000) / População
    df_clean['PIB_Per_Capita'] = (df_clean['Media_PIB_Mil_Reais'] * 1000) / df_clean['Populacao_2022']
    
    # Densidade Demográfica: População / Área
    df_clean['Densidade_Demografica'] = df_clean['Populacao_2022'] / df_clean['Area_km2']

    # --- 4. EXPORTAÇÃO ---
    colunas_finais = [
        'UF', 
        'Populacao_2022', 
        'Area_km2', 
        'Densidade_Demografica', 
        'Media_PIB_Mil_Reais', 
        'PIB_Per_Capita'
    ]
    
    df_final = df_clean[colunas_finais].sort_values('PIB_Per_Capita', ascending=False)

    # Exportar Excel e CSV
    df_final.to_csv(caminho_saida_csv, index=False, sep=';', decimal=',', encoding='utf-8-sig')
    df_final.to_excel(caminho_saida_xlsx, index=False)

    print(f">>> Processamento concluído.")
    print(f"Arquivos gerados:\n - {caminho_saida_csv}\n - {caminho_saida_xlsx}")
    print("\nAmostra dos dados processados:")
    print(df_final.head())

if __name__ == "__main__":
    processar_dados_ibge()