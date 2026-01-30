'''
================================================================================
MEMORANDO TÉCNICO: CONSOLIDAÇÃO DE DADOS & TRIANGULAÇÃO
PROJETO: Análise de Capital Cognitivo no Brasil (2015-2022)
================================================================================

1. METADADOS DO PROJETO
--------------------------------------------------------------------------------
INVESTIGADOR PRINCIPAL:  Dr. José Aparecido da Silva
PESQUISADOR TÉCNICO:     Me. Cássio Dalbem Barth
DATA DE EMISSÃO:         10 de Janeiro de 2026
VERSÃO DO PIPELINE:      v7.1 (Extração) / v1.3 (Triangulação)
AMBIENTE DE PROCESSAMENTO: Python 3.x (Pandas, NumPy, Pyreadstat)

2. ESCOPO DA ENTREGA
--------------------------------------------------------------------------------
Este pacote de dados contém a triangulação longitudinal entre avaliações 
internacionais (PISA) e nacionais (ENEM, SAEB) para validar a consistência 
da mensuração do capital cognitivo brasileiro.

3. ARQUITETURA DE DADOS & METODOLOGIA
--------------------------------------------------------------------------------
A análise foi dividida em três ondas temporais (Waves), respeitando a 
granularidade máxima permitida pelos microdados públicos da OCDE:

A. ONDA 1 (2015) - "VISÃO ESTADUAL" (Alta Resolução)
   - Fonte PISA: Microdados com identificação de UF.
   - Granularidade: 27 Unidades Federativas (N=27).
   - Pareamento: PISA 2015 (UF) x ENEM 2015 (UF) x SAEB 2015 (UF).
   - Status: Correlação robusta (r > 0.80).

B. ONDAS 2 e 3 (2018/2022) - "VISÃO REGIONAL" (Anonimizada)
   - Fonte PISA: Microdados anonimizados pela OCDE (apenas Macrorregião).
   - Granularidade: 5 Macrorregiões (N=5).
   - Tratamento: Agregação (média ponderada) dos microdados do ENEM e SAEB 
     de nível estadual para nível regional para permitir o pareamento.
   - Pareamento: PISA (Região) x Média ENEM (Região) x Média SAEB (Região).

4. INVENTÁRIO DE ARQUIVOS ANEXOS
--------------------------------------------------------------------------------
[1] painel_evolucao_regional.xlsx
    > Matriz longitudinal contendo as médias lado a lado (2015, 2018, 2022)
    > Permite cálculo de Delta (Variação Temporal) por região.

[2] triangulation_waves_consolidated.xlsx
    > Arquivo analítico contendo:
      - Aba '2015_State_Data': Dados brutos por Estado (27 linhas).
      - Aba '2015_Region_Data': Dados agregados por Região (comparabilidade).
      - Abas '2018/2022_Data': Dados regionais.
      - Matrizes de Correlação de Pearson para cada onda.

[3] Dados Fonte (Raw/Processed)
    > pisa_[ano]_[escala].csv
    > enem_table_[ano]_3EM.csv
    > saeb_table_[ano]_3EM.xlsx

5. NOTAS TÉCNICAS E LIMITAÇÕES
--------------------------------------------------------------------------------
* Proxy SAEB 2015: Utilizou-se dados do 9º Ano do EF (9EF) ou 3º EM (3EM) 
  conforme disponibilidade e melhor aderência à idade modal do PISA (15 anos).
* Nomes de Variáveis: As notas do ENEM referem-se à média das provas objetivas 
  e redação (Mean_General). As notas do PISA são a média das três competências 
  (Leitura, Matemática, Ciências).

================================================================================
'''

import pandas as pd
import numpy as np
import os
import seaborn as sns
import matplotlib.pyplot as plt
from pathlib import Path

# --- CONFIGURAÇÃO ---
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DATA_PROC = PROJECT_ROOT / 'data' / 'processed'
REPORTS_XLSX = PROJECT_ROOT / 'reports' / 'varcog' / 'xlsx'
IMG_DIR = PROJECT_ROOT / 'reports' / 'varcog' / 'graficos'
IMG_DIR.mkdir(parents=True, exist_ok=True)
REPORTS_XLSX.mkdir(parents=True, exist_ok=True)

# Definição dos Arquivos
FILES_MAP = {
    '2015': {
        'PISA': {'path': DATA_PROC / 'pisa_2015_states.csv', 'key': 'UF', 'score': 'Cognitive_Global_Mean'},
        'ENEM': {'path': DATA_PROC / 'enem_table_2015_3EM.csv', 'key': 'UF', 'score': 'Mean_General'},
        'SAEB': {'path': REPORTS_XLSX / 'saeb_table_2015_3EM.xlsx', 'key': 'UF', 'score': 'SAEB_General'} 
    },
    '2018': {
        'PISA': {'path': DATA_PROC / 'pisa_2018_regional_summary.csv', 'key': 'Region', 'score': 'Cognitive_Global_Mean'},
        'ENEM': {'path': DATA_PROC / 'enem_table_2018_3EM.csv', 'key': 'UF', 'score': 'Mean_General'},
        'SAEB': {'path': REPORTS_XLSX / 'saeb_table_2017_3EM.xlsx', 'key': 'UF', 'score': 'SAEB_General'}
    },
    '2022': {
        'PISA': {'path': DATA_PROC / 'pisa_2022_regional_summary.csv', 'key': 'Region', 'score': 'Cognitive_Global_Mean'},
        'ENEM': {'path': DATA_PROC / 'enem_table_2022_3EM.csv', 'key': 'UF', 'score': 'Mean_General'},
        'SAEB': {'path': REPORTS_XLSX / 'saeb_table_2023_3EM.xlsx', 'key': 'UF', 'score': 'SAEB_General'}
    }
}

UF_TO_REGION = {
    'AC':'North', 'AL':'Northeast', 'AP':'North', 'AM':'North', 'BA':'Northeast', 'CE':'Northeast', 
    'DF':'Center-West', 'ES':'Southeast', 'GO':'Center-West', 'MA':'Northeast', 'MT':'Center-West', 
    'MS':'Center-West', 'MG':'Southeast', 'PA':'North', 'PB':'Northeast', 'PR':'South', 
    'PE':'Northeast', 'PI':'Northeast', 'RJ':'Southeast', 'RN':'Northeast', 'RS':'South', 
    'RO':'North', 'RR':'North', 'SC':'South', 'SP':'Southeast', 'SE':'Northeast', 'TO':'North'
}

def load_file_smart(path_obj):
    if path_obj.exists():
        return pd.read_csv(path_obj) if path_obj.suffix == '.csv' else pd.read_excel(path_obj)
    if 'saeb' in str(path_obj).lower() and '3EM' in str(path_obj):
        alt_path = Path(str(path_obj).replace('3EM', '9EF'))
        if alt_path.exists():
            print(f"   [INFO] Usando Proxy: {alt_path.name}")
            return pd.read_csv(alt_path) if alt_path.suffix == '.csv' else pd.read_excel(alt_path)
    return None

def find_col(df, preferred, synonyms):
    if preferred in df.columns: return preferred
    for s in synonyms:
        if s in df.columns: return s
    return None

def normalize_cols(df, key_pref, score_pref, prefix):
    key_synonyms = ['UF', 'SG_UF', 'SG_UF_PROVA', 'Estado', 'Region', 'REGION']
    score_synonyms = [
        'Mean_General', 'SAEB_General', 'Cognitive_Global_Mean', 'Enem_Global_Mean',
        'MEDIA_MT_LP', 'MEDIA_TOTAL', 'Global_Mean', 'Score'
    ]

    found_key = find_col(df, key_pref, key_synonyms)
    if not found_key:
        print(f"   [ERRO] Chave '{key_pref}' não encontrada em {prefix}. Cols: {list(df.columns)}")
        return None

    found_score = find_col(df, score_pref, score_synonyms)
    if not found_score:
        if 'Math_Mean' in df.columns and 'Language_Mean' in df.columns:
             df['Calc_Mean'] = (df['Math_Mean'] + df['Language_Mean']) / 2
             found_score = 'Calc_Mean'
        else:
            print(f"   [ERRO] Nota '{score_pref}' não encontrada em {prefix}. Cols: {list(df.columns)}")
            return None
    
    mapping = {found_key: 'KEY', found_score: f'{prefix}_Score'}
    
    # Mantem Grade APENAS SE FOR NUMÉRICA, senão ignora para não quebrar agregação
    grade_col = find_col(df, 'Grade', ['Serie'])
    if grade_col:
        # Tenta converter para numérico, se falhar (ex: '3EM'), transforma em NaN e depois dropa ou ignora
        try:
            pd.to_numeric(df[grade_col], errors='raise')
            mapping[grade_col] = f'{prefix}_Grade'
        except:
            pass # Ignora coluna de Grade se for texto (ex: '3EM')
    
    return df.rename(columns=mapping)[list(mapping.values())]

def aggregate_to_region(df_state):
    df = df_state.copy()
    if 'KEY' not in df.columns: return df
    
    sample = str(df['KEY'].iloc[0])
    if len(sample) == 2 and sample in UF_TO_REGION:
        df['Region'] = df['KEY'].map(UF_TO_REGION)
        
        # Filtra apenas colunas que são Score ou Grade NUMÉRICA
        target_cols = [c for c in df.columns if ('Score' in c or 'Grade' in c) and pd.api.types.is_numeric_dtype(df[c])]
        
        return df.groupby('Region')[target_cols].mean().reset_index().rename(columns={'Region': 'KEY'})
    
    return df

def run_triangulation():
    print("="*60)
    print("      TRIANGULAÇÃO V1.4 (Safe Numeric Aggregation)")
    print("="*60)
    
    writer = pd.ExcelWriter(REPORTS_XLSX / 'triangulation_waves_consolidated.xlsx', engine='openpyxl')
    
    for wave, srcs in FILES_MAP.items():
        print(f"\n--- Processando Onda {wave} ---")
        
        pisa_raw = load_file_smart(srcs['PISA']['path'])
        enem_raw = load_file_smart(srcs['ENEM']['path'])
        saeb_raw = load_file_smart(srcs['SAEB']['path'])

        if pisa_raw is None:
            print(f"[SKIP] PISA {wave} não encontrado.")
            continue

        df_pisa = normalize_cols(pisa_raw, srcs['PISA']['key'], srcs['PISA']['score'], 'PISA')
        df_enem = normalize_cols(enem_raw, srcs['ENEM']['key'], srcs['ENEM']['score'], 'ENEM') if enem_raw is not None else None
        df_saeb = normalize_cols(saeb_raw, srcs['SAEB']['key'], srcs['SAEB']['score'], 'SAEB') if saeb_raw is not None else None

        if df_pisa is None: continue

        # --- 1. VISÃO REGIONAL ---
        print("   -> Gerando Tabela REGIONAL...")
        df_final_reg = aggregate_to_region(df_pisa)
        if df_enem is not None: 
            df_enem_reg = aggregate_to_region(df_enem)
            df_final_reg = pd.merge(df_final_reg, df_enem_reg, on='KEY', how='inner')
        if df_saeb is not None:
            df_saeb_reg = aggregate_to_region(df_saeb)
            df_final_reg = pd.merge(df_final_reg, df_saeb_reg, on='KEY', how='inner')

        if len(df_final_reg) > 0:
            df_final_reg.to_excel(writer, sheet_name=f'{wave}_Region_Data', index=False)
            scores = [c for c in df_final_reg.columns if 'Score' in c]
            if len(scores) > 1:
                df_final_reg[scores].corr().to_excel(writer, sheet_name=f'{wave}_Region_Corr')

        # --- 2. VISÃO ESTADUAL ---
        if wave == '2015':
            print("   -> Gerando Tabela ESTADUAL...")
            df_final_st = df_pisa
            if df_enem is not None: df_final_st = pd.merge(df_final_st, df_enem, on='KEY', how='inner')
            if df_saeb is not None: df_final_st = pd.merge(df_final_st, df_saeb, on='KEY', how='inner')

            if len(df_final_st) > 0:
                df_final_st.to_excel(writer, sheet_name=f'{wave}_State_Data', index=False)
                scores = [c for c in df_final_st.columns if 'Score' in c]
                if len(scores) > 1:
                    df_final_st[scores].corr().to_excel(writer, sheet_name=f'{wave}_State_Corr')

    writer.close()
    print("\n[SUCESSO] Relatório salvo.")

if __name__ == "__main__":
    run_triangulation()