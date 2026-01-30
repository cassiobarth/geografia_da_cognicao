"""
PROJETO: AUDITORIA DE DADOS PISA 2022
ARQUIVO: 99_gerar_amostra_visual_professor.py
OBJETIVO: Gerar um arquivo Excel contendo o 'Head' e 'Tail' dos microdados
          para comprovar acesso à fonte primária e estrutura do arquivo.
"""

import pandas as pd
import pyreadstat
import os

def gerar_amostra_visual():
    # 1. Configurar Caminhos
    script_dir = os.path.dirname(os.path.abspath(__file__))
    base_dir = os.path.dirname(script_dir)
    input_file = os.path.join(base_dir, 'data', 'raw', 'pisa_2022', 'CY08MSP_STU_QQQ.sav')
    output_file = os.path.join(base_dir, 'data', 'processed', 'amostra_estrutura_pisa_audit.xlsx')

    print(f"--- GERANDO AMOSTRA VISUAL PARA AUDITORIA ---")
    print(f"Lendo: {input_file}")

    if not os.path.exists(input_file):
        print("❌ Arquivo não encontrado.")
        return

    # 2. Selecionar colunas estratégicas
    # (Não vamos pegar as 1000 colunas, apenas as que provam seu trabalho)
    colunas_chave = [
        'CNT',          # País (Prova que filtrou Brasil)
        'CNTSCHID',     # ID da Escola
        'CNTSTUID',     # ID do Aluno
        'STRATUM',      # A COLUNA CHAVE (Onde está o código do Estado)
        'ST004D01T',    # Gênero (Dado demográfico comum)
        'ESCS',         # Nível Socioeconômico (Variável complexa)
        'PV1MATH',      # Nota Matemática (Plausible Value 1)
        'PV10MATH',     # Nota Matemática (Plausible Value 10)
        'PV1READ',      # Nota Leitura
        'PV1SCIE'       # Nota Ciências
    ]

    try:
        # Lê o arquivo (pode demorar um pouco pois lê tudo para pegar o final)
        print("⏳ Lendo arquivo e filtrando colunas relevantes...")
        df, meta = pyreadstat.read_sav(input_file, usecols=colunas_chave, disable_datetime_conversion=True)
    except Exception as e:
        print(f"Erro: {e}")
        return

    # 3. Filtrar Brasil
    print("🇧🇷 Filtrando alunos do Brasil...")
    df_bra = df[df['CNT'] == 'BRA'].copy()
    print(f"   Total de alunos encontrados: {len(df_bra)}")

    # 4. Criar o 'Sanduíche' (Primeiras 50 + Últimas 50 linhas)
    print("✂️  Recortando amostra (Head & Tail)...")
    df_head = df_bra.head(50)
    df_tail = df_bra.tail(50)
    
    # Adiciona uma linha divisória fake para ficar visual no Excel
    linha_divisoria = pd.DataFrame([{col: '...' for col in df_bra.columns}])
    
    df_final = pd.concat([df_head, linha_divisoria, df_tail])

    # 5. Exportar para Excel
    print(f"💾 Salvando em Excel: {output_file}")
    df_final.to_excel(output_file, index=False)
    
    print("✅ PRONTO! Pode abrir o Excel e mostrar ao professor.")

if __name__ == "__main__":
    gerar_amostra_visual()