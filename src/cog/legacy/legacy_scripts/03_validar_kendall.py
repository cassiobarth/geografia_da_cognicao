import pandas as pd
import numpy as np
import os

# --- configuração de caminhos ---
input_dir = 'analise_exploratoria'
# buscamos o arquivo gerado pelo script 02
input_csv = os.path.join(input_dir, 'tabela_consolidada_estados_trienio.csv')
input_xlsx = os.path.join(input_dir, 'tabela_consolidada_estados_trienio.xlsx')

output_detalhado = os.path.join(input_dir, 'analise_estabilidade_kendall_completa.xlsx')

def calcular_kendall_w(df_ranks):
    """calcula o coeficiente de concordância w de kendall."""
    m = df_ranks.shape[1]  # número de anos
    n = df_ranks.shape[0]  # número de estados
    
    soma_ranks_linhas = df_ranks.sum(axis=1)
    media_soma_ranks = soma_ranks_linhas.mean()
    s = ((soma_ranks_linhas - media_soma_ranks)**2).sum()
    
    w = (12 * s) / (m**2 * (n**3 - n))
    return w

def executar_validacao():
    # lógica de carregamento flexível (csv ou excel)
    if os.path.exists(input_csv):
        df = pd.read_csv(input_csv, sep=';')
        print(f"✅ lendo dados de: {input_csv}")
    elif os.path.exists(input_xlsx):
        df = pd.read_excel(input_xlsx)
        print(f"✅ lendo dados de: {input_xlsx}")
    else:
        print("❌ erro: tabela consolidada não encontrada. rode o script 02 primeiro.")
        return

    colunas_medias = [c for c in df.columns if c.startswith('media_20')]
    
    # 1. geração de rankings anuais (1º lugar é a maior nota)
    for col in colunas_medias:
        ano = col.split('_')[1]
        df[f'rank_{ano}'] = df[col].rank(ascending=False, method='min').astype(int)

    # 2. cálculo do coeficiente w de kendall
    cols_ranks = [c for c in df.columns if c.startswith('rank_')]
    w_valor = calcular_kendall_w(df[cols_ranks])

    # 3. métricas de estabilidade
    df['variacao_posicao_maxima'] = df[cols_ranks].max(axis=1) - df[cols_ranks].min(axis=1)
    df['posicao_media'] = df[cols_ranks].mean(axis=1).round(1)
    
    # 4. identificação de abismos e consistência
    df = df.sort_values(by='media_trienio', ascending=False)
    
    # 5. resumo estatístico para o reporte
    resumo = pd.DataFrame({
        'métrica': ['coeficiente w de kendall', 'estabilidade do ranking', 'estados analisados', 'período'],
        'valor': [f"{w_valor:.4f}", 'extrema' if w_valor > 0.9 else 'alta', 'vinte e sete', '2022-2024']
    })

    # exportação para excel com múltiplas abas
    with pd.ExcelWriter(output_detalhado, engine='openpyxl') as writer:
        df.to_excel(writer, sheet_name='rankings_e_estabilidade', index=False)
        resumo.to_excel(writer, sheet_name='resumo_estatistico', index=False)

    print(f"\n📊 análise concluída!")
    print(f"✨ w de kendall: {w_valor:.4f}")
    print(f"📂 tabela completa salva em: {output_detalhado}")

if __name__ == "__main__":
    executar_validacao()