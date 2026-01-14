"""
PROJETO: ANALISE DO REPERTORIO COGNITIVO NO BRASIL POR UF
ARQUIVO: 06_analise_exploratoria_visual.py
OBJETIVO: Gerar visualizacoes estatisticas dos indicadores socioeconomicos.

OUTPUT:
- Salva graficos .png em 'analise_exploratoria/ind_se/graficos'
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os

# --- Configuracao de Caminhos ---
script_dir = os.path.dirname(os.path.abspath(__file__))
base_dir = os.path.dirname(os.path.dirname(script_dir))

input_file = os.path.join(base_dir, 'data', 'processed', 'base_mestra_indicadores_completa.xlsx')
output_dir_graficos = os.path.join(base_dir, 'analise_exploratoria', 'ind_se', 'graficos')

def configurar_estilo():
    sns.set_theme(style="whitegrid")
    plt.rcParams['figure.figsize'] = (12, 8)
    plt.rcParams['font.size'] = 11

def gerar_matriz_correlacao(df):
    print("   Gerando Matriz de Correlacao...")
    plt.figure(figsize=(14, 10))
    
    # Seleciona apenas colunas numericas
    cols_num = df.select_dtypes(include=['float64', 'int64']).columns
    corr = df[cols_num].corr()
    
    mask =  None # Pode usar np.triu(np.ones_like(corr, dtype=bool)) se quiser triangular
    sns.heatmap(corr, mask=mask, annot=True, fmt=".2f", cmap='coolwarm', vmin=-1, vmax=1, linewidths=.5)
    
    plt.title('Matriz de Correlação: Indicadores Socioeconômicos (2022)', pad=20)
    plt.tight_layout()
    
    save_path = os.path.join(output_dir_graficos, '01_matriz_correlacao_indicadores.png')
    plt.savefig(save_path, dpi=300)
    plt.close()

def gerar_ranking_idh(df):
    print("   Gerando Ranking IDH...")
    plt.figure(figsize=(12, 8))
    
    df_sorted = df.sort_values('IDH_ESTADUAL', ascending=False)
    
    sns.barplot(x='IDH_ESTADUAL', y='SG_UF_PROVA', data=df_sorted, palette='viridis')
    plt.xlabel('IDH (2021)')
    plt.ylabel('Unidade da Federação')
    plt.title('Ranking de IDH por Estado', pad=15)
    
    save_path = os.path.join(output_dir_graficos, '02_ranking_idh_estados.png')
    plt.savefig(save_path, dpi=300)
    plt.close()

def gerar_dispersao_investimento_pib(df):
    print("   Gerando Dispersao (Investimento x PIB)...")
    plt.figure(figsize=(10, 6))
    
    sns.scatterplot(data=df, x='PIB_PER_CAPITA', y='INVESTIMENTO_RCL_PERC', s=100, color='dodgerblue')
    
    # Adicionar labels nos pontos
    for line in range(0, df.shape[0]):
        plt.text(
            df.PIB_PER_CAPITA[line]+0.2, 
            df.INVESTIMENTO_RCL_PERC[line], 
            df.SG_UF_PROVA[line], 
            horizontalalignment='left', 
            size='small', 
            color='black'
        )

    plt.title('Relação: PIB per Capita vs. Investimento Público (% RCL)')
    plt.xlabel('PIB per Capita (R$)')
    plt.ylabel('Investimento (% da Receita Corrente Líquida)')
    plt.tight_layout()
    
    save_path = os.path.join(output_dir_graficos, '03_dispersao_pib_investimento.png')
    plt.savefig(save_path, dpi=300)
    plt.close()

def main():
    print("Iniciando Analise Visual (Geracao de Graficos)...")
    
    if not os.path.exists(input_file):
        print(f"ERRO: Base mestra nao encontrada em: {input_file}")
        print("Rode o script '05_consolidar_base_uf.py' primeiro.")
        return

    # Garante que a pasta existe
    os.makedirs(output_dir_graficos, exist_ok=True)

    # Carrega dados
    df = pd.read_excel(input_file)
    print(f"   Dados carregados: {len(df)} UFs")

    # Configura e Gera
    configurar_estilo()
    
    try:
        gerar_matriz_correlacao(df)
        gerar_ranking_idh(df)
        gerar_dispersao_investimento_pib(df)
        print(f"\nSUCESSO! Graficos salvos em:\n   {output_dir_graficos}")
    except Exception as e:
        print(f"ERRO ao gerar graficos: {e}")

if __name__ == "__main__":
    main()