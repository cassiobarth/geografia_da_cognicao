import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import os

# --- configuração de caminhos ---
script_dir = os.path.dirname(os.path.abspath(__file__))
base_dir = os.path.dirname(script_dir)
input_dir = os.path.join(base_dir, 'analise_exploratoria')
output_plot = os.path.join(input_dir, 'grafico_validacao_convergente_enem_pisa.png')

def gerar_grafico_convergencia():
    print("🎨 gerando visualização estatística para o professor...")

    # carga de dados (com a lógica de normalização que funcionou no 05a)
    path_enem = os.path.join(input_dir, 'tabela_consolidada_estados_trienio.xlsx')
    path_pisa = os.path.join(input_dir, 'dados_pisa_historico_estados.xlsx')

    if not os.path.exists(path_enem) or not os.path.exists(path_pisa):
        print("❌ erro: arquivos para o gráfico não encontrados.")
        return

    df_enem = pd.read_excel(path_enem)
    df_pisa = pd.read_excel(path_pisa)

    # padroniza colunas
    df_enem.columns = [c.upper() for c in df_enem.columns]
    df_pisa.columns = [c.upper() for c in df_pisa.columns]

    if 'UF' in df_enem.columns:
        df_enem = df_enem.rename(columns={'UF': 'SG_UF_PROVA'})

    # merge
    df = pd.merge(df_enem, df_pisa, on='SG_UF_PROVA')

    # configuração do gráfico
    plt.figure(figsize=(10, 8), dpi=300)
    x = df['MEDIA_2024']
    y = df['PISA_GERAL_2022']
    estados = df['SG_UF_PROVA']

    # plot dos pontos
    plt.scatter(x, y, color='#1f77b4', s=100, alpha=0.7, edgecolors='w', linewidth=1.5, label='estados (uf)')

    # linha de tendência (regressão linear)
    z = np.polyfit(x, y, 1)
    p = np.poly1d(z)
    plt.plot(x, p(x), color="#d62728", linestyle="--", linewidth=2, alpha=0.8, label='tendência linear')

    # etiquetas dos estados (evitando sobreposição básica)
    for i, txt in enumerate(estados):
        plt.annotate(txt, (x.iloc[i], y.iloc[i]), xytext=(5, 5), textcoords='offset points', fontsize=9)

    # títulos e labels (seguindo as normas acadêmicas e suas preferências)
    r_valor = x.corr(y)
    plt.title(f"validade convergente: enem 2024 vs pisa 2022\n(correlação de pearson r = {r_valor:.4f})", 
              fontsize=14, fontweight='bold', pad=20)
    plt.xlabel("média enem 2024 (pontos)", fontsize=12)
    plt.ylabel("média pisa 2022 (pontos)", fontsize=12)
    
    # anotação do abismo identificado
    plt.text(x.min(), y.max(), f"abismo máximo: sessenta e quatro vírgula doze pontos", 
             fontsize=10, bbox=dict(facecolor='white', alpha=0.5))

    plt.grid(True, linestyle=':', alpha=0.6)
    plt.legend()
    
    # salvando
    plt.tight_layout()
    plt.savefig(output_plot)
    print(f"✨ gráfico salvo com sucesso em: {output_plot}")

if __name__ == "__main__":
    gerar_grafico_convergencia()