import pandas as pd
import os

# --- configuração de caminhos ---
script_dir = os.path.dirname(os.path.abspath(__file__))
base_dir = os.path.dirname(script_dir)
input_dir = os.path.join(base_dir, 'analise_exploratoria')

def carregar_dados(nome_base):
    csv_path = os.path.join(input_dir, f"{nome_base}.csv")
    xlsx_path = os.path.join(input_dir, f"{nome_base}.xlsx")
    
    if os.path.exists(csv_path):
        df = pd.read_csv(csv_path, sep=';')
    elif os.path.exists(xlsx_path):
        df = pd.read_excel(xlsx_path)
    else:
        return None
        
    # padroniza nomes de colunas para maiúsculo
    df.columns = [c.upper() for c in df.columns]
    return df

def executar_analise_correlacao():
    print("📊 iniciando validação convergente (enem vs pisa)...")

    df_enem = carregar_dados('tabela_consolidada_estados_trienio')
    df_pisa = carregar_dados('dados_pisa_historico_estados')

    if df_enem is None or df_pisa is None:
        print("❌ erro: bases não encontradas.")
        return

    # normalização das chaves de ligação (unifica UF e SG_UF_PROVA)
    if 'UF' in df_enem.columns:
        df_enem = df_enem.rename(columns={'UF': 'SG_UF_PROVA'})
    
    # cruzamento das bases
    try:
        df_merge = pd.merge(df_enem, df_pisa, on='SG_UF_PROVA')
    except KeyError:
        print(f"❌ erro: colunas incompatíveis. enem: {df_enem.columns} | pisa: {df_pisa.columns}")
        return

    # cálculo da correlação de pearson (r)
    # nota: pisa 2022 é o parâmetro para o triênio recente
    res = []
    for ano in [2022, 2023, 2024]:
        col_enem = f'MEDIA_{ano}'
        col_pisa = 'PISA_GERAL_2022'
        
        if col_enem in df_merge.columns:
            r = df_merge[col_enem].corr(df_merge[col_pisa])
            res.append({'par_analisado': f'enem {ano} vs pisa 2022', 'r': r})

    # análise longitudinal (enem 2024 vs pisa 2018)
    r_hist = df_merge['MEDIA_2024'].corr(df_merge['PISA_GERAL_2018'])
    res.append({'par_analisado': 'enem 2024 vs pisa 2018', 'r': r_hist})

    # exportação acadêmica
    df_res = pd.DataFrame(res)
    output_path = os.path.join(input_dir, 'resultado_estatistico_correlacao.xlsx')
    df_res.to_excel(output_path, index=False)

    print("\n📈 coeficientes de correlação (r) identificados:")
    for _, row in df_res.iterrows():
        print(f"   - {row['par_analisado']}: {row['r']:.4f}")

    r_medio = df_res.iloc[0:3]['r'].mean()
    print(f"\n💡 correlação média do triênio: {r_medio:.4f}")

if __name__ == "__main__":
    executar_analise_correlacao()