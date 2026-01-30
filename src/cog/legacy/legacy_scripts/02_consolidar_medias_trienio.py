import pandas as pd
import os

# configuração de caminhos e arquivos
arquivos = {
    2022: 'tabela_enem_2022.csv',
    2023: 'tabela_enem_2023.csv',
    2024: 'tabela_enem_2024.csv'
}
output_file = 'tabela_consolidada_estados_trienio.xlsx'

def gerar_tabela_consolidada():
    dfs_anos = []

    for ano, nome_arquivo in arquivos.items():
        if os.path.exists(nome_arquivo):
            # lendo os arquivos csv que usam ponto e vírgula como delimitador
            df = pd.read_csv(nome_arquivo, sep=';')
            
            # selecionando apenas a sigla do estado e a média
            df = df[['SG_UF_PROVA', 'media']].rename(columns={'media': f'media_{ano}'})
            dfs_anos.append(df)
        else:
            print(f"⚠️ arquivo não encontrado: {nome_arquivo}")

    if not dfs_anos:
        print("❌ nenhum arquivo encontrado para processar.")
        return

    # consolidando todos os anos em uma única tabela através da sigla do estado
    df_final = dfs_anos[0]
    for df_ano in dfs_anos[1:]:
        df_final = df_final.merge(df_ano, on='SG_UF_PROVA', how='outer')

    # calculando a média das médias (média do triênio)
    colunas_medias = [f'media_{ano}' for ano in arquivos.keys() if f'media_{ano}' in df_final.columns]
    df_final['media_trienio'] = df_final[colunas_medias].mean(axis=1)

    # ordenando pela média do triênio (do maior para o menor)
    df_final = df_final.sort_values(by='media_trienio', ascending=False)

    # salvando em excel
    try:
        df_final.to_excel(output_file, index=False)
        print(f"✅ sucesso! arquivo salvo em: {output_file}")
    except Exception as e:
        print(f"❌ erro ao salvar o excel: {e}")
import pandas as pd
import os

# --- configuração de caminhos ---
# os arquivos estão dentro da subpasta analise_exploratoria
input_dir = 'analise_exploratoria'
output_file = os.path.join(input_dir, 'tabela_consolidada_estados_trienio.xlsx')

arquivos = {
    2022: 'tabela_enem_2022.csv',
    2023: 'tabela_enem_2023.csv',
    2024: 'tabela_enem_2024.csv'
}

def gerar_tabela_consolidada():
    dfs_anos = []

    print("📂 iniciando consolidação dos dados estaduais...")

    for ano, nome_arquivo in arquivos.items():
        caminho_completo = os.path.join(input_dir, nome_arquivo)
        
        if os.path.exists(caminho_completo):
            # lendo os arquivos csv (delimitador ponto e vírgula)
            df = pd.read_csv(caminho_completo, sep=';')
            
            # padronização: garantir que usamos as colunas corretas
            # alguns arquivos usam 'SG_UF_PROVA', outros podem usar 'uf'
            col_uf = 'SG_UF_PROVA' if 'SG_UF_PROVA' in df.columns else 'uf'
            
            df = df[[col_uf, 'media']].rename(columns={
                col_uf: 'uf', 
                'media': f'media_{ano}'
            })
            dfs_anos.append(df)
            print(f"✅ dados de {ano} carregados.")
        else:
            print(f"⚠️ arquivo não encontrado: {caminho_completo}")

    if not dfs_anos:
        print("❌ erro: nenhum arquivo csv foi encontrado na pasta 'analise_exploratoria'.")
        return

    # unindo os anos através da sigla do estado
    df_final = dfs_anos[0]
    for df_ano in dfs_anos[1:]:
        df_final = df_final.merge(df_ano, on='uf', how='outer')

    # calculando a média das médias (triênio)
    colunas_medias = [c for c in df_final.columns if c.startswith('media_')]
    df_final['media_trienio'] = df_final[colunas_medias].mean(axis=1)

    # ordenando pelo desempenho geral do triênio
    df_final = df_final.sort_values(by='media_trienio', ascending=False)

    # salvando o resultado final em excel
    try:
        df_final.to_excel(output_file, index=False)
        print(f"\n✨ sucesso! a tabela consolidada foi salva em: {output_file}")
    except Exception as e:
        print(f"\n❌ erro ao salvar o arquivo excel: {e}")

if __name__ == "__main__":
    gerar_tabela_consolidada()
if __name__ == "__main__":
    gerar_tabela_consolidada()