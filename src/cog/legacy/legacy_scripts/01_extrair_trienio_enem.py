import pandas as pd
import numpy as np
import zipfile
import os

# --- configuração ---
anos = [2022, 2023, 2024]
input_dir = 'data/raw'
output_dir = 'analise_exploratoria'

cols_enem = ['SG_UF_PROVA', 'NU_NOTA_CN', 'NU_NOTA_CH', 'NU_NOTA_LC', 'NU_NOTA_MT', 'NU_NOTA_REDACAO']
map_nomes = {'NU_NOTA_LC': 'linguagem', 'NU_NOTA_CH': 'humanas', 'NU_NOTA_CN': 'natureza', 'NU_NOTA_MT': 'matematica', 'NU_NOTA_REDACAO': 'redacao'}

def processar_trienio():
    os.makedirs(output_dir, exist_ok=True)
    resumo_nacional = []

    for ano in anos:
        zip_path = os.path.join(input_dir, f'microdados_enem_{ano}.zip')
        if not os.path.exists(zip_path):
            print(f"⚠️ arquivo não encontrado: {zip_path}")
            continue

        print(f"\n🚀 processando ano {ano}...")
        
        with zipfile.ZipFile(zip_path, 'r') as z:
            # ESTRATÉGIA V4: Localizar o maior arquivo CSV dentro do ZIP
            # O arquivo de microdados é sempre o maior (vários GB)
            lista_csvs = [info for info in z.infolist() if info.filename.lower().endswith('.csv')]
            
            if not lista_csvs:
                print(f"❌ erro: nenhum arquivo .csv encontrado no zip de {ano}.")
                continue
                
            # Ordena por tamanho e pega o maior
            csv_info = sorted(lista_csvs, key=lambda x: x.file_size, reverse=True)[0]
            csv_internal = csv_info.filename
            
            print(f"✅ arquivo identificado pelo tamanho ({csv_info.file_size / 1e9:.2f} GB): {csv_internal}")

            chunks_list = []
            try:
                with z.open(csv_internal) as f:
                    # leitura em blocos
                    reader = pd.read_csv(f, sep=';', encoding='latin-1', usecols=cols_enem, chunksize=300000)
                    
                    for i, chunk in enumerate(reader):
                        chunk = chunk.dropna(subset=['NU_NOTA_CN', 'NU_NOTA_CH', 'NU_NOTA_LC', 'NU_NOTA_MT', 'NU_NOTA_REDACAO'])
                        if not chunk.empty:
                            chunks_list.append(chunk)
                        if i % 10 == 0: 
                            print(f"⏳ {ano}: processando bloco {i}...")

                if not chunks_list:
                    print(f"⚠️ aviso: o ano {ano} não retornou dados válidos.")
                    continue

                df_ano_completo = pd.concat(chunks_list)

                # 1. tabela por uf
                df_uf = df_ano_completo.groupby('SG_UF_PROVA').mean()
                df_uf['media'] = df_uf.mean(axis=1)
                df_uf['desvio_padrao'] = df_uf[['NU_NOTA_CN', 'NU_NOTA_CH', 'NU_NOTA_LC', 'NU_NOTA_MT', 'NU_NOTA_REDACAO']].std(axis=1)
                df_uf = df_uf.rename(columns=map_nomes).sort_values(by='media', ascending=False)
                
                df_uf.to_csv(os.path.join(output_dir, f'tabela_enem_{ano}.csv'), sep=';', encoding='utf-8-sig')
                df_uf.to_excel(os.path.join(output_dir, f'tabela_enem_{ano}.xlsx'))

                # 2. métricas nacionais
                medias_nac = df_ano_completo[['NU_NOTA_CN', 'NU_NOTA_CH', 'NU_NOTA_LC', 'NU_NOTA_MT', 'NU_NOTA_REDACAO']].mean()
                linha_resumo = medias_nac.to_dict()
                linha_resumo['ano'] = ano
                linha_resumo['participantes_validos'] = len(df_ano_completo)
                resumo_nacional.append(linha_resumo)

            except Exception as e:
                print(f"💥 erro em {ano}: {e}")

    if resumo_nacional:
        df_consolidado = pd.DataFrame(resumo_nacional).set_index('ano').rename(columns=map_nomes)
        df_consolidado['media_geral_brasil'] = df_consolidado[['linguagem', 'humanas', 'natureza', 'matematica', 'redacao']].mean(axis=1)
        df_consolidado.to_csv(os.path.join(output_dir, 'tabela_consolidada_nacional_trienio.csv'), sep=';', encoding='utf-8-sig')
        df_consolidado.to_excel(os.path.join(output_dir, 'tabela_consolidada_nacional_trienio.xlsx'))
        print("\n✨ processamento do triênio finalizado!")
        print(df_consolidado[['media_geral_brasil', 'participantes_validos']])

if __name__ == "__main__":
    processar_trienio()