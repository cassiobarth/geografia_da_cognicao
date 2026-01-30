import pandas as pd
import os

# Caminho base
base = 'data/processed'

# Acha o arquivo do ENEM 2015
files = [f for f in os.listdir(base) if 'enem' in f.lower() and '2015' in f]
print("Arquivos ENEM 2015 encontrados:", files)

for f in files:
    path = os.path.join(base, f)
    print(f"\n--- LENDO: {f} ---")
    try:
        df = pd.read_excel(path) if f.endswith('xlsx') else pd.read_csv(path)
        print("Colunas:", df.columns.tolist())
        
        # Mostra os 3 primeiros e Sergipe para vermos o valor
        print(df[df['UF'].isin(['SP', 'SE', 'DF'])])
    except:
        print("Erro ao ler.")