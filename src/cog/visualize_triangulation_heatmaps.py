import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from pathlib import Path

# Configuração de caminhos
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
INPUT_FILE = PROJECT_ROOT / 'reports' / 'varcog' / 'xlsx' / 'triangulation_waves_consolidated.xlsx'
IMG_DIR = PROJECT_ROOT / 'reports' / 'varcog' / 'graficos'
IMG_DIR.mkdir(parents=True, exist_ok=True)

def generate_heatmaps():
    if not INPUT_FILE.exists():
        print("[ERRO] Arquivo Excel de triangulação não encontrado.")
        return

    # Mapeamento de abas de correlação (Regionais)
    waves = {'2015': '2015_Region_Corr', '2018': '2018_Region_Corr', '2022': '2022_Region_Corr'}

    for year, sheet in waves.items():
        try:
            # Lê a matriz de correlação (index_col=0 para pegar os nomes das provas)
            corr = pd.read_excel(INPUT_FILE, sheet_name=sheet, index_col=0)
            
            plt.figure(figsize=(8, 6))
            sns.heatmap(corr, annot=True, cmap='RdYlGn', vmin=0, vmax=1, center=0.5, fmt=".3f")
            
            plt.title(f"Matriz de Correlação: PISA x ENEM x SAEB ({year})\nNível Regional", fontsize=12)
            plt.tight_layout()
            
            save_path = IMG_DIR / f'triangulation_{year}_heatmap.png'
            plt.savefig(save_path, dpi=300)
            plt.close()
            print(f"[SUCESSO] Gerado: {save_path.name}")
            
        except Exception as e:
            print(f"[AVISO] Não foi possível gerar heatmap para {year}: {e}")

if __name__ == "__main__":
    generate_heatmaps()