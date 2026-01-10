import pandas as pd
import os
import glob

# Configuração
BASE_PATH = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
REPORT_DIR = os.path.join(BASE_PATH, 'reports', 'varcog', 'xlsx')

def audit_files():
    print(f"=== AUDITORIA DE INTEGRIDADE SAEB ===")
    print(f"Diretório: {REPORT_DIR}\n")
    
    files = glob.glob(os.path.join(REPORT_DIR, "saeb_table_*.xlsx"))
    
    if not files:
        print("[ALERTA] Nenhum arquivo encontrado!")
        return

    issues_found = 0

    for filepath in sorted(files):
        filename = os.path.basename(filepath)
        try:
            df = pd.read_excel(filepath)
            
            # 1. Checagem de Dimensões (Esperado: 27 UFs)
            row_count = len(df)
            status_rows = "OK" if row_count == 27 else f"ALERTA ({row_count} UFs)"
            
            # 2. Checagem de Valores Nulos
            nulls = df.isnull().sum().sum()
            status_nulls = "OK" if nulls == 0 else f"FALHA ({nulls} nulos)"
            
            # 3. Checagem de Range de Notas (Validar se não houve erro de decimal)
            # Notas do SAEB raramente fogem de 100 a 450
            min_score = df['SAEB_General'].min()
            max_score = df['SAEB_General'].max()
            status_score = "OK"
            if min_score < 100 or max_score > 500:
                status_score = f"SUSPEITO (Min {min_score:.0f} / Max {max_score:.0f})"
            
            # 4. Checagem do Public Share
            p_share_mean = df['Public_Share'].mean()
            if pd.isna(p_share_mean):
                status_share = "CRÍTICO (Vazio)"
            elif p_share_mean < 0 or p_share_mean > 1:
                status_share = "ERRO LÓGICO (>1 ou <0)"
            else:
                status_share = f"OK (Média {p_share_mean:.2f})"

            # Relatório da Linha
            print(f"📄 {filename}")
            print(f"   ├─ UFs......: {status_rows}")
            print(f"   ├─ Scores...: {status_score}")
            print(f"   └─ P. Share.: {status_share}")
            
            if "ALERTA" in status_rows or "FALHA" in status_nulls or "SUSPEITO" in status_score or "CRÍTICO" in status_share:
                issues_found += 1
                print("   ⚠️  ATENÇÃO NECESSÁRIA AQUI")

        except Exception as e:
            print(f"❌ Erro ao ler {filename}: {e}")

    print("\n" + "="*40)
    if issues_found == 0:
        print("✅ SUCESSO: Todos os arquivos passaram no Health Check.")
    else:
        print(f"⚠️  AVISO: Foram encontradas {issues_found} anomalias potenciais.")

if __name__ == "__main__":
    audit_files()