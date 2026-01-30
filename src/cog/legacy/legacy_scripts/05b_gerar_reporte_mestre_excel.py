import pandas as pd
import os

# --- configuração de caminhos ---
script_dir = os.path.dirname(os.path.abspath(__file__))
base_dir = os.path.dirname(script_dir)
input_dir = os.path.join(base_dir, 'analise_exploratoria')
output_file = os.path.join(input_dir, 'reporte_mestre_capital_cognitivo.xlsx')

# arquivos gerados pelos scripts anteriores
fontes = {
    'tabela_consolidada_estados_trienio.xlsx': 'enem_consolidado_2022_2024',
    'analise_estabilidade_kendall_completa.xlsx': 'estabilidade_kendall_w',
    'dados_pisa_historico_estados.xlsx': 'pisa_historico_2018_2022',
    'resultado_estatistico_correlacao.xlsx': 'validade_convergente'
}

def gerar_informe_excel():
    print("📂 gerando o informe executivo em excel...")
    
    if not os.path.exists(input_dir):
        print(f"❌ erro: pasta {input_dir} não encontrada.")
        return

    try:
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            # 1. criar aba de sumário executivo com os marcos da pesquisa
            sumario_data = {
                'indicador': [
                    'abismo máximo identificado (enem)',
                    'coeficiente w de kendall (estabilidade)',
                    'correlação média (enem vs pisa)',
                    'correlação longitudinal (2024 vs 2018)',
                    'veredito técnico'
                ],
                'valor': [
                    'sessenta e quatro vírgula doze pontos',
                    '0.9817',
                    '0.9393',
                    '0.9601',
                    'rigidez estrutural do capital cognitivo'
                ]
            }
            pd.DataFrame(sumario_data).to_excel(writer, sheet_name='sumario_executivo', index=False)

            # 2. integrar as outras abas
            for arquivo, nome_aba in fontes.items():
                caminho = os.path.join(input_dir, arquivo)
                if os.path.exists(caminho):
                    # trata o arquivo de kendall que tem múltiplas abas
                    if 'kendall' in arquivo:
                        df = pd.read_excel(caminho, sheet_name='rankings_e_estabilidade')
                    else:
                        df = pd.read_excel(caminho)
                    
                    df.to_excel(writer, sheet_name=nome_aba, index=False)
                    
                    # formatação básica de largura
                    worksheet = writer.sheets[nome_aba]
                    for col in worksheet.columns:
                        max_len = max([len(str(cell.value)) for cell in col])
                        worksheet.column_dimensions[col[0].column_letter].width = max_len + 2
                    
                    print(f"✅ aba '{nome_aba}' integrada.")
                else:
                    print(f"⚠️ aviso: {arquivo} não encontrado.")

        print("-" * 30)
        print(f"✨ informe gerado: {output_file}")
        print("-" * 30)

    except Exception as e:
        print(f"❌ erro na geração do excel: {e}")

if __name__ == "__main__":
    gerar_informe_excel()