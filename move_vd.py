"""
SCRIPT: move_vds.py
DESC:   Move pipelines cognitivos de src/cog para src/ind e renomeia
        conforme a Ficha Mestre (VD-Âncora, VD-Nacional, VD-Capilaridade).
"""
import os
import shutil
from pathlib import Path

BASE_DIR = Path.cwd()

# Definição das Migrações
MOVES = [
    {
        "nome_antigo": "src/cog/cog_01_process_unified_pisa_pipeline.py",
        "novo_nome":   "src/ind/vd_ancora_extract_pisa.py",
        "desc":        "VD-Âncora (PISA)"
    },
    {
        "nome_antigo": "src/cog/cog_02_process_unified_enem_pypeline.py",
        "novo_nome":   "src/ind/vd_nacional_extract_enem.py",
        "desc":        "VD-Nacional (ENEM)"
    },
    {
        "nome_antigo": "src/cog/cog_03_process_unified_saeb_pypeline.py",
        "novo_nome":   "src/ind/vd_capilaridade_extract_saeb.py",
        "desc":        "VD-Capilaridade (SAEB)"
    }
]

def main():
    print("=== MIGRANDO VARIÁVEIS DEPENDENTES (COG -> IND) ===")
    
    count = 0
    for item in MOVES:
        src = BASE_DIR / item["nome_antigo"]
        dst = BASE_DIR / item["novo_nome"]
        
        # Verifica se a origem existe
        if src.exists():
            try:
                # Se o destino já existe, avisa e sobrescreve
                if dst.exists():
                    print(f"   [!] Sobrescrevendo destino existente: {item['novo_nome']}")
                    os.remove(dst)
                
                shutil.move(str(src), str(dst))
                print(f"✅ [SUCESSO] {item['desc']}")
                print(f"   De: {item['nome_antigo']}")
                print(f"   Para: {item['novo_nome']}")
                count += 1
            except Exception as e:
                print(f"❌ Erro ao mover {item['desc']}: {e}")
        
        # Caso o arquivo já tenha sido movido ou renomeado antes
        elif dst.exists():
            print(f"OK Já migrado: {item['desc']} está em src/ind.")
        else:
            print(f"⚠️  Arquivo original não encontrado: {item['nome_antigo']}")

    print(f"\n[FIM] {count} scripts migrados.")
    print("Agora todos os Extratores (VD e VI) estão unificados em 'src/ind'.")

if __name__ == "__main__":
    main()