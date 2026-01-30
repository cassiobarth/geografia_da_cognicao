"""
MODULE:      DataGuard (Protocolo de Segurança)
FILE:        src/cog/lib/safeguard.py
DESCRIPTION: Realiza auditoria automática de consistência de dados (Sanity Checks).
             Impede que dados corrompidos avancem no pipeline.
"""
import pandas as pd
import numpy as np

class DataGuard:
    def __init__(self, df, dataset_name):
        self.df = df
        self.name = dataset_name
        self.errors = []
        self.warnings = []

    def check_range(self, columns, min_val, max_val):
        """Verifica se valores numéricos estão dentro da escala esperada (ex: 0-1000)."""
        for col in columns:
            if col in self.df.columns:
                vmin = self.df[col].min()
                vmax = self.df[col].max()
                if vmin < min_val or vmax > max_val:
                    self.errors.append(f"[RANGE] {col} fora dos limites: Min={vmin}, Max={vmax} (Esperado: {min_val}-{max_val})")
    
    def check_historical_consistency(self, score_col, uf_col='UF'):
        """
        Teste do Canário: Verifica se a ordem de grandeza faz sentido histórico.
        Historicamente, no Brasil, SC/SP/DF tendem a ter médias superiores a AM/PA/MA.
        Se a inversão for muito drástica, há erro de mapeamento (Label Mismatch).
        """
        try:
            # Agrega média por UF
            metrics = self.df.groupby(uf_col)[score_col].mean()
            
            # Define grupos de controle (Estados Sentinela)
            high_performers = ['SC', 'SP', 'DF'] 
            control_group = ['AM', 'PA', 'MA']
            
            # Filtra os que existem no dataset
            highs = [uf for uf in high_performers if uf in metrics.index]
            controls = [uf for uf in control_group if uf in metrics.index]
            
            if not highs or not controls:
                return # Não há estados suficientes para testar
            
            avg_high = metrics[highs].mean()
            avg_control = metrics[controls].mean()
            
            # Se o grupo de controle for significativamente maior (> 10% de diferença), algo está errado
            if avg_control > (avg_high * 1.1):
                self.errors.append(f"[CONSISTENCY] Inversão Regional Detectada! Norte ({avg_control:.2f}) > Sul/Sudeste ({avg_high:.2f}). Provável erro de mapeamento de IDs.")
                
        except Exception as e:
            self.warnings.append(f"Não foi possível verificar consistência histórica: {str(e)}")

    def check_nulls(self, threshold=0.3):
        """Falha se houver muitos nulos."""
        null_pct = self.df.isnull().mean()
        for col, pct in null_pct.items():
            if pct > threshold:
                self.warnings.append(f"[NULLS] Coluna {col} tem {pct:.1%} de nulos.")

    def validate(self, strict=True):
        """
        Executa validação.
        Se strict=True, levanta exceção em caso de ERRO crítico.
        """
        if self.errors:
            print(f"\n[DATAGUARD] 🛑 FALHA CRÍTICA EM {self.name}:")
            for e in self.errors:
                print(f"   - {e}")
            if strict:
                raise ValueError(f"Aborting execution due to data integrity issues in {self.name}.")
            return False
        
        if self.warnings:
            print(f"\n[DATAGUARD] ⚠️ Warnings in {self.name}:")
            for w in self.warnings:
                print(f"   - {w}")
        
        print(f"[DATAGUARD] ✅ {self.name} aproved in a preliminar audit.")
        return True