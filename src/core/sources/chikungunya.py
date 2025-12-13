import pandas as pd
from src.interfaces.source import IDataSource
from src.utils.loaders import FileLoader

class ChikungunyaSource(IDataSource):
    def get_name(self) -> str:
        return "Notificações de Chikungunya (SINAN)"

    def read(self, file_path: str) -> pd.DataFrame:
        if not file_path.lower().endswith('.dbf'):
             raise ValueError("Fonte de Chikungunya requer arquivo .dbf")

        print(f" -> [ChikungunyaSource] Carregando DBF: {file_path}")
        df = FileLoader.load_dbf(file_path)
        
        # --- REGRA DE NEGÓCIO ESPECÍFICA ---
        # Exemplo: Renomear colunas antigas para novas
        # df.rename(columns={'DT_NOTIFIC': 'DATA_NOTIFICACAO'}, inplace=True)
        
        return df
