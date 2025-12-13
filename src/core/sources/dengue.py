import pandas as pd
from src.interfaces.source import IDataSource
from src.utils.loaders import FileLoader

class DengueSource(IDataSource):
    def get_name(self) -> str:
        return "Notificações de Dengue (SINAN)"

    def read(self, file_path: str) -> pd.DataFrame:
        # Dengue geralmente vem em DBF
        # Aqui poderíamos validar se é DBF mesmo
        if not file_path.lower().endswith('.dbf'):
             raise ValueError("Fonte de Dengue requer arquivo .dbf")

        print(f" -> [DengueSource] Carregando DBF: {file_path}")
        df = FileLoader.load_dbf(file_path)
        
        # --- AQUI ENTRA A REGRA DE NEGÓCIO ESPECÍFICA ---
        # Exemplo: Validar colunas obrigatórias do SINAN
        # required_cols = ['DT_NOTIFIC', 'ID_MUNICIP']
        # if not all(col in df.columns for col in required_cols):
        #     raise ValueError("Arquivo de Dengue inválido: colunas faltando")
        
        return df
