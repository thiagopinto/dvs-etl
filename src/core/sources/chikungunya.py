import pandas as pd
from src.interfaces.source import IDataSource
from src.utils.loaders import FileLoader

class ChikungunyaSource(IDataSource):
    COLUMNS_TO_KEEP = [
        "NU_NOTIFIC", "ID_AGRAVO", "DT_NOTIFIC", "SEM_NOT", "NU_ANO", 
        "ID_MUNICIP", "ID_UNIDADE", "DT_SIN_PRI", "SEM_PRI", "DT_NASC", 
        "NU_IDADE_N", "CS_SEXO", "CS_RACA", "ID_MN_RESI", "NM_BAIRRO", 
        "CS_ZONA", "RESUL_SORO", "RESUL_NS1", "RESUL_VI_N", "RESUL_PCR_", 
        "SOROTIPO", "HISTOPA_N", "IMUNOH_N", "HOSPITALIZ", "DT_INTERNA", 
        "TPAUTOCTO", "CLASSI_FIN", "CRITERIO", "EVOLUCAO", "DT_OBITO", 
        "DT_ENCERRA"
    ]

    def get_name(self) -> str:
        return "Notificações de Chikungunya (SINAN)"

    def _transform_dates(self, df: pd.DataFrame) -> pd.DataFrame:
        date_cols = [
            "DT_NOTIFIC", "DT_SIN_PRI", "DT_NASC", 
            "DT_INTERNA", "DT_OBITO", "DT_ENCERRA"
        ]
        for col in date_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
        return df

    def _transform_numerics(self, df: pd.DataFrame) -> pd.DataFrame:
        numeric_cols = [
            "SEM_NOT", "SEM_PRI", "NU_ANO", "CS_RACA", "CS_ZONA", 
            "RESUL_SORO", "RESUL_NS1", "RESUL_VI_N", "RESUL_PCR_", 
            "SOROTIPO", "HISTOPA_N", "IMUNOH_N", "HOSPITALIZ", 
            "TPAUTOCTO", "CLASSI_FIN", "CRITERIO", "EVOLUCAO"
        ]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
        return df

    def _filter_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        available_cols = [c for c in self.COLUMNS_TO_KEEP if c in df.columns]
        return df[available_cols].copy()

    def read(self, file_path: str) -> pd.DataFrame:
        if not file_path.lower().endswith('.dbf'):
             raise ValueError("Fonte de Chikungunya requer arquivo .dbf")

        print(f" -> [ChikungunyaSource] Carregando DBF: {file_path}")
        df = FileLoader.load_dbf(file_path)
        
        df = self._transform_dates(df)
        df = self._transform_numerics(df)
        df = self._filter_columns(df)
        
        # Deduplicar registros para evitar erro no UPSERT
        pk_cols = ["ID_AGRAVO", "NU_NOTIFIC", "NU_ANO"]
        if all(col in df.columns for col in pk_cols):
            original_len = len(df)
            df.drop_duplicates(subset=pk_cols, keep='last', inplace=True)
            if len(df) < original_len:
                print(f" -> [Aviso] {original_len - len(df)} registros duplicados removidos (mantido o último).")
                
        return df
