import pandas as pd
from dbfread import DBF
import os

class FileLoader:
    """
    Classe utilitária para leitura bruta de arquivos.
    Não contém regra de negócio, apenas I/O.
    """

    @staticmethod
    def load_csv(file_path: str, **kwargs) -> pd.DataFrame:
        try:
            return pd.read_csv(file_path, **kwargs)
        except Exception as e:
            raise Exception(f"Erro de I/O CSV: {e}")

    @staticmethod
    def load_dbf(file_path: str) -> pd.DataFrame:
        from tqdm import tqdm
        try:
            # Primeiro, abrimos sem carregar para contar registros (rápido em dbfread)
            # Tenta com latin-1
            encoding = 'latin-1'
            try:
                 table = DBF(file_path, load=False, encoding=encoding)
            except:
                 encoding = 'cp1252'
                 table = DBF(file_path, load=False, encoding=encoding)

            record_count = len(table)
            print(f" -> Lendo {record_count} registros do arquivo DBF...")

            # Agora iteramos com barra de progresso
            data = []
            # Recriamos o objeto DBF para iterar do zero
            table_iter = DBF(file_path, load=False, encoding=encoding)
            
            with tqdm(total=record_count, unit="rows", desc=f"Lendo {os.path.basename(file_path)}") as pbar:
                for record in table_iter:
                    data.append(record)
                    pbar.update(1)
            
            return pd.DataFrame(data)

        except Exception as e:
            raise Exception(f"Erro de I/O DBF: {e}")