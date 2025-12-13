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
        try:
            # Tenta ler com a codificação padrão do DBF (geralmente 'latin-1' ou 'cp1252')
            # O 'encoding' pode ser necessário para caracteres especiais/acentuados.
            # dbfread tentará inferir, mas podemos forçar para compatibilidade.
            table = DBF(file_path, load=True, encoding='latin-1')
            return pd.DataFrame(iter(table))
        except Exception as e:
            # Se latin-1 falhar, podemos tentar outra comum como 'cp1252'
            try:
                table = DBF(file_path, load=True, encoding='cp1252')
                return pd.DataFrame(iter(table))
            except Exception as e2:
                # Se ambas falharem, reporta o erro original
                raise Exception(f"Erro de I/O DBF: {e} (tentativa latin-1), {e2} (tentativa cp1252)")