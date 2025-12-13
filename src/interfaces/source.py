from abc import ABC, abstractmethod
import pandas as pd

class IDataSource(ABC):
    """
    Interface para fontes de dados específicas (Dengue, Chikungunya, etc).
    """

    @abstractmethod
    def get_name(self) -> str:
        """Retorna o nome amigável da fonte."""
        pass

    @abstractmethod
    def read(self, file_path: str) -> pd.DataFrame:
        """
        Lê o arquivo, aplica validações específicas e retorna o DataFrame.
        """
        pass
