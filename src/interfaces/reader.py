from abc import ABC, abstractmethod
import pandas as pd

class IDataReader(ABC):
    """
    Interface base para leitores de dados.
    Qualquer classe que ler dados deve implementar esta interface.
    """
    
    @abstractmethod
    def read(self, file_path: str) -> pd.DataFrame:
        """
        Lê um arquivo e retorna um DataFrame.
        Deve ser implementado pelas classes filhas.
        """
        pass
