from abc import ABC, abstractmethod
from typing import Optional

class IFileScanner(ABC):
    """
    Interface base para escaneamento de arquivos.
    """

    @abstractmethod
    def get_latest_file(self, prefix: str, extension: str) -> Optional[str]:
        """
        Busca o arquivo mais recente com base no critério.
        """
        pass
