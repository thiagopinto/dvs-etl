from abc import ABC, abstractmethod

class IFileScanner(ABC):
    """
    Interface base para escaneamento de arquivos.
    """

    @abstractmethod
    def get_latest_file(self, prefix: str, extension: str) -> str | None:
        """
        Busca o arquivo mais recente com base no critério.
        """
        pass
