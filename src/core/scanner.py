import os
import glob
from src.interfaces.scanner import IFileScanner

class FileScanner(IFileScanner):
    """
    Implementação concreta do Scanner de sistema de arquivos local.
    """
    
    def __init__(self, directory: str):
        self.directory = directory

    def get_latest_file(self, prefix: str, extension: str = ".dbf") -> str | None:
        pattern = os.path.join(self.directory, f"{prefix}*{extension}")
        files = glob.glob(pattern)
        
        if not files:
            return None
        
        # Ordena pela data de modificação
        return max(files, key=os.path.getmtime)
