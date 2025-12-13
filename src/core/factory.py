from src.core.sources.dengue import DengueSource
from src.core.sources.chikungunya import ChikungunyaSource
from src.interfaces.source import IDataSource

class SourceFactory:
    """
    Fabrica a instância correta da fonte de dados baseada no prefixo ou identificador.
    """
    
    @staticmethod
    def get_source_by_prefix(prefix: str) -> IDataSource:
        if prefix == 'DENGON':
            return DengueSource()
        elif prefix == 'CHIKON':
            return ChikungunyaSource()
        else:
            raise ValueError(f"Não há implementação de fonte para o prefixo: {prefix}")
