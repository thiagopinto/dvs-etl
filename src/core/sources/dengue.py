import pandas as pd
from src.interfaces.source import IDataSource
from src.utils.loaders import FileLoader

class DengueSource(IDataSource):
    def get_name(self) -> str:
        return "Notificações de Dengue (SINAN)"

    def _transform_dates(self, df: pd.DataFrame) -> pd.DataFrame:
        date_cols = [
            "DT_NOTIFIC", "DT_SIN_PRI", "DT_NASC", 
            "DT_INTERNA", "DT_OBITO", "DT_ENCERRA"
        ]
        
        for col in date_cols:
            if col in df.columns:
                # Convert to datetime, coercing errors to NaT
                df[col] = pd.to_datetime(df[col], errors='coerce')
        return df

    def _transform_numerics(self, df: pd.DataFrame) -> pd.DataFrame:
        numeric_cols = [
            "SEM_NOT", "SEM_PRI", "NU_ANO", "CS_RACA", "CS_ZONA", 
            "RESUL_SORO", "RESUL_NS1", "RESUL_VI_N", "RESUL_PCR_", 
            "SOROTIPO", "HISTOPA_N", "IMUNOH_N", "HOSPITALIZ", 
            "TPAUTOCTO", "CLASSI_FIN", "CRITERIO", "DOENCA_TRA", 
            "CLINC_CHIK", "EVOLUCAO"
        ]
        
        for col in numeric_cols:
            if col in df.columns:
                # Convert to numeric, coercing errors to NaN, then to nullable Int64
                df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
        return df

    def _calculate_age_ranges(self, idade_anos):
        """
        Calcula a faixa etária e o código baseado na idade em anos.
        Baseado nos exemplos: 2 -> [5, 9], 5 -> [20, 29]
        Inferência de faixas padrão de epidemiologia.
        """
        if pd.isna(idade_anos):
            return None, None
            
        idade = int(idade_anos)
        
        if idade < 1:
            return "[<1]", 0
        elif 1 <= idade <= 4:
            return "[1, 4]", 1
        elif 5 <= idade <= 9:
            return "[5, 9]", 2
        elif 10 <= idade <= 14:
            return "[10, 14]", 3
        elif 15 <= idade <= 19:
            return "[15, 19]", 4
        elif 20 <= idade <= 29:
            return "[20, 29]", 5
        elif 30 <= idade <= 39:
            return "[30, 39]", 6
        elif 40 <= idade <= 49:
            return "[40, 49]", 7
        elif 50 <= idade <= 59:
            return "[50, 59]", 8
        elif 60 <= idade <= 69:
            return "[60, 69]", 9
        elif 70 <= idade <= 79:
            return "[70, 79]", 10
        elif idade >= 80:
            return "[80, >]", 11
            
        return None, None

    def _transform_age(self, df: pd.DataFrame) -> pd.DataFrame:
        if "NU_IDADE_N" not in df.columns:
            return df

        # Garante que NU_IDADE_N seja numérico
        nu_idade_n = pd.to_numeric(df["NU_IDADE_N"], errors='coerce')

        # Extrai o primeiro dígito (Tipo)
        # Ex: 4030 -> 4030 // 1000 = 4
        df["TIPO_IDADE"] = (nu_idade_n // 1000)

        # Extrai o valor (Resto)
        # Ex: 4030 -> 4030 % 1000 = 30
        valor_idade = (nu_idade_n % 1000)

        # Calcula IDADE_2 (Idade em Anos)
        # Se Tipo=4 (Ano), IDADE_2 = Valor
        # Se Tipo=1, 2, 3 (Hora, Dia, Mês), IDADE_2 = 0 (menor de 1 ano)
        # Se Tipo=0 ou NaN, IDADE_2 = NaN
        
        # Inicializa com NaN
        df["IDADE_2"] = pd.NA
        
        # Máscaras
        mask_anos = df["TIPO_IDADE"] == 4
        mask_menor = df["TIPO_IDADE"].isin([1, 2, 3])
        
        df.loc[mask_anos, "IDADE_2"] = valor_idade
        df.loc[mask_menor, "IDADE_2"] = 0
        
        # Converte para float para ser compatível com o schema (float8)
        df["IDADE_2"] = df["IDADE_2"].astype(float)
        
        # Aplica Faixa Etária
        # Como apply é lento, poderíamos usar pd.cut, mas a lógica customizada de tupla requer cuidado.
        # Vamos usar uma iteração simples mapeada ou vetorizada se possível. 
        # Dado o volume (milhares), apply row-wise é aceitável, mas vetorizar é melhor.
        
        # Vamos criar listas para atribuir de uma vez
        faixas = []
        codigos = []
        
        for idade in df["IDADE_2"]:
            faixa, cod = self._calculate_age_ranges(idade)
            faixas.append(faixa)
            codigos.append(cod)
            
        df["FAIXA_ETARIA"] = faixas
        df["COD_FAIXA_ETARIA"] = codigos
        
        # Cast para Int64 no código
        df["COD_FAIXA_ETARIA"] = pd.to_numeric(df["COD_FAIXA_ETARIA"], errors='coerce').astype('Int64')

        return df

    COLUMNS_TO_KEEP = [
        "NU_NOTIFIC", "ID_AGRAVO", "DT_NOTIFIC", "SEM_NOT", "NU_ANO", 
        "ID_MUNICIP", "ID_UNIDADE", "DT_SIN_PRI", "SEM_PRI", "DT_NASC", 
        "NU_IDADE_N", "CS_SEXO", "CS_RACA", "ID_MN_RESI", "NM_BAIRRO", 
        "CS_ZONA", "RESUL_SORO", "RESUL_NS1", "RESUL_VI_N", "RESUL_PCR_", 
        "SOROTIPO", "HISTOPA_N", "IMUNOH_N", "HOSPITALIZ", "DT_INTERNA", 
        "TPAUTOCTO", "CLASSI_FIN", "CRITERIO", "DOENCA_TRA", "CLINC_CHIK", 
        "EVOLUCAO", "DT_OBITO", "DT_ENCERRA", "IDADE_2", "TIPO_IDADE", 
        "FAIXA_ETARIA", "COD_FAIXA_ETARIA"
    ]

    def _filter_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        # Mantém apenas as colunas que existem no DF e estão na nossa lista de interesse
        available_cols = [c for c in self.COLUMNS_TO_KEEP if c in df.columns]
        return df[available_cols].copy()

    def read(self, file_path: str) -> pd.DataFrame:
        # Dengue geralmente vem em DBF
        # Aqui poderíamos validar se é DBF mesmo
        if not file_path.lower().endswith('.dbf'):
             raise ValueError("Fonte de Dengue requer arquivo .dbf")

        print(f" -> [DengueSource] Carregando DBF: {file_path}")
        df = FileLoader.load_dbf(file_path)
        
        # Aplicar transformações de data
        df = self._transform_dates(df)

        # Aplicar transformações numéricas
        df = self._transform_numerics(df)
        
        # Aplicar transformações de idade
        df = self._transform_age(df)

        # Filtrar apenas colunas necessárias
        df = self._filter_columns(df)
        
        # Deduplicar registros para evitar erro no UPSERT
        # Mantemos a última ocorrência (presumindo ser a mais atualizada no arquivo)
        pk_cols = ["ID_AGRAVO", "NU_NOTIFIC", "NU_ANO"]
        if all(col in df.columns for col in pk_cols):
            original_len = len(df)
            df.drop_duplicates(subset=pk_cols, keep='last', inplace=True)
            if len(df) < original_len:
                print(f" -> [Aviso] {original_len - len(df)} registros duplicados removidos (mantido o último).")
        
        return df
