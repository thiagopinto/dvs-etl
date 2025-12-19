from sqlalchemy import create_engine, text, inspect
import os
from dotenv import load_dotenv

load_dotenv()

class Database:
    _engine = None

    @classmethod
    def get_engine(cls):
        if cls._engine is None:
            db_user = os.getenv("DB_USER")
            db_password = os.getenv("DB_PASSWORD")
            db_host = os.getenv("DB_HOST")
            db_name = os.getenv("DB_NAME")

            if not all([db_user, db_password, db_host, db_name]):
                raise ValueError("Credenciais do banco de dados não configuradas nas variáveis de ambiente.")

            # Formato da string de conexão para PostgreSQL
            DATABASE_URL = f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}/{db_name}"
            cls._engine = create_engine(DATABASE_URL)
        return cls._engine

    @staticmethod
    def load_dataframe(df, table_name, if_exists='append', index=False, chunksize=2000):
        from tqdm import tqdm
        import math
        
        engine = Database.get_engine()
        total_rows = len(df)
        chunks = math.ceil(total_rows / chunksize)
        
        print(f" -> Carregando {total_rows} registros para a tabela '{table_name}' em {chunks} lotes...")
        
        try:
            # Barra de progresso para o upload
            with tqdm(total=total_rows, unit="rows", desc=f"Upload {table_name}") as pbar:
                for i in range(0, total_rows, chunksize):
                    chunk = df.iloc[i : i + chunksize]
                    
                    # Lógica para if_exists:
                    # O primeiro chunk respeita o parâmetro original (ex: 'replace' ou 'append')
                    # Os chunks subsequentes devem ser sempre 'append'
                    current_if_exists = if_exists if i == 0 else 'append'
                    
                    chunk.to_sql(table_name, engine, if_exists=current_if_exists, index=index, schema='public')
                    pbar.update(len(chunk))
                    
            print(f" -> Dados carregados com sucesso na tabela '{table_name}'.")
        except Exception as e:
            print(f" -> ERRO ao carregar dados para a tabela '{table_name}': {e}")
            raise # Re-raise a exceção para notificar o chamador

    @staticmethod
    def upsert_dataframe(df, table_name, pk_columns=["NU_NOTIFIC", "NU_ANO"], chunksize=5000):
        """
        Realiza UPSERT (Insert or Update) utilizando tabela de Staging para alta performance.
        1. Carrega dados para tabela 'staging_{table_name}'.
        2. Aplica PK na tabela destino se necessário.
        3. Executa INSERT ... ON CONFLICT ... DO UPDATE do staging para destino.
        4. Remove staging.
        """
        from tqdm import tqdm
        
        if df.empty:
            print(" -> DataFrame vazio. Nada a processar.")
            return

        engine = Database.get_engine()
        staging_table = f"staging_{table_name}"
        
        print(f" -> Iniciando UPSERT em '{table_name}' via '{staging_table}'...")

        try:
            # 1. Verificar colunas existentes se a tabela já existir
            # Isso garante que não tentaremos inserir colunas novas que não estão no schema físico
            inspector = inspect(engine)
            if inspector.has_table(table_name):
                print(f" -> 1/5 Sincronizando colunas com o schema de '{table_name}'...")
                db_cols = [c['name'] for c in inspector.get_columns(table_name)]
                # Filtra o DF para ter apenas colunas que existem no banco
                df_cols_before = set(df.columns)
                df = df[[c for c in df.columns if c in db_cols]]
                df_cols_after = set(df.columns)
                
                dropped = df_cols_before - df_cols_after
                if dropped:
                    print(f" -> Aviso: Ignorando colunas ausentes no banco: {dropped}")
            else:
                print(f" -> Tabela '{table_name}' não existe. Será criada com o schema do DataFrame.")

            # 2. Carga para Staging (Replace garante limpeza prévia)
            print(f" -> 2/5 Carregando Staging ({len(df)} registros)...")
            Database.load_dataframe(df, staging_table, if_exists='replace', chunksize=chunksize)

            with engine.begin() as conn:
                # 3. Garantir que Tabela Destino exista
                print(" -> 3/5 Verificando/Criando tabela destino...")
                conn.execute(text(f"CREATE TABLE IF NOT EXISTS {table_name} (LIKE {staging_table} INCLUDING ALL)"))
                
                # 3.1 Garantir Primary Key
                pk_str = ", ".join([f'"{c}"' for c in pk_columns])
                try:
                    conn.execute(text(f"ALTER TABLE {table_name} ADD PRIMARY KEY ({pk_str})"))
                    print(f" -> PK ({pk_str}) adicionada.")
                except:
                    pass 

                # 4. Executar o MERGE (UPSERT)
                print(" -> 4/5 Executando Merge (INSERT ... ON CONFLICT)...")
                cols_str = ", ".join([f'"{c}"' for c in df.columns])
                update_cols = [c for c in df.columns if c not in pk_columns]
                set_clause = ", ".join([f'"{c}" = EXCLUDED."{c}"' for c in update_cols])
                
                sql_upsert = f"""
                INSERT INTO {table_name} ({cols_str})
                SELECT {cols_str} FROM {staging_table}
                ON CONFLICT ({pk_str}) 
                DO UPDATE SET {set_clause};
                """
                conn.execute(text(sql_upsert))
                
                # 5. Limpeza
                print(" -> 5/5 Removendo tabela de Staging...")
                conn.execute(text(f"DROP TABLE {staging_table}"))
                
            print(f" -> UPSERT concluído com sucesso em '{table_name}'.")

        except Exception as e:
            print(f" -> ERRO Crítico no UPSERT: {e}")
            # Tenta limpar staging em caso de erro, se possível (nova conexão fora da transação falha)
            try:
                with engine.begin() as clean_conn:
                    clean_conn.execute(text(f"DROP TABLE IF EXISTS {staging_table}"))
            except:
                pass
            raise e