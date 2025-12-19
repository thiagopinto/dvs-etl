import argparse
import sys
import os
from config import Config
from src.core.scanner import FileScanner
from src.core.factory import SourceFactory
from src.utils.loaders import FileLoader # Usado apenas para leitura genérica manual
from src.utils.database import Database # Novo import

def process_source(file_path, prefix_or_label):
    """
    Processa usando a lógica de negócio específica via Factory.
    """
    try:
        # Tenta obter uma fonte específica pelo prefixo
        source = SourceFactory.get_source_by_prefix(prefix_or_label)
        print(f"\n--- Processando {source.get_name()} ---")
        
        df = source.read(file_path)
        
        print("Status: Sucesso (Validado pela Classe Específica)")
        print(f"Dimensões: {df.shape[0]} registros, {df.shape[1]} colunas")
        if not df.empty:
            print("Preview:")
            print(df.head(3))

            # Carregar dados no banco de dados se for Dengue
            if source.get_name() == "Notificações de Dengue (SINAN)":
                Database.upsert_dataframe(df, "dengue_completo", pk_columns=["ID_AGRAVO", "NU_NOTIFIC", "NU_ANO"])
            elif source.get_name() == "Notificações de Chikungunya (SINAN)":
                Database.upsert_dataframe(df, "chik_completo", pk_columns=["ID_AGRAVO", "NU_NOTIFIC", "NU_ANO"])

    except ValueError:
        # Se não achou fonte específica na Factory, cai aqui (modo manual genérico)
        print(f"\n--- Leitura Genérica: {os.path.basename(file_path)} ---")
        try:
            if file_path.lower().endswith('.csv'):
                df = FileLoader.load_csv(file_path)
            else:
                df = FileLoader.load_dbf(file_path)
            
            print("Status: Sucesso (Leitura Genérica)")
            print(f"Dimensões: {df.shape[0]} registros, {df.shape[1]} colunas")
        except Exception as e:
            print(f"ERRO Genérico: {e}")
            
    except Exception as e:
        print(f"ERRO no processamento: {e}")


def run_auto_mode():
    """Busca automática DENGON e CHIKON."""
    scanner = FileScanner(Config.DATA_INPUT_DIR)
    
    # Mapeamento Prefixo -> Rótulo (apenas para log se não achar)
    targets = ['DENGON', 'CHIKON']
    
    print(f"Iniciando varredura em: {Config.DATA_INPUT_DIR}")

    # Verifica se o diretório de entrada está vazio
    if not os.listdir(Config.DATA_INPUT_DIR):
        print(f"\nAviso: O diretório de entrada '{Config.DATA_INPUT_DIR}' está vazio.")
        print("Por favor, coloque os arquivos .dbf ou .csv a serem processados aqui.")
        return # Sai da função se o diretório estiver vazio
    
    found_any = False
    for prefix in targets:
        latest_file = scanner.get_latest_file(prefix)
        
        if latest_file:
            # Passa o prefixo para a Factory decidir qual classe usar
            process_source(latest_file, prefix)
            found_any = True
        else:
            print(f"\nAviso: Nenhum arquivo encontrado para prefixo '{prefix}'")

    if not found_any:
        print("\nNenhum arquivo válido encontrado.")

def main():
    parser = argparse.ArgumentParser(description="ETL DVS - CLI")
    subparsers = parser.add_subparsers(dest='command', help='Comandos')
    
    # Comando AUTO
    subparsers.add_parser('auto', help='Processa automaticamente Dengue/Chikungunya')

    # Comando READ
    parser_read = subparsers.add_parser('read', help='Lê arquivo manual')
    parser_read.add_argument('filename', help='Nome do arquivo')

    args = parser.parse_args()

    if args.command == 'auto':
        run_auto_mode()
    elif args.command == 'read':
        target = args.filename if os.path.exists(args.filename) else os.path.join(Config.DATA_INPUT_DIR, args.filename)
        # No modo manual, não sabemos o prefixo, passamos 'UNKNOWN' para cair no genérico
        # Ou poderíamos tentar deduzir o prefixo pelo nome do arquivo aqui.
        prefix = os.path.basename(target)[:6] # Tenta pegar os 6 primeiros caracteres
        process_source(target, prefix)
    else:
        parser.print_help()

if __name__ == "__main__":
    main()