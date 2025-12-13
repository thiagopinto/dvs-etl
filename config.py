import os
from dotenv import load_dotenv

# Carrega as variáveis do arquivo .env
load_dotenv()

class Config:
    # Define o diretório de entrada. Se não existir no .env, usa './data/input' como fallback
    DATA_INPUT_DIR = os.getenv('DATA_INPUT_DIR', './data/input')

# Cria o diretório se não existir
if not os.path.exists(Config.DATA_INPUT_DIR):
    os.makedirs(Config.DATA_INPUT_DIR)
