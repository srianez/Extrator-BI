import logging
import os

class Logger:
    @staticmethod
    def configure(log_directory, log_file):
        Logger.log_directory = log_directory
        Logger.log_file = log_file
        Logger.log_path = os.path.join(Logger.log_directory, Logger.log_file)
        
        # Verifica se o diretório existe, se não, cria-o
        if not os.path.exists(Logger.log_directory):
            os.makedirs(Logger.log_directory)

        # Configuração básica do logging
        logging.basicConfig(
            filename=Logger.log_path,
            filemode='w',  # Abrir arquivo em modo de escrita para sobrescrever o conteúdo existente
            level=logging.INFO,  # Define o nível de log (DEBUG, INFO, WARNING, ERROR, CRITICAL)
            format='%(asctime)s - %(levelname)s - %(message)s',  # Formato da mensagem de log
            datefmt='%Y-%m-%d %H:%M:%S'  # Formato da data
        )
        Logger.logger = logging.getLogger()

    @staticmethod
    def log_info(message):
        Logger.logger.info(message)

    @staticmethod
    def log_error(message):
        Logger.logger.error(message)
