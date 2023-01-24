import logging

class Logger:
    configure = False
    
    @staticmethod
    def log(message : str):
        if not Logger.configure:
            logging.root.setLevel(logging.INFO)
            logging.basicConfig(format="[ %(asctime)s ] %(message)s", datefmt="%d/%m/%Y %I:%M:%S %p", handlers=[logging.StreamHandler()])
            Logger.configure = True
            
        logging.info(message)