# logging_config.py
import logging
import logging.handlers

def setup_logging():
    logger = logging.getLogger()
    
    # Si ya tiene handlers, no agregar más
    if logger.handlers:
        return logger
    
    logger.setLevel(logging.INFO)

    # Formato
    formatter = logging.Formatter("[%(levelname)s][%(asctime)s] - %(message)s")

    # Handler para archivo
    file_handler = logging.FileHandler("etl_requests.log", mode="a", encoding="utf-8-sig")
    file_handler.setFormatter(formatter)

    # Handler para consola
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    # Agregar handlers al logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger