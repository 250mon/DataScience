import sys
import logging
import logging.config
import yaml


def setup_logger():
    with open('log_config.yaml', 'rt') as f:
        config = yaml.load(f, Loader=yaml.FullLoader)
        logging.config.dictConfig(config)
    sys.excepthook = handle_exception

def handle_exception(exc_type, exc_value, exc_traceback):
    err_logger = logging.getLogger("main")
    err_logger.error("Unexpected exception",
                     exc_info=(exc_type, exc_value, exc_traceback))
