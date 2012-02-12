"""Logging configuration"""

import logging
import logging.handlers

if not hasattr(logging, "framework_init_done"):
    logging.framework_init_done = False

LOG_FILENAME = 'pynamo.log'
LOG_LEVEL = logging.DEBUG


def init_logging():
    """Initialize logging"""
    if logging.framework_init_done:
        return
    # Initialize logging
    logger = logging.getLogger('dynamo')
    logger.setLevel(LOG_LEVEL)
    formatstring = ("%(asctime)s|%(levelname)-7s|"
                    "%(filename)15s|%(lineno)3s|%(message)s")
    formatter = logging.Formatter(formatstring)
    file_handler = logging.handlers.RotatingFileHandler(LOG_FILENAME, maxBytes=1000000, backupCount=5)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    logger.info('Logger initialized, input filtering at level %d to file %s',
                LOG_LEVEL, LOG_FILENAME)
    logging.framework_init_done = True
