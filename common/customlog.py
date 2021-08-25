import sys
import logging
from functools import partial
import config

LOG_FORMAT = "[{asctime}-{levelname}-{thread}-{classname}-{filename}-{lineno}] {message}"


class CustomFormatter(logging.Formatter):
    def __init__(self, fmt=None, datefmt=None, style='{', validate=True):
        super().__init__(fmt, datefmt, style, validate)

    def formatMessage(self, record):
        return self._fmt.format_map(CustomFormatter.Default(record.__dict__))

    class Default(dict):
        def __missing__(self, key):
            return '{' + key + '}'


def default_logger(logger_name='customLogger'):
    logger = logging.getLogger(logger_name)
    logger.setLevel(config.LOG_LEVEL)
    if not logger.hasHandlers():
        console_handler = logging.StreamHandler()
        logger.addHandler(console_handler)
    for handler in logger.handlers:
        handler.setFormatter(CustomFormatter(LOG_FORMAT))

    return logger


class CustomLogger(object):
    def __init__(self, logger=default_logger(), extra={}):
        self.logger = logger
        self.extra = extra

    def __getattr__(self, name):
        return partial(getattr(self.logger, name), extra=self.extra)