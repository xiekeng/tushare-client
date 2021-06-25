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


def default_logger():
    logger = logging.getLogger('customLogger')
    logger.setLevel(config.LOG_LEVEL)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(CustomFormatter(LOG_FORMAT))
    logger.addHandler(console_handler)

    return logger


class CustomLogger(object):
    def __init__(self, logger=default_logger(), extra={}):
        self.logger = logger
        self.extra = extra

    def __getattr__(self, name):
        return partial(getattr(self.logger, name), extra=self.extra)