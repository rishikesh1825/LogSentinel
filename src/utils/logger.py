"""Central logging configuration for LogSentinel.

Every module gets its logger via ``get_logger(__name__)`` so that all
components log in one consistent format to the console and to a
rotating file under ``logs/``.
"""

import logging
import os
from logging.handlers import RotatingFileHandler

LOG_DIR = os.getenv("LOG_DIR", "logs")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"


def get_logger(name: str) -> logging.Logger:
    """Return a configured logger.

    Args:
        name: Logger name, normally the calling module's ``__name__``.

    Returns:
        A logger writing to stdout and to ``logs/logsentinel.log``
        (rotated at 5 MB, 3 backups kept).
    """
    logger = logging.getLogger(name)
    if logger.handlers:  # already configured; avoid duplicate output
        return logger

    logger.setLevel(LOG_LEVEL)
    formatter = logging.Formatter(LOG_FORMAT)

    console = logging.StreamHandler()
    console.setFormatter(formatter)
    logger.addHandler(console)

    os.makedirs(LOG_DIR, exist_ok=True)
    file_handler = RotatingFileHandler(
        os.path.join(LOG_DIR, "logsentinel.log"), maxBytes=5_000_000, backupCount=3
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    logger.propagate = False
    return logger
