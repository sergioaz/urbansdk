import logging
from logging.config import dictConfig

from app.core.config import config, DevConfig


def configure_logging() -> None:
    dictConfig({
        "version" : 1,
        "disable_existing_loggers" : False,
        "formatters" : {
            "console" : {
                "class" : "logging.Formatter",
                "datefmt" : "%Y-%m-%d %H:%M:%S",
                "format": "%(name)s: %(levelname)s - %(message)s"
            },
            "file": {
                "class": "logging.Formatter",
                "datefmt": "%Y-%m-%d %H:%M:%S",
                "format": "%(asctime)s %(levelname)-8s %(name)s %(lineno)d %(message)s"
            }
        },
        "handlers" : {
            "default": {
                "class" : "logging.StreamHandler",
                "formatter" : "console",
                "level" : "DEBUG" if isinstance (config, DevConfig) else "INFO"
            },
            "rotating_file": {
                "class": "logging.handlers.RotatingFileHandler",
                "formatter": "file",
                "level": "DEBUG",
                "filename": "urbansdk.log",
                "maxBytes": 1024 * 1024 * 5,                    # 5 MB
                "backupCount": 2,
                "encoding": "utf8"
            }
        },
        "loggers" : {
            "uvicorn" : { "handlers" : ["default", "rotating_file"],"level" : "INFO"},
            "databases" : { "handlers" : ["default"],"level" : "WARNING"},
            "aiosqlite": {"handlers": ["default"], "level": "WARNING"},
            "root" : {
            "handlers" : ["default", "rotating_file"],
            "level" : "DEBUG" if isinstance (config, DevConfig) else "INFO",
            "propagate" : False
        }},
    })
