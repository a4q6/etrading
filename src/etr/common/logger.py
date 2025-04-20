import pandas as pd
import numpy as np
import logging
import datetime
from typing import AbstractSet, Dict, List, Sequence, Tuple, Any
from pathlib import Path 
import numpy as np
import logging
import logging.handlers


class LoggerFactory:
    """
        Normal logger factory class, which is intended to be used for process logging.
    """
    _instances = {}

    def get_logger(
        self,
        logger_name: str,
        log_file: str = None,
        log_level = logging.INFO,
    ) -> logging.Logger:
        
        if logger_name in LoggerFactory._instances.keys():
            return LoggerFactory._instances[logger_name]
        else:
            # create logger
            fmt = '%(asctime)s.%(msecs)-03d|%(levelname)-8s|%(name)s: %(module)s.%(funcName)s %(lineno)d|%(message)s'
            formatter = logging.Formatter(fmt, datefmt="%Y-%m-%d %H:%M:%S")
            logger = logging.getLogger(logger_name)

            # add stream Handler
            handler = logging.StreamHandler()
            handler.setFormatter(formatter)
            logger.addHandler(handler)

            # add file Handler
            if log_file is not None:
                log_file = Path(log_file)
                log_file.parent.mkdir(parents=True, exist_ok=True)
                handler = logging.handlers.TimedRotatingFileHandler(filename=log_file, when='MIDNIGHT', backupCount=30, utc=True)
                handler.setFormatter(formatter)
                logger.addHandler(handler)
            
            logger.setLevel(log_level)

            # register instance
            LoggerFactory._instances[logger_name] = logger
            return logger
