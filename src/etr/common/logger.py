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
        Normal logger factory class, which is intended to be used in ordinal classes.
    """
    _instances = {}

    def __init__(self, logfile: str = None, loglevel=logging.INFO):
        """ Factory class for logger. 
            yourlogger = LoggerFactory(log_filepath).getLogger(logger_name)
        Args:
            logfile (str, optional): path to generate *.log file. Defaults to None.
            loglevel (int, optional): Defaults to logging.INFO.
        """
        self.loglevel = loglevel
        if logfile is not None:
            self.logfile = Path(logfile)
            self.logfile.parent.mkdir(parents=True, exist_ok=True)
        else:
            self.logfile = None

    def getLogger(self, logger_name: str) -> logging.Logger:
        
        if logger_name in LoggerFactory._instances.keys():
            return LoggerFactory._instances[logger_name]
        else:
            fmt = '%(asctime)s.%(msecs)-03d|%(levelname)-8s|%(name)s: %(module)s.%(funcName)s %(lineno)d|%(message)s'
            formatter = logging.Formatter(fmt, datefmt="%Y-%m-%d %H:%M:%S")
            logger = logging.getLogger(logger_name)

            # add stream Handler
            handler = logging.StreamHandler()
            handler.setFormatter(formatter)
            logger.addHandler(handler)

            # add file Handler
            if self.logfile is not None:
                handler = logging.handlers.TimedRotatingFileHandler(filename=self.logfile, when='MIDNIGHT', backupCount=30, utc=True)
                handler.setFormatter(formatter)
                logger.addHandler(handler)
            
            logger.setLevel(self.loglevel)

            # register instance
            LoggerFactory._instances[logger_name] = logger
            return logger
