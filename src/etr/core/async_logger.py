import logging
from logging.handlers import TimedRotatingFileHandler, QueueHandler, QueueListener
import asyncio
import queue
import os
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor


class AsyncBufferedLogger:
    """
        Buffered & multi-threaded logger for websocket message logging.
    """
    
    executor = ThreadPoolExecutor(max_workers=2)

    def __init__(
        self,
        logger_name: str,
        log_dir="logs"
    ):
        Path(log_dir).mkdir(parents=True, exist_ok=True)

        self.log_queue = queue.Queue(-1)

        # Logger + RotatingHandler
        formatter = logging.Formatter('%(asctime)s.%(msecs)-03d||%(message)s', datefmt="%Y-%m-%d %H:%M:%S")
        file_handler = TimedRotatingFileHandler(
            filename=os.path.join(log_dir, f"{logger_name}.log"),
            utc=True,
            when='midnight',
            backupCount=100,
            encoding='utf-8',
        )
        file_handler.setFormatter(formatter)
        self.logger = logging.getLogger(logger_name)
        self.logger.setLevel(logging.INFO)
        self.logger.handlers = []
        self.logger.addHandler(QueueHandler(self.log_queue))

        # Thread for log writing
        self.listener = QueueListener(self.log_queue, file_handler)
        self.listener.start()

    async def info(self, msg: str):
        await asyncio.get_running_loop().run_in_executor(AsyncBufferedLogger.executor, self.logger.info, msg)

    async def warning(self, msg: str):
        await asyncio.get_running_loop().run_in_executor(AsyncBufferedLogger.executor, self.logger.warning, msg)

    async def error(self, msg: str):
        await asyncio.get_running_loop().run_in_executor(AsyncBufferedLogger.executor, self.logger.error, msg)

    def stop(self):
        self.listener.stop()
        # AsyncBufferedLogger.executor.shutdown()
