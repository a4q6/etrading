import pandas as pd
import asyncio
from pathlib import Path
from typing import Callable, Awaitable, List

from etr.config import Config
from etr.common.logger import LoggerFactory


class CEP:
    def __init__(self, logger_name, log_file=None, *args, **kwargs):
        if log_file is not None:
            log_file = Path(Config.LOG_DIR).joinpath(log_file).as_posix()
        self.logger = LoggerFactory().get_logger(logger_name=logger_name, log_file=log_file)

    def start_heartbeat(
        interval: int = 10,
        callbacks: List[Callable[[dict], Awaitable[None]]] = [],
    ) -> asyncio.Task:
        async def _heartbeat_loop():
            while True:
                now = pd.Timestamp.today(tz="UTC")
                msg = {
                    "_data_type": "Heartbeat",
                    "timestamp": now.to_pydatetime(),
                }
                for cb in callbacks:
                    await cb(msg)
                next_time = now.ceil(f"{interval}s")
                sleep_duration = (next_time - now).total_seconds()
                await asyncio.sleep(sleep_duration)

        return asyncio.create_task(_heartbeat_loop())
