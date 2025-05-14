import pandas as pd
import aiohttp
import asyncio
from typing import List, Dict
from pathlib import Path

from etr.common.logger import LoggerFactory
from etr.config import Config

class Bgeometric:
    url = "https://bitcoin-data.com/v1"
    
    def __init__(self):
        self.logger = LoggerFactory().get_logger(__name__)

    @staticmethod
    def list_channels():
        return [
            "nupl",
            "sopr",
            "open-interest-futures",
            "etf-flow-btc",
            "etf-btc-total",
            "nrpl-usd",
            "sth-sopr",
            "lth-sopr",
            "mvrv",
            "lth-mvrv",
            "sth-mvrv",
        ]
    
    async def get_historical(
        self,
        channel: str,
        start_date: str,
        end_date: str,
    ) -> pd.DataFrame:
        # https://bitcoin-data.com/v1/nupl?startday=%222024-04-01%22&endday=%222024-04-10%22
        ymd_s = pd.Timestamp(start_date).strftime("%Y-%m-%d")
        ymd_e = pd.Timestamp(end_date).strftime("%Y-%m-%d")
        params = "%22".join(["startday=", ymd_s, "&endday=", ymd_e]) + "%22"
        url = f"{self.url}/{channel}?{params}"
        async with aiohttp.ClientSession() as session:
            self.logger.info(f"Send request to {url}")
            try:
                async with session.get(url) as response:
                    response.raise_for_status()  # error check
                    data = await response.json()  # read json
                    data = self._cleansing(data)
                    return data
            except aiohttp.ClientError as e:
                self.logger.error(f"{e}", exc_info=True)

    @staticmethod
    def _cleansing(records: List[Dict]) -> pd.DataFrame:
        df = pd.DataFrame(records)
        if len(df) > 0:
            timestamp = pd.to_datetime(df.iloc[:, 0]).to_frame("date")
            contents = df.loc[:, (df.columns == ("unixTs")).cumsum().astype(bool)].iloc[:, 1:]
            cleansed = timestamp.join(contents.astype(float))
            return cleansed
        return pd.DataFrame()


if __name__ == "__main__":
    
    path = Path(Config.HDB_DIR).parent.joinpath("bgeometric")
    path.mkdir(exist_ok=True, parents=True)
    df = asyncio.run(Bgeometric().get_historical("nupl", "2018-01-01", pd.Timestamp.today()))
    print(df.tail())
