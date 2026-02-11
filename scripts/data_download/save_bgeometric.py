import pandas as pd
import asyncio
from pathlib import Path

from etr.data.bgeometric import Bgeometric
from etr.config import Config


if __name__ == "__main__":
    
    api = Bgeometric()
    path = Path(Config.HDB_DIR).parent.joinpath("bgeometric")
    path.mkdir(exist_ok=True, parents=True)
    for channel in Bgeometric.list_channels():
        df = asyncio.run(api.get_historical(channel, "2018-01-01", pd.Timestamp.today()))
        df.to_parquet(path.joinpath(channel + ".parquet"))
        api.logger.info(f"Saved '{channel}.parquet'")
