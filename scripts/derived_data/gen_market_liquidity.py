import pandas as pd
from pathlib import Path
from tqdm import tqdm
from glob import glob
from etr.data.data_loader import load_data
from etr.preprocessing.book import calc_impact_price
from etr.config import Config

HDB_DIR = Path(Config.HDB_DIR)


def generate_book_features(book):
    sym = book.sym.iloc[0]
    if "JPY" in sym:
        tiers = {
            '1m': 1000000.0,
            '5m': 5000000.0,
            '10m': 10000000.0,
            '20m': 20000000.0,
            '30m': 30000000.0
        }
    elif "USD" in sym:
        tiers = {
            '1m': 10000.0,
            '5m': 50000.0,
            '10m': 100000.0,
            '20m': 200000.0,
            '30m': 300000.0
        }
    else:
        return pd.DataFrame()

    # impact price
    book["mid"] = (book.bids.str[0].str[0] + book.asks.str[0].str[0]) / 2
    book["bid_spread_0"] = (book.bids.str[0].str[0] / book.mid).sub(1).abs() * 1e4
    book["ask_spread_0"] = (book.asks.str[0].str[0] / book.mid).sub(1).abs() * 1e4
    for label, amt in tqdm(tiers.items()):
        bid = calc_impact_price(book.bids.values, target_amount=amt, use_term=True, force_return=True)
        ask = calc_impact_price(book.asks.values, target_amount=amt, use_term=True, force_return=True)
        book[f"bid_spread_{label}"] = pd.Series(bid, index=book.index).div(book.mid.values).sub(1).mul(1e4).abs()
        book[f"ask_spread_{label}"] = pd.Series(ask, index=book.index).div(book.mid.values).sub(1).mul(1e4).abs()
        book[f"spread_imbalance_{label}"] = book[f"ask_spread_{label}"] - book[f"bid_spread_{label}"]

    # amount by level
    book["bid_amount_l1"] = book.bids.str[0].str[1].fillna(0)
    book["ask_amount_l1"] = book.asks.str[0].str[1].fillna(0)
    book["bid_amount_l3"] = sum([book.bids.str[i].str[1].fillna(0) for i in range(3)])
    book["ask_amount_l3"] = sum([book.asks.str[i].str[1].fillna(0) for i in range(3)])
    book["bid_amount_l10"] = sum([book.bids.str[i].str[1].fillna(0) for i in range(10)])
    book["ask_amount_l10"] = sum([book.asks.str[i].str[1].fillna(0) for i in range(10)])

    # price by level
    book["bid_spread_l3"] = book.bids.str[2].str[0].div(book.mid).sub(1).abs().mul(1e4)
    book["ask_spread_l3"] = book.asks.str[2].str[0].div(book.mid).sub(1).abs().mul(1e4)
    book["bid_spread_l10"] = book.bids.str[9].str[0].div(book.mid).sub(1).abs().mul(1e4)
    book["ask_spread_l10"] = book.asks.str[9].str[0].div(book.mid).sub(1).abs().mul(1e4)

    result = book.drop(["bids", "asks"], axis=1)
    return result


if __name__ == "__main__":

    def generate_and_save(date):
        TABLE = "MarketLiquidity"
        BASE = Path(f"{Config.HDB_DIR}/{TABLE}")

        # search sym list
        ymd = date.strftime("%Y-%m-%d")
        files = glob(f"{Config.HDB_DIR}/MarketBook/*/{ymd}/*")
        if len(files) > 0:
            print(f"({ymd}) Found {len(files)} files")
            sym_list = pd.DataFrame(
                pd.Series(files).str.split("MarketBook/").str[-1].str.split("/").values.tolist(),
                columns=["venue", "date", "sym"],
            ).drop("date", axis=1).drop_duplicates()
        else:
            print(f"({ymd}) No source MarketBook files found")
            return

        # run
        for venue, sym in sym_list[["venue", "sym"]].values:
            if "USD" in sym or "JPY" in sym:
                path = BASE.joinpath(venue).joinpath(ymd).joinpath(sym).joinpath(f"{TABLE}_{venue}_{sym}_{ymd}.parquet")
                if not path.exists():
                    book = load_data(date=ymd, table="MarketBook", venue=venue, symbol=sym)
                    print(f"{venue} {sym} N={book.shape[0]}")
                    if len(book) > 0:
                        res = generate_book_features(book)
                        if len(res) > 0:
                            path.parent.mkdir(exist_ok=True, parents=True)
                            res.to_parquet(path)
                            print(f"Saved: {path}")
                            del book
                            del res

    # RUN
    today = pd.Timestamp.today().floor("D")
    # today = pd.Timestamp("2025-07-29")
    for date in pd.date_range(today - pd.Timedelta("7D"), today):
        generate_and_save(date)
