import requests
import numpy as np
import pandas as pd
import json
from typing import List, Dict, Optional, Any
from etr.config import Config

BASE_URL = "https://api.jquants.com/v2"


def auth_headers():
    return {"x-api-key": Config.JQUANTS_API_KEY}


def get_equities_master(code: Optional[str] = None, date: Optional[str] = None):
    """

    Parameters
    ----------
    id_token : str
        get_id_token() で取得した IDトークン
    code : str | None
        銘柄コード (例: "72030" or "7203")。Noneなら全銘柄
    date : str | None
        日付 (例: "2024-01-04" または "20240104")。Noneなら「実行日」の情報
    """
    url = f"{BASE_URL}/equities/master"
    params: dict[str, str] = {}

    if code is not None:
        params["code"] = code
    if date is not None:
        params["date"] = date

    resp = requests.get(url, headers=auth_headers(), params=params, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    return pd.DataFrame(data.get("data", []))


def get_equities_bars_daily(
    code: Optional[str] = None,
    date: Optional[str] = None,
    from_: Optional[str] = None,
    to_: Optional[str] = None,
) -> pd.DataFrame:
    """
    日足OHLCV(/price/daily)

    Parameters
    ----------
    id_token : str
        認証トークン
    code : str | None
        銘柄コード (4 or 5桁) を指定するとその銘柄のみ
        省略時は全銘柄
    date : str | None
        単一日付 'YYYY-MM-DD' or 'YYYYMMDD'
        from_ / to_ を使う場合は指定しない
    from_ : str | None
        開始日 'YYYY-MM-DD' or 'YYYYMMDD'
    to_ : str | None
        終了日 'YYYY-MM-DD' or 'YYYYMMDD'

    Returns
    -------
    List[Dict[str, Any]]
        例: {"Date", "Code", "Open", "High", "Low", "Close", "Volume", ...}
    """
    url = f"{BASE_URL}/equities/bars/daily"

    params: Dict[str, Any] = {}
    if code is not None:
        params["code"] = str(code)
    if date is not None:
        params["date"] = date
    if from_ is not None:
        params["from"] = from_
    if to_ is not None:
        params["to"] = to_

    resp = requests.get(url, headers=auth_headers(), params=params or None)
    resp.raise_for_status()
    data = resp.json()
    return pd.DataFrame(data["data"])


def get_equities_earnings_calendar() -> pd.DataFrame:
    """
    決算発表予定日(/fins/announcement)
    - 3月期・9月期決算会社の「翌営業日」の決算発表予定が返る。
    - Free プランで利用可。:contentReference[oaicite:1]{index=1}
    """
    url = f"{BASE_URL}/equities/earnings-calendar"
    resp = requests.get(url, headers=auth_headers())
    resp.raise_for_status()
    data = resp.json()
    # res.json() -> {"announcement": [ {...}, ... ]} :contentReference[oaicite:2]{index=2}
    df = pd.DataFrame(data.get("data", []))
    return df


def get_fins_summary(
    code: Optional[str] = None,
    date: Optional[str] = None,
) -> pd.DataFrame:
    """
    財務情報(/fins/statements)
    - code: 銘柄コード（4 or 5桁, 例: '7203', '86970'）
    - date: 開示日 'YYYY-MM-DD' or 'YYYYMMDD'
      （code だけ指定しても直近2年分などがまとめて返ってくるケースが多い）
    """
    url = f"{BASE_URL}/fins/summary"
    params: Dict[str, str] = {}
    if code is not None:
        params["code"] = str(code)
    if date is not None:
        params["date"] = date

    resp = requests.get(url, headers=auth_headers(), params=params or None)
    resp.raise_for_status()
    data = resp.json()
    # 公式サンプルでは top-level key は "statements" :contentReference[oaicite:0]{index=0}
    res = pd.DataFrame(data.get("data", []))
    str_cols = [
        'DiscDate',
        'DiscTime',
        'Code',
        'DiscNo',
        'DocType',
        'CurPerType',
        'CurPerSt',
        'CurPerEn',
        'CurFYSt',
        'CurFYEn',
        'NxtFYSt',
        'NxtFYEn',
        'SigChgInC',
        'ChgByASRev',
        'ChgNoASRev',
        'ChgAcEst',
        'RetroRst',
        'MatChgSub'
    ]
    num_cols = [col for col in res.columns if col not in str_cols]
    res[num_cols] = res[num_cols].replace("", np.nan).astype(float)
    res = res.rename(lambda x: x.replace("(", "").replace(")", ""), axis=1)
    return res


def get_markets_calendar(
    holidaydivision: Optional[int] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    取引カレンダー(/markets/trading_calendar)
    - holidaydivision:
        0: 非営業日
        1: 営業日
        2: 東証 半日立会日
        3: 祝日取引あり非営業日
      など（詳細はAPI仕様参照）:contentReference[oaicite:3]{index=3}
    - date_from, date_to: 'YYYY-MM-DD' or 'YYYYMMDD'
    """
    url = f"{BASE_URL}/markets/calendar"
    params: Dict[str, Any] = {}
    if holidaydivision is not None:
        params["hol_div"] = str(holidaydivision)
    if date_from is not None:
        params["from"] = date_from
    if date_to is not None:
        params["to"] = date_to

    mapper = {
        0: "非営業日",
        1: "営業日",
        2: "東証半日立会日",
        3: "非営業日(祝日取引あり)",
    }
    resp = requests.get(url, headers=auth_headers(), params=params or None)
    resp.raise_for_status()
    data = resp.json()
    # 公式記事では json()["trading_calendar"] :contentReference[oaicite:4]{index=4}
    data = pd.DataFrame(data.get("data", []))
    data.HolDiv = data.HolDiv.astype(int).replace(mapper)
    return data


def get_indices_bars_daily(
    code: Optional[str] = None,
    date: Optional[str] = None,
    from_: Optional[str] = None,
    to: Optional[str] = None,
) -> pd.DataFrame:
    """
    指数四本値 (/indices) を取得し、ページング処理もすべて行って
    DataFrame にまとめて返す。

    Parameters
    ----------
    id_token : str
        /token/auth_refresh で取得した ID トークン
    code : str, optional
        指数コード (例: '0000', '0028')
    date : str, optional
        単一日付
    from_ : str, optional
        期間開始 (YYYY-MM-DD)
    to : str, optional
        期間終了 (YYYY-MM-DD)

    Returns
    -------
    pd.DataFrame
        columns = ["Date", "Code", "Open", "High", "Low", "Close"]
    """
    # クエリパラメータ
    params: Dict[str, str] = {}
    if code is not None:
        params["code"] = code
    if date is not None:
        params["date"] = date
    if from_ is not None:
        params["from"] = from_
    if to is not None:
        params["to"] = to

    if not params:
        raise ValueError("code, date, from_, to のいずれかは必須です。")

    all_rows: List[Dict[str, Any]] = []
    pagination_key: Optional[str] = None

    while True:
        if pagination_key is not None:
            params["pagination_key"] = pagination_key

        r = requests.get(
            f"{BASE_URL}/indices/bars/daily",
            headers=auth_headers(),
            params=params,
            timeout=30,
        )
        r.raise_for_status()
        data = r.json()

        rows = data.get("data", [])
        all_rows.extend(rows)

        pagination_key = data.get("pagination_key")
        if not pagination_key:
            break

    return pd.DataFrame(all_rows)


def _get_with_pagination(path: str, params: dict, list_key: str = "data") -> pd.DataFrame:
    headers = auth_headers()
    all_rows = []
    pagination_key = None

    while True:
        p = dict(params)
        if pagination_key:
            p["pagination_key"] = pagination_key

        r = requests.get(BASE_URL + path, headers=headers, params=p)
        r.raise_for_status()
        data = r.json()

        rows = data.get(list_key, [])
        all_rows.extend(rows)

        pagination_key = data.get("pagination_key")
        if not pagination_key:
            break

    if not all_rows:
        return pd.DataFrame()

    df = pd.DataFrame(all_rows)
    return df


def get_derivatives_bars_index_options(date: str) -> pd.DataFrame:
    """
    /option/index_option を叩いて、日付指定で DataFrame を返す。
    date: 'YYYYMMDD' or 'YYYY-MM-DD'
    """
    return _get_with_pagination(
        "derivatives/bars/daily/options",
        {"date": date},
        "index_option",
    ).rename({"Volume(OnlyAuction)": "Volume_OnlyAuction"}, axis=1)


def get_derivatives_bars_daily_options_225(date: str) -> pd.DataFrame:
    """
    /option/index_option を叩いて、日付指定で DataFrame を返す。
    date: 'YYYYMMDD' or 'YYYY-MM-DD'
    """
    return _get_with_pagination(
        "/derivatives/bars/daily/options/225",
        {"date": date},
    ).rename({"Volume(OnlyAuction)": "Volume_OnlyAuction"}, axis=1)


def get_markets_margin_interest(
    code: str = None,
    date: str = None,
    date_from: str = None,
    date_to: str = None,
) -> pd.DataFrame:
    """
    /markets/weekly_margin_interest を叩いて DataFrame を返す。
    code, date, date_from(from), date_to(to) は J-Quants の仕様通り。
    """
    params: dict[str, str] = {}
    if code is not None:
        params["code"] = code
    if date is not None:
        params["date"] = date
    if date_from is not None:
        params["from"] = date_from
    if date_to is not None:
        params["to"] = date_to

    df = _get_with_pagination(
        "/markets/margin-interest",
        params,
    )

    return df


def get_markets_short_ratio(
    sector33code: str = None,
    date: str = None,
    date_from: str = None,
    date_to: str = None,
) -> pd.DataFrame:
    """
    /markets/short_selling を叩いて DataFrame を返す。
    sector33code, date, from, to は J-Quants 仕様通り。
    """
    params: dict[str, str] = {}
    if sector33code is not None:
        params["s33"] = sector33code
    if date is not None:
        params["date"] = date
    if date_from is not None:
        params["from"] = date_from
    if date_to is not None:
        params["to"] = date_to

    df = _get_with_pagination(
        "/markets/short-ratio",
        params,
    )
    return df


def get_markets_short_sale_report(
    code: str = None,
    disclosed_date: str = None,
    disclosed_date_from: str = None,
    disclosed_date_to: str = None,
    calculated_date: str = None,
) -> pd.DataFrame:
    """
    /markets/short_selling_positions を叩いて DataFrame を返す。
    code / disclosed_date / disclosed_date_from / disclosed_date_to / calculated_date をそのまま投げる。
    """
    params: dict[str, str] = {}
    if code is not None:
        params["code"] = code
    if disclosed_date is not None:
        params["disc_date"] = disclosed_date
    if disclosed_date_from is not None:
        params["disc_date_from"] = disclosed_date_from
    if disclosed_date_to is not None:
        params["disc_date_to"] = disclosed_date_to
    if calculated_date is not None:
        params["calc_date"] = calculated_date

    df = _get_with_pagination(
        "/markets/short-sale-report",
        params,
    )
    return df


def get_markets_margin_alert(
    code: str = None,
    date: str = None,
    date_from: str = None,
    date_to: str = None,
) -> pd.DataFrame:
    """
    /markets/daily_margin_interest を叩いて DataFrame を返す。
    code / date / from / to をそのまま投げる。
    PublishReason は副作用少なく扱うため JSON 文字列にして PublishReason_JSON に格納してもよい。
    """
    params: dict[str, str] = {}
    if code is not None:
        params["code"] = code
    if date is not None:
        params["date"] = date
    if date_from is not None:
        params["from"] = date_from
    if date_to is not None:
        params["to"] = date_to

    df = _get_with_pagination(
        "/markets/margin-alert",
        params,
    )

    if len(df) > 0:
        df.PubReason = df.PubReason.apply(lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, dict) else None)
    return df


def get_bulk_list(
    endpoint="/equities/bars/minute",  # /equities/trades
) -> pd.DataFrame:
    params: dict[str, str] = {"endpoint": endpoint}
    df = _get_with_pagination(
        "/bulk/list",
        params,
    )
    return df


def get_bulk_url(
    key: str,
) -> Optional[str]:
    params: dict[str, str] = {"key": key}
    resp = requests.get(f"{BASE_URL}/bulk/get", headers=auth_headers(), params=params or None)
    resp.raise_for_status()
    data = resp.json()
    return data.get("url")


def get_bulk_data(key: str) -> pd.DataFrame:
    url = get_bulk_url(key)
    if url is None:
        return pd.DataFrame()
    else:
        return pd.read_csv(url, compression="gzip", low_memory=False)


if __name__ == "__main__":
    pass
