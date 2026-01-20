import os
import json
import requests
import numpy as np
import pandas as pd
import datetime
from typing import List, Dict, Optional, Any
from etr.config import Config

BASE_URL = "https://api.jquants.com/v1"


def get_refresh_token(mailaddress: str, password: str) -> str:
    """
    /token/auth_user でリフレッシュトークンを取得
    有効期限: 1週間
    docs: https://jpx.gitbook.io/j-quants-ja/api-reference/refreshtoken
    """
    url = f"{BASE_URL}/token/auth_user"
    payload = {
        "mailaddress": mailaddress,
        "password": password,
    }
    # 公式サンプルは data=json.dumps(...) だが、json= を使うと Content-Type も自動設定されて楽
    resp = requests.post(url, json=payload, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    refresh_token = data["refreshToken"]
    return refresh_token


def get_id_token(refresh_token: str) -> str:
    """
    /token/auth_refresh でIDトークンを取得
    有効期限: 24時間
    docs: https://jpx.gitbook.io/j-quants-ja/api-reference/idtoken
    """
    url = f"{BASE_URL}/token/auth_refresh"
    params = {"refreshtoken": refresh_token}
    resp = requests.post(url, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    id_token = data["idToken"]
    return id_token


def auth_headers(id_token: str) -> pd.DataFrame:
    return {"Authorization": f"Bearer {id_token}"}


def get_listed_info(id_token: str, code: Optional[str] = None, date: Optional[str] = None):
    """
    /listed/info で上場銘柄一覧・銘柄情報を取得
    docs: https://jpx.gitbook.io/j-quants-ja/api-reference/listed_info

    Parameters
    ----------
    id_token : str
        get_id_token() で取得した IDトークン
    code : str | None
        銘柄コード (例: "72030" or "7203")。Noneなら全銘柄
    date : str | None
        日付 (例: "2024-01-04" または "20240104")。Noneなら「実行日」の情報
    """
    url = f"{BASE_URL}/listed/info"
    headers = {"Authorization": f"Bearer {id_token}"}
    params: dict[str, str] = {}

    if code is not None:
        params["code"] = code
    if date is not None:
        params["date"] = date

    resp = requests.get(url, headers=headers, params=params, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    # 正常時は {"info": [ {...}, {...}, ... ]} という形
    return pd.DataFrame(data.get("info", []))


def get_price_daily_quotes(
    id_token: str,
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
    url = f"{BASE_URL}/prices/daily_quotes"

    params: Dict[str, Any] = {}
    if code is not None:
        params["code"] = str(code)
    if date is not None:
        params["date"] = date
    if from_ is not None:
        params["from"] = from_
    if to_ is not None:
        params["to"] = to_

    resp = requests.get(url, headers=auth_headers(id_token), params=params or None)
    resp.raise_for_status()
    data = resp.json()
    # 仕様上は {"daily_prices": [ {...}, ... ]} 的な構造になっているはず
    # 実際のキー名は docs/実レスポンスに合わせて修正してね
    res = data.get("daily_prices", data.get("prices", data))
    return pd.DataFrame(res["daily_quotes"])


def get_fins_statements(
    id_token: str,
    code: Optional[str] = None,
    date: Optional[str] = None,
) -> pd.DataFrame:
    """
    財務情報(/fins/statements)
    - code: 銘柄コード（4 or 5桁, 例: '7203', '86970'）
    - date: 開示日 'YYYY-MM-DD' or 'YYYYMMDD'
      （code だけ指定しても直近2年分などがまとめて返ってくるケースが多い）
    """
    url = f"{BASE_URL}/fins/statements"
    params: Dict[str, str] = {}
    if code is not None:
        params["code"] = str(code)
    if date is not None:
        params["date"] = date

    resp = requests.get(url, headers=auth_headers(id_token), params=params or None)
    resp.raise_for_status()
    data = resp.json()
    # 公式サンプルでは top-level key は "statements" :contentReference[oaicite:0]{index=0}
    res = pd.DataFrame(data.get("statements", []))
    str_cols = [
        'DisclosedDate',
        'DisclosedTime',
        'LocalCode',
        'DisclosureNumber',
        'TypeOfDocument',
        'TypeOfCurrentPeriod',
        'CurrentPeriodStartDate',
        'CurrentPeriodEndDate',
        'CurrentFiscalYearStartDate',
        'CurrentFiscalYearEndDate',
        'NextFiscalYearStartDate',
        'NextFiscalYearEndDate',
        'SignificantChangesInTheScopeOfConsolidation',
        'ChangesBasedOnRevisionsOfAccountingStandard',
        'ChangesOtherThanOnesBasedOnRevisionsOfAccountingStandard',
        'ChangesInAccountingEstimates',
        'RetrospectiveRestatement',
        "MaterialChangesInSubsidiaries",
    ]
    num_cols = [col for col in res.columns if col not in str_cols]
    res[num_cols] = res[num_cols].replace("", np.nan).astype(float)
    res = res.rename(lambda x: x.replace("(", "").replace(")", ""), axis=1)
    return res


def get_fins_announcement(id_token: str) -> pd.DataFrame:
    """
    決算発表予定日(/fins/announcement)
    - 3月期・9月期決算会社の「翌営業日」の決算発表予定が返る。
    - Free プランで利用可。:contentReference[oaicite:1]{index=1}
    """
    url = f"{BASE_URL}/fins/announcement"
    resp = requests.get(url, headers=auth_headers(id_token))
    resp.raise_for_status()
    data = resp.json()
    # res.json() -> {"announcement": [ {...}, ... ]} :contentReference[oaicite:2]{index=2}
    df = pd.DataFrame(data.get("announcement", []))
    df["RetrievedDate"] = datetime.datetime.now(tz=datetime.timezone.utc).replace(tzinfo=None)
    return df


def get_markets_trading_calendar(
    id_token: str,
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
    url = f"{BASE_URL}/markets/trading_calendar"
    params: Dict[str, Any] = {}
    if holidaydivision is not None:
        params["holidaydivision"] = str(holidaydivision)
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
    resp = requests.get(url, headers=auth_headers(id_token), params=params or None)
    resp.raise_for_status()
    data = resp.json()
    # 公式記事では json()["trading_calendar"] :contentReference[oaicite:4]{index=4}
    data = pd.DataFrame(data.get("trading_calendar", []))
    data.HolidayDivision = data.HolidayDivision.astype(int).replace(mapper)
    return data

def get_indices(
    id_token: str,
    *,
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
    headers = {"Authorization": f"Bearer {id_token}"}

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
            f"{BASE_URL}/indices",
            headers=headers,
            params=params,
            timeout=30,
        )
        r.raise_for_status()
        data = r.json()

        rows = data.get("indices", [])
        all_rows.extend(rows)

        pagination_key = data.get("pagination_key")
        if not pagination_key:
            break

    # ---- DataFrame 化 ----
    if not all_rows:
        return pd.DataFrame(columns=["Date", "Code", "Open", "High", "Low", "Close"])

    df = pd.DataFrame(all_rows)

    # カラム揃える
    expected = ["Date", "Code", "Open", "High", "Low", "Close"]
    for col in expected:
        if col not in df.columns:
            df[col] = None

    return df[expected]

def _get_with_pagination(path: str, id_token: str, params: dict, list_key: str) -> pd.DataFrame:
    headers = {"Authorization": f"Bearer {id_token}"}
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

def get_index_option(id_token: str, date: str) -> pd.DataFrame:
    """
    /option/index_option を叩いて、日付指定で DataFrame を返す。
    date: 'YYYYMMDD' or 'YYYY-MM-DD'
    """
    return _get_with_pagination(
        "/option/index_option",
        id_token,
        {"date": date},
        "index_option",
    ).rename({"Volume(OnlyAuction)": "Volume_OnlyAuction"}, axis=1)

def get_weekly_margin_interest(
    id_token: str,
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
        "/markets/weekly_margin_interest",
        id_token,
        params,
        "weekly_margin_interest",
    )

    return df

def get_short_selling(
    id_token: str,
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
        params["sector33code"] = sector33code
    if date is not None:
        params["date"] = date
    if date_from is not None:
        params["from"] = date_from
    if date_to is not None:
        params["to"] = date_to

    df = _get_with_pagination(
        "/markets/short_selling",
        id_token,
        params,
        "short_selling",
    )

    cols = [
        "Date", "Sector33Code",
        "SellingExcludingShortSellingTurnoverValue",
        "ShortSellingWithRestrictionsTurnoverValue",
        "ShortSellingWithoutRestrictionsTurnoverValue",
    ]
    if not df.empty:
        df = df[[c for c in cols if c in df.columns]]
    return df

def get_short_selling_positions(
    id_token: str,
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
        params["disclosed_date"] = disclosed_date
    if disclosed_date_from is not None:
        params["disclosed_date_from"] = disclosed_date_from
    if disclosed_date_to is not None:
        params["disclosed_date_to"] = disclosed_date_to
    if calculated_date is not None:
        params["calculated_date"] = calculated_date

    df = _get_with_pagination(
        "/markets/short_selling_positions",
        id_token,
        params,
        "short_selling_positions",
    )

    cols = [
        "DisclosedDate", "CalculatedDate",
        "Code",
        "ShortSellerName", "ShortSellerAddress",
        "DiscretionaryInvestmentContractorName",
        "DiscretionaryInvestmentContractorAddress",
        "InvestmentFundName",
        "ShortPositionsToSharesOutstandingRatio",
        "ShortPositionsInSharesNumber",
        "ShortPositionsInTradingUnitsNumber",
        "CalculationInPreviousReportingDate",
        "ShortPositionsInPreviousReportingRatio",
        "Notes",
    ]
    if not df.empty:
        df = df[[c for c in cols if c in df.columns]]
    return df

def get_daily_margin_interest(
    id_token: str,
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

    df_raw = _get_with_pagination(
        "/markets/daily_margin_interest",
        id_token,
        params,
        "daily_margin_interest",
    )
    if df_raw.empty:
        return df_raw

    # PublishReason(Map) を JSON 文字列に潰しておく（任意）
    if "PublishReason" in df_raw.columns:
        df_raw["PublishReason_JSON"] = df_raw["PublishReason"].apply(
            lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, dict) else None
        )
        df_raw = df_raw.drop(columns=["PublishReason"])

    cols = [
        "PublishedDate", "Code", "ApplicationDate", "PublishReason_JSON",
        "ShortMarginOutstanding",
        "DailyChangeShortMarginOutstanding",
        "ShortMarginOutstandingListedShareRatio",
        "LongMarginOutstanding",
        "DailyChangeLongMarginOutstanding",
        "LongMarginOutstandingListedShareRatio",
        "ShortLongRatio",
        "ShortNegotiableMarginOutstanding",
        "DailyChangeShortNegotiableMarginOutstanding",
        "ShortStandardizedMarginOutstanding",
        "DailyChangeShortStandardizedMarginOutstanding",
        "LongNegotiableMarginOutstanding",
        "DailyChangeLongNegotiableMarginOutstanding",
        "LongStandardizedMarginOutstanding",
        "DailyChangeLongStandardizedMarginOutstanding",
        "TSEMarginBorrowingAndLendingRegulationClassification",
    ]
    df = df_raw[[c for c in cols if c in df_raw.columns]]
    return df


if __name__ == "__main__":
    # 環境変数から認証情報を読む想定
    mailaddress = Config.JQUANTS_ID
    password = Config.JQUANTS_PW

    # 1. Refresh Token取得 (1週間有効)
    refresh_token = get_refresh_token(mailaddress, password)
    print("refreshToken:", refresh_token[:20] + "...")

    # 2. ID Token取得 (24時間有効)
    id_token = get_id_token(refresh_token)
    print("idToken:", id_token[:20] + "...")

    # 3. listed_info取得（例: 当日時点の全銘柄）
    info_list = get_listed_info(id_token)

    # pandas.DataFrame にして冒頭だけ確認
    df = pd.DataFrame(info_list)
    print(df.head())
