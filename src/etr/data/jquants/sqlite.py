from __future__ import annotations
from pathlib import Path
import sqlite3
import pandas as pd
from typing import Optional

from etr.config import Config


def _path_handler(db_path: str) -> Path:
    # 絶対パスならそれを返す, 相対パスならlibrary top levelからを返す
    if Path(db_path).as_posix().startswith("/"):
        return Path(db_path)
    else:
        cd = Path(__file__).parent
        base_path = cd.parent.parent.parent.parent
        return base_path.joinpath(db_path)


def apply_schema(db_path=Config.JQUANTS_DB):
    cd = Path(__file__).parent
    sql = cd.joinpath("jquants_ddl.sql").read_text(encoding="utf-8")
    path = _path_handler(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(path) as conn:
        conn.executescript(sql)
        conn.commit()


def get(
    query: str,
    db_path: Optional[str] = Config.JQUANTS_DB
) -> pd.DataFrame:
    path = _path_handler(db_path)
    with sqlite3.connect(path) as conn:
        return pd.read_sql(query, con=conn)


def run(
    query: str,
    db_path: Optional[str] = Config.JQUANTS_DB
):
    path = _path_handler(db_path)
    with sqlite3.connect(path) as conn:
        return conn.execute(query)


def upsert(
    df: pd.DataFrame,
    table: str,
    db_path: str = Config.JQUANTS_DB
) -> None:
    """
    Lightweight SQLite upsert for pandas DataFrame.
    - Opens/closes connection per call (safe for daily jobs)
    - Reads table schema (columns + PRIMARY KEY) from SQLite
    - Aligns df columns to table columns
    - UPSERT using ON CONFLICT(primary_key...)
    """

    with sqlite3.connect(_path_handler(db_path=db_path)) as conn:
        # Pragmas (optional but usually good for ETL-ish usage)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")

        # Read schema
        info = pd.read_sql(f"PRAGMA table_info({table})", conn)
        if info.empty:
            raise ValueError(f"Table not found (or no columns): {table}")

        all_cols = info["name"].tolist()
        cols = [c for c in all_cols if c != "ingested_at"]
        pk = (
            info.loc[info["pk"] > 0, ["name", "pk"]]
            .sort_values("pk")["name"]
            .tolist()
        )
        if not pk:
            raise ValueError(f"No PRIMARY KEY defined on table {table}")

        # Align df to table columns (drop extras / add missing as NaN)
        df2 = df.reindex(columns=cols)
        df2 = df2.dropna(subset=pk)
        if df2.empty:
            return

        # Optional: convert NaN -> None so SQLite stores NULL cleanly
        df2 = df2.where(pd.notnull(df2), None)

        # Build Upsert Query
        placeholders = ",".join(["?"] * len(cols))
        updates_exprs = [f"{c}=excluded.{c}" for c in cols if c not in pk]
        if "ingested_at" in all_cols:
            updates_exprs.append("ingested_at = datetime('now')")
        updates = ",".join(updates_exprs)
        sql = f"""
        INSERT INTO {table} ({",".join(cols)})
        VALUES ({placeholders})
        ON CONFLICT({",".join(pk)}) DO UPDATE SET
        {updates}
        """

        conn.executemany(sql, df2.itertuples(index=False, name=None))
        conn.commit()
