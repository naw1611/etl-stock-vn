import os

import pandas as pd
from sqlalchemy import create_engine,text

from config import DB_CONFIG, OUTPUT_DIR
from src.logger     import get_logger
from src.exceptions import LoadError

logger = get_logger("load")

COLS_TO_LOAD = [
    "symbol", "trade_date",
    "open_price", "high_price", "low_price", "close_price",
    "volume", "pct_change", "ma5", "ma20", "vol_ma5", "signal",
]


def _get_engine():
    """Tạo SQLAlchemy engine kết nối SQL Server (Windows Auth)."""
    driver = DB_CONFIG["driver"].replace(" ", "+")
    conn_str = (
        f"mssql+pyodbc://{DB_CONFIG['server']}/{DB_CONFIG['database']}"
        f"?driver={driver}&trusted_connection=yes"
    )
    return create_engine(conn_str, fast_executemany=True)


def _save_csv(df: pd.DataFrame) -> str:
    """Lưu CSV backup trước khi load vào DB."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    path = f"{OUTPUT_DIR}/stock_data.csv"
    df.to_csv(path, index=False)
    logger.info(f"CSV backup saved: {path}")
    return path


def load_upsert(df: pd.DataFrame) -> int:
    """
    Chỉ insert các record chưa có trong DB.
    Trả về số rows đã insert.
    """
    # Bước 1: Luôn lưu CSV backup
    _save_csv(df)

    # Bước 2: Lấy danh sách (symbol, trade_date) đã có trong DB
    try:
        engine = _get_engine()
        query = text("""
            SELECT symbol, CAST(trade_date AS DATE) AS trade_date
            FROM stock_daily
            """)
        with engine.connect() as conn:
            existing = pd.read_sql(query, conn)
        existing["trade_date"] = pd.to_datetime(existing["trade_date"]).dt.date
    except Exception as e:
        raise LoadError(f"Không đọc được dữ liệu hiện có từ DB: {e}")

    # Bước 3: Tìm rows chưa có trong DB
    merged   = df.merge(existing, on=["symbol", "trade_date"], how="left", indicator=True)
    new_rows = merged[merged["_merge"] == "left_only"].drop(columns=["_merge"])

    logger.info(f"{len(new_rows)}/{len(df)} rows mới cần insert")

    if new_rows.empty:
        logger.info("Không có gì mới — bỏ qua insert")
        return 0

    # Bước 4: Insert vào SQL Server
    try:
        new_rows[COLS_TO_LOAD].to_sql(
            name      = "stock_daily",
            con       = engine,
            if_exists = "append",
            index     = False,
            chunksize = 500,
        )
        logger.info(f"Loaded {len(new_rows)} rows vào SQL Server")
        return len(new_rows)
    except Exception as e:
        raise LoadError(f"Insert thất bại: {e}")