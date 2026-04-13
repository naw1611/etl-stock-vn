import pandas as pd

from src.logger     import get_logger
from src.exceptions import TransformError

logger = get_logger("transform")


def transform(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean data và tính các chỉ số kỹ thuật:
    - Rename cột, convert datetime
    - Drop missing & duplicate
    - Validate giá trị hợp lệ
    - Tính pct_change, MA5, MA20, vol_ma5
    - Thêm cột signal BUY/SELL
    """
    logger.info("Bắt đầu transform...")

    # --- 1. Rename & chuẩn hoá tên cột ---
    df = df.rename(columns={
        "Date"  : "trade_date",
        "Open"  : "open_price",
        "High"  : "high_price",
        "Low"   : "low_price",
        "Close" : "close_price",
        "Volume": "volume",
    })

    # --- 2. Convert datetime ---
    df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date

    # --- 3. Drop missing & invalid ---
    before = len(df)
    df = df.dropna(subset=["close_price", "volume"])
    df = df[df["close_price"] > 0]
    df = df.drop_duplicates(subset=["symbol", "trade_date"])
    dropped = before - len(df)
    if dropped > 0:
        logger.warning(f"Đã loại {dropped} rows không hợp lệ")

    # Cảnh báo nếu missing quá nhiều
    missing_pct = df["close_price"].isnull().sum() / len(df) * 100
    if missing_pct > 5:
        logger.warning(f"Missing close_price: {missing_pct:.1f}% — kiểm tra lại source")

    logger.info(f"Cleaned: {before} → {len(df)} rows")

    # --- 4. Sắp xếp trước khi tính rolling ---
    df = df.sort_values(["symbol", "trade_date"]).reset_index(drop=True)

    # --- 5. Tính chỉ số kỹ thuật (theo từng symbol) ---
    grp = df.groupby("symbol")["close_price"]

    df["pct_change"] = grp.pct_change().mul(100).round(2)

    for window in [5, 20]:
        df[f"ma{window}"] = (
            grp.transform(lambda x: x.rolling(window).mean()).round(2)
        )

    df["vol_ma5"] = (
        df.groupby("symbol")["volume"]
        .transform(lambda x: x.rolling(5).mean())
        .round(0)
    )

    # --- 6. Signal BUY / SELL ---
    df["signal"] = "NEUTRAL"
    df.loc[df["ma5"] > df["ma20"], "signal"] = "BUY"
    df.loc[df["ma5"] < df["ma20"], "signal"] = "SELL"

    # --- 7. Thêm metadata ---
    df["loaded_at"] = pd.Timestamp.now()

    logger.info(f"Transform xong — cột: {df.columns.tolist()}")
    return df


def validate(df: pd.DataFrame) -> bool:
    """
    Kiểm tra data sau transform.
    Raise TransformError nếu phát hiện vấn đề nghiêm trọng.
    """
    logger.info("Bắt đầu validate...")

    if df["close_price"].le(0).any():
        raise TransformError("Phát hiện close_price <= 0")

    if df["volume"].lt(0).any():
        raise TransformError("Phát hiện volume âm")

    dups = df.duplicated(["symbol", "trade_date"]).sum()
    if dups > 0:
        raise TransformError(f"Phát hiện {dups} duplicate (symbol, trade_date)")

    symbols   = sorted(df["symbol"].unique())
    date_min  = df["trade_date"].min()
    date_max  = df["trade_date"].max()
    logger.info(f"Symbols: {symbols}")
    logger.info(f"Date range: {date_min} → {date_max}")
    logger.info(f"Validate OK — {len(df)} rows sạch")
    return True