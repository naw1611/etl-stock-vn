import time

import pandas as pd
import yfinance as yf

from config import SYMBOLS, PERIOD
from src.logger     import get_logger
from src.exceptions import ExtractError
from src.utils      import retry

logger = get_logger("extract")


@retry(max_attempts=3, delay=2, exceptions=(Exception,))
def _fetch_one(symbol: str) -> pd.DataFrame:
    """
    Lấy data OHLCV của 1 mã từ Yahoo Finance.
    Decorator @retry tự xử lý retry — không cần vòng lặp thủ công.
    """
    logger.debug(f"Fetching {symbol}...")
    ticker = yf.Ticker(symbol)
    df = ticker.history(period=PERIOD)

    if df.empty:
        raise ExtractError(f"Yahoo Finance trả về data rỗng cho {symbol}")

    df["symbol"] = symbol.replace(".VN", "")
    df = df.reset_index()

    logger.info(f"{symbol} OK — {len(df)} rows")
    return df[["Date", "Open", "High", "Low", "Close", "Volume", "symbol"]]


def extract() -> pd.DataFrame:
    """
    Lấy data tất cả symbols trong config.SYMBOLS.
    Trả về DataFrame gộp, sẵn sàng cho bước Transform.
    """
    logger.info(f"Bắt đầu extract {len(SYMBOLS)} symbols: {SYMBOLS}")
    frames = []

    for sym in SYMBOLS:
        try:
            df = _fetch_one(sym)
            frames.append(df)
        except ExtractError as e:
            # Ghi lỗi nhưng tiếp tục fetch các symbol còn lại
            logger.error(f"Bỏ qua {sym}: {e}")
        time.sleep(0.5)  # tránh rate limit

    if not frames:
        raise ExtractError("Không lấy được data từ bất kỳ symbol nào")

    result = pd.concat(frames, ignore_index=True)
    logger.info(
        f"Extract xong: {len(result)} records, "
        f"{result['symbol'].nunique()} mã"
    )
    return result