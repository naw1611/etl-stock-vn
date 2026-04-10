import yfinance as yf
import pandas as pd
import time
from config import SYMBOLS, PERIOD

def fetch_one(symbol, retries=3):
    """Lấy data 1 mã, có retry khi API lỗi"""
    for attempt in range(retries):
        try:
            ticker = yf.Ticker(symbol)
            df = ticker.history(period=PERIOD)
            if df.empty:
                print(f"[WARN] Không có data cho {symbol}")
                return None
            df["symbol"] = symbol.replace(".VN", "")
            return df[["Open", "High", "Low", "Close", "Volume", "symbol"]]
        except Exception as e:
            print(f"[ERROR] {symbol} lần {attempt+1}: {e}")
            time.sleep(2)
    return None

def extract():
    """Lấy data tất cả symbols, trả về DataFrame gộp"""
    frames = []
    for sym in SYMBOLS:
        df = fetch_one(sym)
        if df is not None:
            frames.append(df)
        time.sleep(0.5)  # tránh bị rate limit
    
    if not frames:
        raise ValueError("Không lấy được data từ bất kỳ symbol nào")
    
    result = pd.concat(frames).reset_index()
    print(f"[INFO] Extract xong: {len(result)} records, {result['symbol'].nunique()} mã")
    return result

#test
# if __name__ == "__main__":
#     df = extract()

#     print(df.shape)
#     print(df.head())

def transform(df):
    """Clean và tính toán chỉ số kỹ thuật"""

    # --- 1. Rename & chuẩn hoá cột ---
    df = df.rename(columns={
        "Date"  : "trade_date",
        "Open"  : "open_price",
        "High"  : "high_price",
        "Low"   : "low_price",
        "Close" : "close_price",
        "Volume": "volume"
    })

    # --- 2. Convert datetime ---
    df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date

    # --- 3. Drop missing & validate ---
    before = len(df)
    df = df.dropna(subset=["close_price", "volume"])
    df = df[df["close_price"] > 0]        # loại giá âm / zero
    df = df.drop_duplicates(
        subset=["symbol", "trade_date"])  # loại duplicate
    print(f"[INFO] Cleaned: {before} → {len(df)} rows")

    # --- 4. Tính chỉ số kỹ thuật (theo từng mã) ---
    df = df.sort_values(["symbol", "trade_date"])

    df["pct_change"] = (
        df.groupby("symbol")["close_price"]
        .pct_change() * 100
    ).round(2)

    for window in [5, 20]:
        col = f"ma{window}"
        df[col] = (
            df.groupby("symbol")["close_price"]
            .transform(lambda x: x.rolling(window).mean())
        ).round(2)

    df["vol_ma5"] = (
        df.groupby("symbol")["volume"]
        .transform(lambda x: x.rolling(5).mean())
    ).round(0)

    # --- 5. Thêm metadata ---
    df["loaded_at"] = pd.Timestamp.now()

    print(f"[INFO] Transform xong: {df.columns.tolist()}")
    return df

def validate(df):
    """Kiểm tra nhanh data sau transform"""
    assert df["close_price"].gt(0).all(), "Có giá <= 0!"
    assert df["volume"].ge(0).all(),       "Có volume âm!"
    assert df.duplicated(["symbol", "trade_date"]).sum() == 0, "Có duplicate!"
    
    missing_pct = df["close_price"].isnull().sum() / len(df) * 100
    print(f"[INFO] Missing close_price: {missing_pct:.1f}%")
    print(f"[INFO] Symbols: {sorted(df['symbol'].unique())}")
    print(f"[INFO] Date range: {df['trade_date'].min()} → {df['trade_date'].max()}")
    return True
#test
# if __name__ == "__main__":
#     df_raw = extract()        # lấy data từ API
#     df_trans = transform(df_raw)  # transform
#     validate(df_trans)        # kiểm tra

#     print(df_trans.head())

from sqlalchemy import create_engine, text
import os
from config import DB_CONFIG, OUTPUT_DIR

def get_engine():
    password = os.getenv("DB_PASSWORD", "")
    conn_str = (
        f"mssql+pyodbc://{DB_CONFIG['server']}/{DB_CONFIG['database']}"
        f"?driver={DB_CONFIG['driver'].replace(' ', '+')}"
        "&trusted_connection=yes"  # Windows Auth, không cần password
    )
    return create_engine(conn_str)

def load(df):
    """Load DataFrame vào SQL Server và lưu CSV backup"""

    # Bước 1: Lưu CSV backup
    import os
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    csv_path = f"{OUTPUT_DIR}/stock_data.csv"
    df.to_csv(csv_path, index=False)
    print(f"[INFO] CSV saved: {csv_path}")

    # Bước 2: Load vào SQL Server
    engine = get_engine()
    cols = ["symbol","trade_date","open_price","high_price",
            "low_price","close_price","volume",
            "pct_change","ma5","ma20","vol_ma5"]

    try:
        df[cols].to_sql(
            name       = "stock_daily",
            con        = engine,
            if_exists  = "append",   # append, không replace!
            index      = False,
            chunksize  = 500          # tránh timeout với data lớn
        )
        print(f"[INFO] Loaded {len(df)} rows vào SQL Server")
    except Exception as e:
        print(f"[ERROR] SQL load thất bại: {e}")
        raise

def load_upsert(df):
    """Chỉ insert record chưa có trong DB"""
    engine = get_engine()
    
    # Lấy danh sách (symbol, date) đã có trong DB
    existing = pd.read_sql(
        "SELECT symbol, trade_date FROM stock_daily", engine
    )
    existing["trade_date"] = pd.to_datetime(existing["trade_date"]).dt.date
    
    # Merge để tìm record mới
    merged = df.merge(existing, on=["symbol","trade_date"],
                     how="left", indicator=True)
    new_rows = merged[merged["_merge"] == "left_only"].drop(columns=["_merge"])
    
    print(f"[INFO] {len(new_rows)}/{len(df)} rows mới cần insert")
    if not new_rows.empty:
        load(new_rows)
    return len(new_rows)
 
#test
# if __name__ == "__main__":
#     df_raw = extract()
#     df_trans = transform(df_raw)
#     validate(df_trans)

#     load_upsert(df_trans)

# from extract   import extract
# from transform import transform, validate
# from load      import load_upsert
import sys

def main():
    print("="*50)
    print("ETL Stock VN — bắt đầu chạy")

    try:
        raw       = extract()
        processed = transform(raw)
        validate(processed)
        inserted  = load_upsert(processed)
        print(f"[DONE] Inserted {inserted} rows mới")
        print("="*50)

    except Exception as e:
        print(f"[FATAL] Pipeline thất bại: {e}")
        sys.exit(1)  # exit code 1 → Task Scheduler biết bị lỗi

if __name__ == "__main__":
    main()