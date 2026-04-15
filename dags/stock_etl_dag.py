from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
sys.path.insert(0, "/mnt/d/Visual Studio Code/python/etl-stock-vn")

from src import extract, transform, validate, load_upsert

# --- Cấu hình DAG ---
default_args = {
    "owner"           : "de_team",
    "retries"         : 3,
    "retry_delay"     : timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id          = "stock_etl_daily",
    default_args    = default_args,
    description     = "ETL pipeline for VN stock market data",
    schedule        = "0 18 * * 1-5",  # 18:00 thứ 2–6
    start_date      = datetime(2025, 1, 1),
    catchup         = False,
    tags            = ["stock", "etl", "vietnam"],
) as dag:

    # --- Task wrapper functions ---
    def task_extract(**ctx):
        raw = extract()
        # truyền data sang task tiếp theo qua XCom
        ctx["ti"].xcom_push(key="row_count", value=len(raw))
        raw.to_csv("/tmp/raw_stock.csv", index=False)

    def task_transform(**ctx):
        import pandas as pd
        raw       = pd.read_csv("/tmp/raw_stock.csv")
        processed = transform(raw)
        validate(processed)
        processed.to_csv("/tmp/processed_stock.csv", index=False)

    def task_load(**ctx):
        import pandas as pd
        processed = pd.read_csv("/tmp/processed_stock.csv")
        n = load_upsert(processed)
        ctx["ti"].xcom_push(key="inserted", value=n)

    # --- Định nghĩa tasks ---
    t_extract   = PythonOperator(task_id="extract",   python_callable=task_extract)
    t_transform = PythonOperator(task_id="transform", python_callable=task_transform)
    t_load      = PythonOperator(task_id="load",      python_callable=task_load)

    # --- Dependency: Extract → Transform → Load ---
    t_extract >> t_transform >> t_load