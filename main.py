import sys
import time

from src import extract, transform, validate, load_upsert
from src.logger     import get_logger
from src.exceptions import ETLException

logger = get_logger("main")


def run_pipeline() -> dict:
    """
    Orchestrator: chạy toàn bộ pipeline E → T → L.
    Trả về dict report tóm tắt kết quả.
    """
    start  = time.time()
    report = {
        "status"       : "success",
        "rows_extracted": 0,
        "rows_inserted" : 0,
        "duration_sec"  : 0,
        "errors"        : [],
    }

    logger.info("=" * 55)
    logger.info("ETL Stock VN — pipeline bắt đầu")

    try:
        # Extract
        raw = extract()
        report["rows_extracted"] = len(raw)

        # Transform
        processed = transform(raw)
        validate(processed)

        # Load
        report["rows_inserted"] = load_upsert(processed)

        logger.info(
            f"Pipeline hoàn thành — "
            f"extracted={report['rows_extracted']}, "
            f"inserted={report['rows_inserted']}"
        )

    except ETLException as e:
        report["status"] = "failed"
        report["errors"].append(str(e))
        logger.exception(f"Pipeline thất bại (ETL error): {e}")
        sys.exit(1)

    except Exception as e:
        report["status"] = "failed"
        report["errors"].append(str(e))
        logger.exception(f"Pipeline thất bại (unexpected): {e}")
        sys.exit(1)

    finally:
        report["duration_sec"] = round(time.time() - start, 2)
        logger.info(f"=== PIPELINE REPORT === {report}")
        logger.info("=" * 55)

    return report


if __name__ == "__main__":
    run_pipeline()