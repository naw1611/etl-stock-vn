import time
import functools

from src.logger import get_logger

logger = get_logger("utils")


def retry(max_attempts: int = 3, delay: float = 2, exceptions: tuple = (Exception,)):
    """
    Decorator retry với exponential backoff.

    Cách dùng:
        @retry(max_attempts=3, delay=2)
        def fetch_data():
            ...

    Delay tăng dần: 2s → 4s → 8s (2 ** attempt * delay)
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    wait = delay * (2 ** (attempt - 1))  # exponential backoff
                    if attempt == max_attempts:
                        logger.error(
                            f"{func.__name__} thất bại sau {max_attempts} lần thử: {e}"
                        )
                        raise
                    logger.warning(
                        f"{func.__name__} lần {attempt}/{max_attempts} thất bại "
                        f"— thử lại sau {wait:.0f}s | lỗi: {e}"
                    )
                    time.sleep(wait)
        return wrapper
    return decorator