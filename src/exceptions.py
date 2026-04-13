class ETLException(Exception):
    """Base exception cho toàn bộ pipeline."""
    pass

class ExtractError(ETLException):
    """Lỗi khi lấy data từ API."""
    pass

class TransformError(ETLException):
    """Lỗi khi xử lý hoặc validate data."""
    pass

class LoadError(ETLException):
    """Lỗi khi ghi vào database."""
    pass