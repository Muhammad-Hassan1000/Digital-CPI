import dotenv
import os

dotenv.load_dotenv()

LOG_DIR = os.getenv("LOG_DIR")
MAX_LOG_FILE_SIZE = int(os.getenv("MAX_LOG_FILE_SIZE"))
LOG_FILE_BACKUP_COUNT = int(os.getenv("LOG_FILE_BACKUP_COUNT"))
LOG_UTC_FORMAT = os.getenv("LOG_UTC_FORMAT")

import logging
from datetime import datetime, timezone
from pathlib import Path
from logging.handlers import RotatingFileHandler


class CustomLogger:
    """
    Simple file-based logger with timestamp, filename, and log level.
    Creates daily log files in a logs directory.
    """
    
    def __init__(self, name: str, log_dir: str = LOG_DIR, max_file_size: int = MAX_LOG_FILE_SIZE, backup_count: int = LOG_FILE_BACKUP_COUNT, use_utc: bool = LOG_UTC_FORMAT):
        """
        Initialize CustomLogger with rotating file handler.
        
        Args:
            name: Logger name (usually filename without extension)
            log_dir: Directory for log files
            max_file_size: Maximum file size in MB before rotation (default: 10MB)
            backup_count: Number of backup files to keep (default: 5)
            use_utc: Use UTC timestamps instead of local time (default: True)
        """
        self.name = name
        self.log_dir = log_dir
        self.max_file_size = max_file_size * 1024 * 1024  # Convert MB to bytes
        self.backup_count = backup_count
        self.use_utc = use_utc
        self.logger = None
        self._setup_logger()
    
    def _setup_logger(self):
        """Setup the logger with rotating file handler and formatter."""
        # Create logs directory if it doesn't exist
        os.makedirs(self.log_dir, exist_ok=True)
        
        # Create log file name with logger name
        log_filename = f"{self.name}.log"
        log_filepath = os.path.join(self.log_dir, log_filename)
        
        # Create logger
        self.logger = logging.getLogger(self.name)
        self.logger.setLevel(logging.DEBUG)
        
        # Remove existing handlers to avoid duplicates
        self.logger.handlers.clear()
        
        # Create rotating file handler
        # Files will be named: app.log, app.log.1, app.log.2, etc.
        rotating_handler = RotatingFileHandler(
            filename=log_filepath,
            maxBytes=self.max_file_size,
            backupCount=self.backup_count,
            encoding='utf-8'
        )
        rotating_handler.setLevel(logging.DEBUG)
        
        # Create console handler for important messages
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        
        # Create detailed formatter for file logs
        timezone_suffix = " UTC" if self.use_utc else ""
        file_formatter = UTCFormatter(
            fmt=f'%(asctime)s{timezone_suffix} | %(levelname)-8s | %(name)s | %(message)s',
            # fmt=f'%(asctime)s{timezone_suffix} | %(levelname)-8s | %(filename)s:%(lineno)d | %(funcName)s() | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S',
            use_utc=self.use_utc
        )
        
        # Create simpler formatter for console logs
        console_formatter = UTCFormatter(
            fmt=f'%(asctime)s{timezone_suffix} | %(levelname)-8s | %(name)s | %(message)s',
            # fmt=f'%(asctime)s{timezone_suffix} | %(levelname)-8s | %(filename)s | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S',
            use_utc=self.use_utc
        )
        
        # Set formatters for handlers
        rotating_handler.setFormatter(file_formatter)
        console_handler.setFormatter(console_formatter)
        
        # Add handlers to logger
        self.logger.addHandler(rotating_handler)
        self.logger.addHandler(console_handler)
        
        # Log the logger initialization
        timezone_info = "UTC" if self.use_utc else "local time"
        self.logger.debug(f"Logger '{self.name}' initialized with rotating file handler (max_size={self.max_file_size/1024/1024:.1f}MB, backup_count={self.backup_count}, timezone={timezone_info})")


    def debug(self, message: str):
        """Log debug message."""
        self.logger.debug(message)
    
    def info(self, message: str):
        """Log info message."""
        self.logger.info(message)
    
    def warning(self, message: str):
        """Log warning message."""
        self.logger.warning(message)
    
    def error(self, message: str):
        """Log error message."""
        self.logger.error(message)
    
    def critical(self, message: str):
        """Log critical message."""
        self.logger.critical(message)
    
    def exception(self, message: str):
        """Log exception with traceback."""
        self.logger.exception(message)
        


class UTCFormatter(logging.Formatter):
    """Custom formatter that can use either UTC or local time."""
    
    def __init__(self, fmt=None, datefmt=None, use_utc=True):
        super().__init__(fmt, datefmt)
        self.use_utc = use_utc
    
    def formatTime(self, record, datefmt=None):
        """Override formatTime to use UTC or local time based on configuration."""
        if self.use_utc:
            # Convert to UTC
            dt = datetime.fromtimestamp(record.created, tz=timezone.utc)
        else:
            # Use local time
            dt = datetime.fromtimestamp(record.created)
        
        if datefmt:
            return dt.strftime(datefmt)
        else:
            return dt.isoformat()
    


def get_logger(filename: str = None, max_file_size: int = MAX_LOG_FILE_SIZE, backup_count: int = LOG_FILE_BACKUP_COUNT, use_utc: bool = LOG_UTC_FORMAT) -> CustomLogger:
    """
    Get a logger instance for the calling file with rotating file handler.
    
    Args:
        filename: Optional filename. If not provided, tries to get from caller.
        max_file_size: Maximum file size in MB before rotation (default: 10MB)
        backup_count: Number of backup files to keep (default: 5)
        use_utc: Use UTC timestamps instead of local time (default: True, recommended)
        
    Returns:
        CustomLogger instance
    """
    if filename is None:
        # Try to get the calling file name
        import inspect
        frame = inspect.currentframe()
        try:
            caller_frame = frame.f_back
            filename = os.path.basename(caller_frame.f_code.co_filename)
        finally:
            del frame
    
    # Remove .py extension for cleaner logger name
    logger_name = Path(filename).stem if filename else "unknown"
    
    return CustomLogger(logger_name, max_file_size=max_file_size, backup_count=backup_count, use_utc=use_utc)


# # Example usage and testing
# if __name__ == "__main__":
#     print("Testing UTC logger (recommended for production):")
#     # Test with UTC (recommended)
#     utc_logger = get_logger("test_utc.py", max_file_size=1, backup_count=3, use_utc=True)
#     utc_logger.info("UTC Logger started - this timestamp is in UTC")
    
#     print("\nTesting local time logger:")
#     # Test with local time
#     local_logger = get_logger("test_local.py", max_file_size=1, backup_count=3, use_utc=False)
#     local_logger.info("Local Logger started - this timestamp is in local time")
    
#     # Show the difference
#     print(f"\nCurrent UTC time: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}")
#     print(f"Current local time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
#     utc_logger.debug("This is a debug message in UTC")
#     utc_logger.warning("This is a warning message in UTC")
#     utc_logger.error("This is an error message in UTC")
    
#     try:
#         # Test exception logging
#         raise ValueError("Test exception for UTC logging")
#     except Exception as e:
#         utc_logger.exception("Exception occurred during UTC testing")
    
#     utc_logger.critical("This is a critical message in UTC")
#     print("\nLogger test completed. Check the logs directory for output files.")
#     print("Compare the timestamps between UTC and local time log files.")