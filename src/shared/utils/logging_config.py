import logging
from datetime import datetime
from pathlib import Path


class Colors:
    RESET = "\033[0m"
    BOLD = "\033[1m"
    RED = "\033[91m"
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    BLUE = "\033[94m"
    MAGENTA = "\033[95m"
    CYAN = "\033[96m"
    WHITE = "\033[97m"
    GRAY = "\033[90m"


class ColoredFormatter(logging.Formatter):
    FORMATS = {
        logging.DEBUG: f"{Colors.GRAY}%(asctime)s | %(levelname)-8s | %(funcName)-20s | %(message)s{Colors.RESET}",
        logging.INFO: f"{Colors.GREEN}%(asctime)s | %(levelname)-8s | %(message)s{Colors.RESET}",
        logging.WARNING: f"{Colors.YELLOW}%(asctime)s | %(levelname)-8s | %(funcName)-20s | %(message)s{Colors.RESET}",
        logging.ERROR: f"{Colors.RED}%(asctime)s | %(levelname)-8s | %(funcName)-20s | %(message)s{Colors.RESET}",
        logging.CRITICAL: f"{Colors.RED}{Colors.BOLD}%(asctime)s | %(levelname)-8s | %(funcName)-20s | %(message)s{Colors.RESET}",
    }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt, datefmt="%H:%M:%S")
        return formatter.format(record)


def setup_logging(name="lol_odds", log_dir="logs"):
    log_dir = Path(log_dir)
    log_dir.mkdir(exist_ok=True)
    log_file = log_dir / f"{name}_{datetime.now():%Y%m%d_%H%M%S}.log"

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    for handler in logger.handlers[:]:
        logger.removeHandler(handler)

    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_formatter = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(funcName)-20s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    file_handler.setFormatter(file_formatter)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(ColoredFormatter())

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger
