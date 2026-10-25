"""Log to the Prefect run logger and to a per-task file in logs/."""

import logging

from prefect.exceptions import MissingContextError
from prefect.logging import get_run_logger

from pipeline.config import LOG_DIR


def _file_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(f"pipeline.{name}")
    if not logger.handlers:
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        handler = logging.FileHandler(LOG_DIR / f"{name}.log")
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False
    return logger


class TeeLogger:
    """Sends every message to both the Prefect logger (if in a run) and the task's log file."""

    def __init__(self, name: str):
        self.file = _file_logger(name)
        try:
            self.prefect = get_run_logger()
        except MissingContextError:
            self.prefect = None

    def _log(self, level: int, msg: str) -> None:
        self.file.log(level, msg)
        if self.prefect:
            self.prefect.log(level, msg)

    def info(self, msg: str) -> None:
        self._log(logging.INFO, msg)

    def warning(self, msg: str) -> None:
        self._log(logging.WARNING, msg)

    def error(self, msg: str) -> None:
        self._log(logging.ERROR, msg)
