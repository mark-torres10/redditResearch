"""Creates wrapper logger class."""
import logging
from typing import Any, Dict

# mypy: ignore-errors
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
    handlers=[logging.StreamHandler()],
)

# mypy: ignore-errors
class RedditLogger(logging.Logger):
    def __init__(self, name: str, level: int = logging.INFO) -> None:
        super().__init__(name, level)

    def log(self, message: str, **kwargs: Dict[str, Any]) -> None:
        """Convenience method to log a message with some extra data."""
        self.info(message, extra=kwargs)
