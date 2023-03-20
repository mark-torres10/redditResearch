"""Creates wrapper logger class."""
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s: %(message)s',
    handlers=[logging.StreamHandler()]
)

# built on top of the base Python logger class.
class RedditLogger(logging.Logger):
    def __init__(self, name, level=logging.INFO):
        super().__init__(name, level)
