import time
import json
from FBD.core.config import Config


class RateLimiter:
    """
    Local rate limiter based on timestamps stored in cache.
    """

    def __init__(self, name="download"):
        self.name = name
        self.path = Config.CACHE_DIR / f"{name}_rate.json"

    def _load(self):
        if self.path.exists():
            try:
                return json.loads(self.path.read_text())
            except Exception:
                return []
        return []

    def _save(self, calls):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps(calls))

    def check(self):
        if not Config.DOWNLOAD_RATE_LIMIT_ENABLED:
            return

        now = time.time()
        calls = self._load()

        # keep only calls in window
        calls = [
            t for t in calls
            if now - t < Config.DOWNLOAD_WINDOW_SECONDS
        ]

        if len(calls) >= Config.DOWNLOAD_MAX_CALLS:
            raise RuntimeError(
                f"Download limit reached: "
                f"{Config.DOWNLOAD_MAX_CALLS} downloads per "
                f"{Config.DOWNLOAD_WINDOW_SECONDS // 60} minutes."
            )

        calls.append(now)
        self._save(calls)

