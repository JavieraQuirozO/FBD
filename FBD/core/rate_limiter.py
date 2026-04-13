import time
import json
from FBD.core.config import Config


class RateLimiter:
    """
    Rate limiter local basado en ventana deslizante de timestamps persistidos en caché.
    Guarda el historial en un archivo JSON para que el límite sobreviva entre sesiones.
    El límite real sobre el servidor se aplica en la edge function por IP.
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
        """
        Verifica si se puede realizar una descarga. Lanza RuntimeError si se
        supera el límite configurado en Config.
        """
        if not Config.DOWNLOAD_RATE_LIMIT_ENABLED:
            return

        now   = time.time()
        calls = self._load()
        calls = [t for t in calls if now - t < Config.DOWNLOAD_WINDOW_SECONDS]

        if len(calls) >= Config.DOWNLOAD_MAX_CALLS:
            raise RuntimeError(
                f"Límite de descargas alcanzado: "
                f"{Config.DOWNLOAD_MAX_CALLS} por "
                f"{Config.DOWNLOAD_WINDOW_SECONDS // 60} minutos."
            )

        calls.append(now)
        self._save(calls)
