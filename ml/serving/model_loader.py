"""ml/serving/model_loader.py

Thread-safe model cache with background hot-reload from MLflow Model Registry.

Features:
  - Loads "Production" model on startup; falls back to "Staging".
  - Background thread checks for a new Production version every CHECK_INTERVAL_SEC.
  - Hot-swap: new model replaces cached one atomically (no downtime).
  - If new model fails to load, previous version stays active.
"""

from __future__ import annotations

import logging
import os
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import mlflow.pyfunc
from mlflow.tracking import MlflowClient

logger = logging.getLogger(__name__)

_DEFAULT_MODEL_NAME = os.environ.get("MODEL_NAME", "anomaly-detector-fx")
_CHECK_INTERVAL_SEC = int(os.environ.get("MODEL_RELOAD_INTERVAL_SEC", "300"))  # 5 min


@dataclass
class _ModelState:
    model: Any = None
    version: str = "unknown"
    stage: str = "unknown"
    loaded_at: datetime = field(default_factory=lambda: datetime.now(tz=timezone.utc))
    run_id: str = "unknown"


class ModelLoader:
    """Thread-safe model loader with background hot-reload.

    Usage:
        loader = ModelLoader(model_name="anomaly-detector-fx")
        loader.start()            # begins background reload thread

        model = loader.model      # safe concurrent access
        version = loader.version
    """

    def __init__(
        self,
        model_name: str = _DEFAULT_MODEL_NAME,
        check_interval_sec: int = _CHECK_INTERVAL_SEC,
    ) -> None:
        self._model_name = model_name
        self._check_interval = check_interval_sec
        self._lock = threading.RLock()
        self._state = _ModelState()
        self._client = MlflowClient()
        self._stop_event = threading.Event()
        self._reload_thread: threading.Thread | None = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    @property
    def model(self) -> Any:
        with self._lock:
            return self._state.model

    @property
    def version(self) -> str:
        with self._lock:
            return self._state.version

    @property
    def loaded_at(self) -> datetime:
        with self._lock:
            return self._state.loaded_at

    @property
    def is_ready(self) -> bool:
        with self._lock:
            return self._state.model is not None

    def start(self) -> None:
        """Load model immediately, then start background reload thread."""
        self._load_best_available()
        self._reload_thread = threading.Thread(
            target=self._background_reload_loop,
            name="model-reload",
            daemon=True,
        )
        self._reload_thread.start()
        logger.info("Background model reload started (interval=%ds)", self._check_interval)

    def stop(self) -> None:
        """Stop the background reload thread."""
        self._stop_event.set()
        if self._reload_thread:
            self._reload_thread.join(timeout=5)

    # ------------------------------------------------------------------
    # Internal load helpers
    # ------------------------------------------------------------------

    def _load_best_available(self) -> bool:
        """Try Production, fall back to Staging. Return True if loaded."""
        for stage in ("Production", "Staging"):
            if self._try_load(stage):
                return True
        logger.error("No model available in Production or Staging stage.")
        return False

    def _try_load(self, stage: str) -> bool:
        model_uri = f"models:/{self._model_name}/{stage}"
        try:
            logger.info("Loading model from %s…", model_uri)
            new_model = mlflow.pyfunc.load_model(model_uri)

            # Resolve version + run_id from registry
            versions = self._client.get_latest_versions(self._model_name, stages=[stage])
            version = versions[0].version if versions else "unknown"
            run_id = versions[0].run_id if versions else "unknown"

            new_state = _ModelState(
                model=new_model,
                version=version,
                stage=stage,
                loaded_at=datetime.now(tz=timezone.utc),
                run_id=run_id,
            )
            with self._lock:
                self._state = new_state

            logger.info("Model loaded: name=%s stage=%s version=%s", self._model_name, stage, version)
            return True

        except Exception as exc:
            logger.warning("Failed to load %s: %s", model_uri, exc)
            return False

    def _current_production_version(self) -> str | None:
        """Return current Production version string, or None if not found."""
        try:
            versions = self._client.get_latest_versions(self._model_name, stages=["Production"])
            return versions[0].version if versions else None
        except Exception:
            return None

    # ------------------------------------------------------------------
    # Background loop
    # ------------------------------------------------------------------

    def _background_reload_loop(self) -> None:
        while not self._stop_event.wait(timeout=self._check_interval):
            try:
                latest = self._current_production_version()
                with self._lock:
                    current = self._state.version

                if latest and latest != current:
                    logger.info(
                        "New Production version detected: %s → %s. Hot-reloading…",
                        current, latest,
                    )
                    # Keep old state if new load fails
                    old_state = self._state
                    if not self._try_load("Production"):
                        with self._lock:
                            self._state = old_state
                        logger.warning("Hot-reload failed; keeping version %s.", current)
                    else:
                        logger.info("Hot-reload complete. Now serving version %s.", latest)
                else:
                    logger.debug("Model up-to-date (version=%s).", current)

            except Exception as exc:
                logger.error("Error in background reload loop: %s", exc)
