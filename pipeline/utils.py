"""Utilitaires transverses : journalisation et mesure de duree.

Les temps affiches par `step()` servent aussi de mesures pour la soutenance
(cout de l'extraction, du chargement, de la conversion Parquet, etc.).
"""

from __future__ import annotations

import logging
import sys
import time
from contextlib import contextmanager
from typing import Iterator


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter("%(asctime)s  %(levelname)-7s %(message)s", "%H:%M:%S"))
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False
    return logger


@contextmanager
def step(logger: logging.Logger, label: str) -> Iterator[None]:
    """Encadre une etape du pipeline en journalisant sa duree."""
    logger.info("--> %s", label)
    started = time.perf_counter()
    try:
        yield
    except Exception:
        logger.error("!!! %s a echoue apres %.1f s", label, time.perf_counter() - started)
        raise
    logger.info("    %s termine en %.1f s", label, time.perf_counter() - started)


def human_size(num_bytes: int) -> str:
    value = float(num_bytes)
    for unit in ("o", "Ko", "Mo", "Go"):
        if value < 1024 or unit == "Go":
            return f"{value:.1f} {unit}"
        value /= 1024
    return f"{value:.1f} Go"
