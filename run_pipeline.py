"""
Pipeline ETL principal - FINESS Occitanie
==========================================
Lance les 3 étapes dans l'ordre :
  1. Collecte (C1)          → src/collect.py
  2. Préparation (C2/C3)    → src/prepare.py
  3. Stockage SQLite (C4)   → src/store.py

Usage :
    python run_pipeline.py              # Utilise les fichiers locaux
    python run_pipeline.py --download   # Force le téléchargement
"""

import argparse
import importlib
import logging
import sys
import time
from pathlib import Path

# Ajoute src/ au path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from collect import collect
from store import store

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


def run_pipeline(force_download: bool = False):
    start = time.time()

    logger.info("╔══════════════════════════════════════════════════════╗")
    logger.info("║      PIPELINE ETL - FINESS OCCITANIE                ║")
    logger.info("╚══════════════════════════════════════════════════════╝")

    # ------------------------------------------------------------------
    # Étape 1 : Collecte
    # ------------------------------------------------------------------
    logger.info("\n[1/3] COLLECTE DES DONNÉES")
    results = collect(force_download=force_download)
    errors = [k for k, v in results.items() if not v.startswith("ok")]
    if errors:
        logger.warning(f"Fichiers en erreur : {errors} — poursuite avec les fichiers existants.")

    # ------------------------------------------------------------------
    # Étape 2 : Préparation
    # prepare.py s'exécute à l'import (code au niveau module).
    # On utilise importlib.reload pour forcer la ré-exécution si le
    # module était déjà chargé.
    # ------------------------------------------------------------------
    logger.info("\n[2/3] NETTOYAGE ET PRÉPARATION")
    try:
        import prepare as _prep
        importlib.reload(_prep)
        logger.info("  → CSV exportés dans data/processed/")
    except Exception as e:
        logger.error(f"Erreur lors de la préparation : {e}")
        raise

    # ------------------------------------------------------------------
    # Étape 3 : Stockage
    # ------------------------------------------------------------------
    logger.info("\n[3/3] STOCKAGE SQLite")
    store()

    elapsed = time.time() - start
    logger.info(f"\n✓ Pipeline terminé en {elapsed:.1f}s")
    logger.info("Pour lancer l'API : uvicorn api.api:app --reload")
    logger.info("Swagger UI        : http://localhost:8000/docs")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pipeline ETL FINESS Occitanie")
    parser.add_argument("--download", action="store_true", help="Force le téléchargement des sources")
    args = parser.parse_args()
    run_pipeline(force_download=args.download)
