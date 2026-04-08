"""
C1 - Collecte des données FINESS
=================================
Script d'extraction automatisé depuis data.gouv.fr
Gestion des erreurs, logs, vérification d'intégrité.
"""

import os
import sys
import logging
import hashlib
import requests
from datetime import datetime
from pathlib import Path

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DATA_DIR = Path(__file__).resolve().parent.parent / "data" / "raw"
DATA_DIR.mkdir(parents=True, exist_ok=True)

LOG_DIR = Path(__file__).resolve().parent.parent / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

# Sources officielles data.gouv.fr
SOURCES = {
    "finess_equipements_sociaux.csv": {
        "url": "https://www.data.gouv.fr/api/1/datasets/r/5eb67ddc-b58c-4531-a3f2-2c2843cf3986",
        "description": "FINESS equipements sociaux",
        "min_rows": 5000,
    },
    "finess_activites_soins.csv": {
        "url": "https://www.data.gouv.fr/api/1/datasets/r/69cb3e2f-dc0a-4cee-800a-cf98901d257d",
        "description": "FINESS activités de soins",
        "min_rows": 5000,
    },
    #"finess_etablissements.csv": {
    #    "url": "https://www.data.gouv.fr/api/1/datasets/r/98f3161f-79ff-4f16-8f6a-6d571a80fea2",
    #    "description": "FINESS équipements sociaux et médico-sociaux",
    #    "min_rows": 50000,
    #}, ne fonctionne pas actuellement
    "v_commune_2025.csv": {
        "url": "https://www.insee.fr/fr/statistiques/fichier/8377162/v_commune_2025.csv",
        "description": "Communes françaises 2025 (INSEE)",
        "min_rows": 30000,
    },
}

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

log_file = LOG_DIR / f"collect_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Fonctions utilitaires
# ---------------------------------------------------------------------------

def compute_md5(filepath: Path) -> str:
    """Calcule le MD5 d'un fichier pour vérifier son intégrité."""
    hasher = hashlib.md5()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def count_rows(filepath: Path, sep: str = ";") -> int:
    """Compte le nombre de lignes d'un CSV sans le charger entièrement."""
    with open(filepath, "r", encoding="utf-8", errors="replace") as f:
        return sum(1 for _ in f) - 1  # -1 pour l'en-tête


def download_file(url: str, dest: Path, timeout: int = 60) -> bool:
    """
    Télécharge un fichier avec gestion des erreurs HTTP et réseau.
    Retourne True si succès, False sinon.
    """
    logger.info(f"Téléchargement : {url}")
    try:
        response = requests.get(url, timeout=timeout, stream=True)
        response.raise_for_status()

        total = int(response.headers.get("content-length", 0))
        downloaded = 0

        with open(dest, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)

        size_mb = downloaded / (1024 * 1024)
        logger.info(f"  ✓ Téléchargé : {dest.name} ({size_mb:.1f} Mo)")
        return True

    except requests.exceptions.HTTPError as e:
        logger.error(f"  ✗ Erreur HTTP {e.response.status_code} : {url}")
        return False
    except requests.exceptions.ConnectionError:
        logger.error(f"  ✗ Erreur de connexion : impossible d'atteindre {url}")
        return False
    except requests.exceptions.Timeout:
        logger.error(f"  ✗ Timeout ({timeout}s) dépassé pour : {url}")
        return False
    except Exception as e:
        logger.error(f"  ✗ Erreur inattendue : {e}")
        return False


def verify_file(filepath: Path, min_rows: int) -> bool:
    """
    Vérifie l'intégrité d'un fichier CSV :
    - Existence
    - Taille non nulle
    - Nombre de lignes minimum
    """
    if not filepath.exists():
        logger.error(f"  ✗ Fichier absent : {filepath}")
        return False

    if filepath.stat().st_size == 0:
        logger.error(f"  ✗ Fichier vide : {filepath}")
        return False

    rows = count_rows(filepath)
    if rows < min_rows:
        logger.error(f"  ✗ Trop peu de lignes : {rows} < {min_rows} attendues")
        return False

    md5 = compute_md5(filepath)
    logger.info(f"  ✓ Intégrité OK : {rows} lignes | MD5 : {md5}")
    return True


# ---------------------------------------------------------------------------
# Point d'entrée principal
# ---------------------------------------------------------------------------

def collect(force_download: bool = False) -> dict:
    """
    Collecte tous les fichiers sources.
    Si les fichiers existent déjà en local, skip le téléchargement
    sauf si force_download=True.

    Retourne un dictionnaire {filename: status}
    """
    logger.info("=" * 60)
    logger.info("DÉMARRAGE DE LA COLLECTE FINESS")
    logger.info(f"Répertoire de destination : {DATA_DIR}")
    logger.info("=" * 60)

    results = {}
    errors = []

    for filename, config in SOURCES.items():
        dest = DATA_DIR / filename
        logger.info(f"\n→ {config['description']} ({filename})")

        # Skip si fichier déjà présent et valide
        if dest.exists() and not force_download:
            logger.info("  ⏭ Fichier déjà présent, vérification...")
            ok = verify_file(dest, config["min_rows"])
            results[filename] = "ok_existing" if ok else "error_existing"
            if not ok:
                errors.append(filename)
            continue

        # Téléchargement
        success = download_file(config["url"], dest)
        if not success:
            # Tentative alternative : utiliser le fichier local s'il existe
            if dest.exists():
                logger.warning("  ⚠ Téléchargement échoué, utilisation du fichier local existant")
                ok = verify_file(dest, config["min_rows"])
                results[filename] = "ok_fallback" if ok else "error"
            else:
                results[filename] = "error_download"
                errors.append(filename)
            continue

        # Vérification post-téléchargement
        ok = verify_file(dest, config["min_rows"])
        results[filename] = "ok" if ok else "error_integrity"
        if not ok:
            errors.append(filename)

    # Résumé
    logger.info("\n" + "=" * 60)
    logger.info("RÉSUMÉ COLLECTE")
    for fname, status in results.items():
        icon = "✓" if status.startswith("ok") else "✗"
        logger.info(f"  {icon} {fname} : {status}")

    if errors:
        logger.error(f"\n{len(errors)} erreur(s) détectée(s) : {errors}")
    else:
        logger.info("\n✓ Tous les fichiers collectés avec succès.")
    logger.info("=" * 60)

    return results


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Collecte des données FINESS")
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force le re-téléchargement même si les fichiers existent",
    )
    args = parser.parse_args()

    results = collect(force_download=args.force)

    # Code de retour : 0 si succès, 1 si erreurs
    has_errors = any(not s.startswith("ok") for s in results.values())
    sys.exit(1 if has_errors else 0)
