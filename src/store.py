"""
C4 - Stockage des données dans SQLite
======================================
- Création du schéma de base de données (tables, types, clés)
- Injection des données nettoyées
- Index pour performance des requêtes
- Justification du choix SQLite
"""

import logging
import sqlite3
import sys
from pathlib import Path

import pandas as pd

# ---------------------------------------------------------------------------
# Chemins  (src/ → parent.parent → racine projet)
# ---------------------------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent.parent
PROCESSED_DIR = BASE_DIR / "data" / "processed"
DB_PATH = BASE_DIR / "data" / "finess_occitanie.db"

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Schéma SQL
# ---------------------------------------------------------------------------

SQL_SCHEMA = """
-- ============================================================
-- FINESS Occitanie - Schéma de base de données
-- ============================================================

-- Table principale : établissements Occitanie
CREATE TABLE IF NOT EXISTS etablissements (
    nofinesset          TEXT PRIMARY KEY,
    nofinessej          TEXT,
    rs                  TEXT,
    rslongue            TEXT,
    departement         TEXT NOT NULL,
    libdepartement      TEXT,
    commune             TEXT,
    code_insee          TEXT,
    libcategetab        TEXT,
    libcategagretab     TEXT,
    numvoie             TEXT,
    typvoie             TEXT,
    voie                TEXT,
    compvoie            TEXT,
    complrs             TEXT,
    compldistrib        TEXT,
    lieuditbp           TEXT,
    coordxet            REAL,
    coordyet            REAL,
    longitude           REAL,
    latitude            REAL,
    nom_commune         TEXT,
    population          INTEGER,
    region              TEXT DEFAULT 'Occitanie',
    groupe              TEXT
    );
-- Table des équipements sociaux (détail)
CREATE TABLE IF NOT EXISTS equipements (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    nofinesset          TEXT NOT NULL,
    nofinessej          TEXT,
    libde               TEXT,
    libta               TEXT,
    libclient           TEXT,
    capinstot           REAL,
    groupe_equipement   TEXT,
    departement         TEXT,
    libdepartement      TEXT,
    commune             TEXT,
    code_insee          TEXT,
    latitude            REAL,
    longitude           REAL,
    FOREIGN KEY (nofinesset) REFERENCES etablissements(nofinesset)
);
-- Table des activités de soins (détail)
CREATE TABLE IF NOT EXISTS activites (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    nofinesset          TEXT NOT NULL,
    nofinessej          TEXT,
    rsej                TEXT,
    libactivite         TEXT,
    libmodalite         TEXT,
    libforme            TEXT,
    datefin             TEXT,
    groupe_activites    TEXT,
    departement         TEXT,
    libdepartement      TEXT,
    commune             TEXT,
    code_insee          TEXT,
    latitude            REAL,
    longitude           REAL,
FOREIGN KEY (nofinesset) REFERENCES etablissements(nofinesset)
);
-- Table de référence des départements
CREATE TABLE IF NOT EXISTS departements (
    code_dept   TEXT PRIMARY KEY,
    nom_dept    TEXT NOT NULL,
    population  INTEGER,
    region      TEXT DEFAULT 'Occitanie'
    );
 
    """


SQL_INDEXES = """
CREATE INDEX IF NOT EXISTS idx_etab_dept       ON etablissements(departement);
CREATE INDEX IF NOT EXISTS idx_etab_code_insee ON etablissements(code_insee);
CREATE INDEX IF NOT EXISTS idx_equip_nofiness  ON equipements(nofinesset);
CREATE INDEX IF NOT EXISTS idx_equip_dept      ON equipements(departement);
CREATE INDEX IF NOT EXISTS idx_equip_groupe    ON equipements(groupe_equipement);
CREATE INDEX IF NOT EXISTS idx_activ_nofiness  ON activites(nofinesset);
CREATE INDEX IF NOT EXISTS idx_activ_libelle   ON activites(libactivite);
CREATE INDEX IF NOT EXISTS idx_activ_groupe    ON activites(groupe_activites);
CREATE INDEX IF NOT EXISTS idx_activ_dept      ON activites(departement);
"""

DEPARTEMENTS_OCCITANIE = [
    ("09", "Ariège"), ("11", "Aude"), ("12", "Aveyron"), ("30", "Gard"),
    ("31", "Haute-Garonne"), ("32", "Gers"), ("34", "Hérault"), ("46", "Lot"),
    ("48", "Lozère"), ("65", "Hautes-Pyrénées"), ("66", "Pyrénées-Orientales"),
    ("81", "Tarn"), ("82", "Tarn-et-Garonne"),
]


# ---------------------------------------------------------------------------
# Fonctions
# ---------------------------------------------------------------------------

def create_schema(conn: sqlite3.Connection):
    conn.executescript(SQL_SCHEMA)
    conn.executescript(SQL_INDEXES)
    conn.commit()
    logger.info("  ✓ Tables et index créés")

def insert_departements(conn: sqlite3.Connection):
    path = DB_PATH.parent.parent / "data" / "raw" / "communes-france-2025.csv"
    occ = {"09","11","12","30","31","32","34","46","48","65","66","81","82"}

    pop_by_dept = {}
    if path.exists():
        df = pd.read_csv(path, low_memory=False, encoding="utf-8")
        df["dep_code"] = df["dep_code"].astype(str).str.strip().str.zfill(2)
        df_occ = df[df["dep_code"].isin(occ)]
        pop_by_dept = df_occ.groupby("dep_code")["population"].sum().to_dict()
        logger.info(f"  Lignes Occitanie trouvées : {len(df_occ)}")
    else:
        logger.warning(f"  ⚠ {path} absent, population non renseignée")

    rows = [
        (code, nom, pop_by_dept.get(code))
        for code, nom in DEPARTEMENTS_OCCITANIE
    ]
    conn.executemany(
        "INSERT OR REPLACE INTO departements (code_dept, nom_dept, population) VALUES (?, ?, ?)",
        rows,
    )
    conn.commit()
    logger.info(f"  ✓ {len(rows)} départements insérés (avec population)")


def load_and_insert(conn: sqlite3.Connection):
    """Charge les CSV nettoyés (data/processed/) et les insère dans SQLite."""

    files = {
        "etablissements": PROCESSED_DIR / "etablissements_occitanie.csv",
        "equipements":    PROCESSED_DIR / "equipements_occitanie.csv",
        "activites":      PROCESSED_DIR / "activites_occitanie.csv",
    }

    numeric_cols = {
        "etablissements": ["coordxet", "coordyet", "longitude", "latitude"],
        "equipements":    ["capinstot", "longitude", "latitude"],
        "activites":      ["longitude", "latitude"],
    }

    for table, path in files.items():
        if not path.exists():
            logger.warning(f"  ⚠ Fichier absent : {path}")
            continue
        df = pd.read_csv(path, dtype=str)
        for col in numeric_cols.get(table, []):
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        df.to_sql(table, conn, if_exists="replace", index=False)
        logger.info(f"  ✓ {table} : {len(df)} lignes insérées")


def run_validation_queries(conn: sqlite3.Connection):
    queries = {
        "Total établissements":
            "SELECT COUNT(*) as total FROM etablissements",
        "Par département":
            "SELECT departement, libdepartement, COUNT(*) as nb FROM etablissements GROUP BY departement ORDER BY nb DESC",
        "Établissements avec activités soins":
            "SELECT COUNT(DISTINCT nofinesset) as nb FROM activites",
        "Top 5 groupes équipements":
            "SELECT groupe_equipement, COUNT(*) as nb FROM equipements GROUP BY groupe_equipement ORDER BY nb DESC LIMIT 5",
        "Top 5 groupes activités":
            "SELECT groupe_activites, COUNT(*) as nb FROM activites GROUP BY groupe_activites ORDER BY nb DESC LIMIT 5",
        "Taux géolocalisation (%)":
            "SELECT ROUND(100.0 * SUM(CASE WHEN latitude IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 1) as pct FROM etablissements",
    }
    for label, sql in queries.items():
        try:
            result = pd.read_sql(sql, conn)
            logger.info(f"\n  [{label}]\n{result.to_string(index=False)}")
        except Exception as e:
            logger.error(f"  Erreur requête '{label}': {e}")


# ---------------------------------------------------------------------------
# Pipeline principal
# ---------------------------------------------------------------------------

def store():
    logger.info("=" * 60)
    logger.info("DÉMARRAGE DU STOCKAGE SQLite")
    logger.info(f"Base de données : {DB_PATH}")
    logger.info("=" * 60)

    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    if DB_PATH.exists():
        DB_PATH.unlink()
        logger.info("  Ancienne base supprimée.")

    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")

    try:
        create_schema(conn)
        insert_departements(conn)
        load_and_insert(conn)
        run_validation_queries(conn)
        logger.info(f"\n✓ Base créée : {DB_PATH} ({DB_PATH.stat().st_size / 1024:.1f} Ko)")
    except Exception as e:
        logger.error(f"Erreur critique : {e}")
        conn.rollback()
        raise
    finally:
        conn.close()

    logger.info("=" * 60)
    logger.info("STOCKAGE TERMINÉ")
    logger.info("=" * 60)


if __name__ == "__main__":
    store()
