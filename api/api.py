"""
C5 - API FastAPI - Mise à disposition des données FINESS Occitanie
===================================================================
- Sécurisation par clé API (header X-API-Key)
- Documentation Swagger auto-générée sur /docs
- Endpoints pour établissements, équipements, activités, stats
"""

import os
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Optional

import pandas as pd
from fastapi import Depends, FastAPI, HTTPException, Query, Security
from fastapi.security.api_key import APIKeyHeader
from pydantic import BaseModel

# ---------------------------------------------------------------------------
# Configuration  (api/ → parent.parent → racine projet)
# ---------------------------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "data" / "finess_occitanie.db"

API_KEY = os.getenv("FINESS_API_KEY", "finess-occitanie-2025-demo")
API_KEY_NAME = "X-API-Key"

# ---------------------------------------------------------------------------
# Application FastAPI
# ---------------------------------------------------------------------------

app = FastAPI(
    title="API FINESS Occitanie",
    description="""
## API d'accès aux données FINESS - Région Occitanie

Cette API expose les données des établissements sanitaires et médico-sociaux
de la région Occitanie, issues du Fichier National des Établissements
Sanitaires et Sociaux (FINESS).

### Authentification
Toutes les routes (sauf `/health`) nécessitent une clé API dans le header :
```
X-API-Key: finess-occitanie-2025-demo
```

### Sources de données
- **FINESS établissements** : données géographiques et administratives
- **FINESS équipements sociaux** : structures médicosociales, capacités
- **FINESS activités de soins** : activités autorisées par établissement
- **INSEE communes 2025** : référentiel géographique

### Région couverte
Occitanie — 13 départements : 09, 11, 12, 30, 31, 32, 34, 46, 48, 65, 66, 81, 82
    """,
    version="1.0.0",
    license_info={"name": "Données ouvertes - data.gouv.fr"},
)

# ---------------------------------------------------------------------------
# Sécurité
# ---------------------------------------------------------------------------

api_key_header = APIKeyHeader(name=API_KEY_NAME, auto_error=False)


async def verify_api_key(api_key: str = Security(api_key_header)):
    if api_key != API_KEY:
        raise HTTPException(
            status_code=403,
            detail="Clé API invalide ou manquante. Passez votre clé dans le header X-API-Key.",
        )
    return api_key


# ---------------------------------------------------------------------------
# Base de données
# ---------------------------------------------------------------------------

@contextmanager
def get_db():
    if not DB_PATH.exists():
        raise HTTPException(
            status_code=503,
            detail=f"Base de données introuvable : {DB_PATH}. Exécutez d'abord le pipeline ETL.",
        )
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()


def query_db(sql: str, params: tuple = ()) -> list[dict]:
    with get_db() as conn:
        df = pd.read_sql(sql, conn, params=params)
        return df.to_dict(orient="records")


# ---------------------------------------------------------------------------
# Modèles Pydantic
# ---------------------------------------------------------------------------

class Etablissement(BaseModel):
    nofinesset: str
    nofinessej: Optional[str] = None
    rs: Optional[str] = None
    rslongue: Optional[str] = None
    departement: str
    libdepartement: Optional[str] = None
    commune: Optional[str] = None
    code_insee: Optional[str] = None
    libcategetab: Optional[str] = None
    libcategagretab: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    nom_commune: Optional[str] = None
    population: Optional[int] = None
    region: Optional[str] = None


class Equipement(BaseModel):
    nofinesset: str
    nofinessej: Optional[str] = None
    libde: Optional[str] = None
    libta: Optional[str] = None
    libclient: Optional[str] = None
    capinstot: Optional[float] = None
    groupe_equipement: Optional[str] = None
    departement: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None


class Activite(BaseModel):
    nofinesset: str
    nofinessej: Optional[str] = None
    rsej: Optional[str] = None
    libactivite: Optional[str] = None
    libmodalite: Optional[str] = None
    libforme: Optional[str] = None
    datefin: Optional[str] = None
    groupe_activites: Optional[str] = None
    departement: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None


class StatsDepartement(BaseModel):
    departement: str
    libdepartement: Optional[str] = None
    nb_etablissements: int
    nb_equipements: int
    capacite_totale: Optional[float] = None
    nb_activites: int


class StatsRegion(BaseModel):
    total_etablissements: int
    total_equipements: int
    total_activites: int
    capacite_totale: Optional[float] = None
    nb_departements: int
    nb_geolocalisés: int


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.get("/health", tags=["Système"])
def health_check():
    db_ok = DB_PATH.exists()
    return {"status": "ok" if db_ok else "degraded", "database_exists": db_ok}


@app.get("/etablissements", response_model=list[Etablissement], tags=["Établissements"],
         summary="Liste des établissements Occitanie", dependencies=[Depends(verify_api_key)])
def get_etablissements(
    departement: Optional[str] = Query(None, description="Code département (ex: 31)"),
    categorie: Optional[str] = Query(None, description="Recherche partielle dans libcategetab"),
    avec_coordonnees: Optional[bool] = Query(None, description="Filtrer sur établissements géolocalisés"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    """Liste paginée des établissements avec filtres optionnels."""
    conditions, params = ["1=1"], []
    if departement:
        conditions.append("departement = ?")
        params.append(departement.zfill(2))
    if categorie:
        conditions.append("libcategetab LIKE ?")
        params.append(f"%{categorie}%")
    if avec_coordonnees is True:
        conditions.append("latitude IS NOT NULL AND longitude IS NOT NULL")
    elif avec_coordonnees is False:
        conditions.append("(latitude IS NULL OR longitude IS NULL)")
    params.extend([limit, offset])
    return query_db(
        f"SELECT * FROM etablissements WHERE {' AND '.join(conditions)} ORDER BY departement, nofinesset LIMIT ? OFFSET ?",
        tuple(params)
    )


@app.get("/etablissements/{nofinesset}", response_model=Etablissement, tags=["Établissements"],
         summary="Détail d'un établissement", dependencies=[Depends(verify_api_key)])
def get_etablissement(nofinesset: str):
    results = query_db("SELECT * FROM etablissements WHERE nofinesset = ?", (nofinesset.zfill(9),))
    if not results:
        raise HTTPException(status_code=404, detail=f"Établissement {nofinesset} non trouvé")
    return results[0]


@app.get("/etablissements/{nofinesset}/equipements", response_model=list[Equipement],
         tags=["Établissements"], dependencies=[Depends(verify_api_key)])
def get_etablissement_equipements(nofinesset: str):
    return query_db("SELECT * FROM equipements WHERE nofinesset = ?", (nofinesset.zfill(9),))


@app.get("/etablissements/{nofinesset}/activites", response_model=list[Activite],
         tags=["Établissements"], dependencies=[Depends(verify_api_key)])
def get_etablissement_activites(nofinesset: str):
    return query_db("SELECT * FROM activites WHERE nofinesset = ?", (nofinesset.zfill(9),))


@app.get("/equipements", response_model=list[Equipement], tags=["Équipements"],
         summary="Liste des équipements sociaux", dependencies=[Depends(verify_api_key)])
def get_equipements(
    departement: Optional[str] = Query(None),
    groupe: Optional[str] = Query(None, description="Groupe (Personnes âgées, Enfance, Handicap…)"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    conditions, params = ["1=1"], []
    if departement:
        conditions.append("departement = ?"); params.append(departement.zfill(2))
    if groupe:
        conditions.append("groupe_equipement = ?"); params.append(groupe)
    params.extend([limit, offset])
    return query_db(
        f"SELECT * FROM equipements WHERE {' AND '.join(conditions)} ORDER BY departement LIMIT ? OFFSET ?",
        tuple(params)
    )


@app.get("/activites", response_model=list[Activite], tags=["Activités de soins"],
         summary="Liste des activités de soins", dependencies=[Depends(verify_api_key)])
def get_activites(
    departement: Optional[str] = Query(None),
    groupe: Optional[str] = Query(None, description="Groupe (Chirurgie, Médecine, Psychiatrie…)"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    conditions, params = ["1=1"], []
    if departement:
        conditions.append("departement = ?"); params.append(departement.zfill(2))
    if groupe:
        conditions.append("groupe_activites = ?"); params.append(groupe)
    params.extend([limit, offset])
    return query_db(
        f"SELECT * FROM activites WHERE {' AND '.join(conditions)} ORDER BY departement LIMIT ? OFFSET ?",
        tuple(params)
    )


@app.get("/stats/region", response_model=StatsRegion, tags=["Statistiques"],
         summary="Statistiques globales Occitanie", dependencies=[Depends(verify_api_key)])
def get_stats_region():
    results = query_db("""
        SELECT
            (SELECT COUNT(*) FROM etablissements)                                      AS total_etablissements,
            (SELECT COUNT(*) FROM equipements)                                         AS total_equipements,
            (SELECT COUNT(*) FROM activites)                                           AS total_activites,
            (SELECT SUM(capinstot) FROM equipements)                                   AS capacite_totale,
            (SELECT COUNT(DISTINCT departement) FROM etablissements)                   AS nb_departements,
            (SELECT COUNT(*) FROM etablissements WHERE latitude IS NOT NULL)           AS nb_geolocalisés
    """)
    return results[0]


@app.get("/stats/departements", response_model=list[StatsDepartement], tags=["Statistiques"],
         summary="Statistiques par département", dependencies=[Depends(verify_api_key)])
def get_stats_departements():
    return query_db("""
        SELECT
            e.departement,
            e.libdepartement,
            COUNT(DISTINCT e.nofinesset) AS nb_etablissements,
            COUNT(eq.id)                 AS nb_equipements,
            SUM(eq.capinstot)            AS capacite_totale,
            COUNT(ac.id)                 AS nb_activites
        FROM etablissements e
        LEFT JOIN equipements eq ON e.nofinesset = eq.nofinesset
        LEFT JOIN activites   ac ON e.nofinesset = ac.nofinesset
        GROUP BY e.departement
        ORDER BY nb_etablissements DESC
    """)


@app.get("/stats/groupes-equipements", tags=["Statistiques"],
         summary="Répartition par groupe d'équipements", dependencies=[Depends(verify_api_key)])
def get_stats_groupes_equipements():
    return query_db("""
        SELECT groupe_equipement, COUNT(*) AS nb, SUM(capinstot) AS capacite
        FROM equipements WHERE groupe_equipement IS NOT NULL
        GROUP BY groupe_equipement ORDER BY nb DESC
    """)


@app.get("/stats/groupes-activites", tags=["Statistiques"],
         summary="Répartition par groupe d'activités", dependencies=[Depends(verify_api_key)])
def get_stats_groupes_activites():
    return query_db("""
        SELECT groupe_activites, COUNT(*) AS nb
        FROM activites WHERE groupe_activites IS NOT NULL
        GROUP BY groupe_activites ORDER BY nb DESC
    """)


@app.get("/activites/top", tags=["Activités de soins"],
         summary="Top activités les plus fréquentes", dependencies=[Depends(verify_api_key)])
def get_top_activites(limit: int = Query(10, ge=1, le=50)):
    return query_db(
        "SELECT libactivite, COUNT(*) as nb FROM activites WHERE libactivite IS NOT NULL GROUP BY libactivite ORDER BY nb DESC LIMIT ?",
        (limit,)
    )


@app.get("/departements", tags=["Référentiels"],
         summary="Liste des départements Occitanie", dependencies=[Depends(verify_api_key)])
def get_departements():
    return query_db("SELECT * FROM ref_departements ORDER BY code_dept")


# ---------------------------------------------------------------------------
# Point d'entrée
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("api:app", host="0.0.0.0", port=8000, reload=True)
