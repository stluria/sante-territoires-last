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
from enum import Enum
from functools import lru_cache
from pathlib import Path
from typing import Optional

import pandas as pd
from fastapi import Depends, FastAPI, HTTPException, Query, Security
from fastapi.security.api_key import APIKeyHeader
from pydantic import BaseModel

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent.parent
# permet de remonter 2 niveaux au dessus du fichier
DB_PATH = BASE_DIR / "data" / "finess_occitanie.db"

# Clé API (en prod : lire depuis variable d'environnement)
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
Toutes les routes (sauf `/health`) nécessitent une clé API passée dans le header :
```
X-API-Key: finess-occitanie-2025-demo
```

### Sources de données
- **FINESS équipements sociaux** : structures médicosociales, capacités
- **FINESS établissements** : tous les établissements sanitaires
- **FINESS activités de soins** : activités autorisées par établissement
- **INSEE communes 2025** : référentiel géographique

### Région couverte
Occitanie (13 départements) : 09, 11, 12, 30, 31, 32, 34, 46, 48, 65, 66, 81, 82
    """,
    version="1.0.0",
    contact={
        "name": "Projet FINESS Occitanie",
        "url": "https://github.com/stluria/sante-territoires-last",
    },
    license_info={"name": "Données ouvertes - data.gouv.fr"},
)

# ---------------------------------------------------------------------------
# Sécurité - Clé API
# ---------------------------------------------------------------------------

api_key_header = APIKeyHeader(name=API_KEY_NAME, auto_error=False)


async def verify_api_key(api_key: str = Security(api_key_header)):
    """Vérifie la clé API. Lève HTTP 403 si invalide ou absente."""
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
    """Gestionnaire de contexte pour la connexion SQLite."""
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
    """Exécute une requête et retourne une liste de dicts."""
    with get_db() as conn:
        df = pd.read_sql(sql, conn, params=params)
        return df.to_dict(orient="records")


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class GroupeEtablissement(str, Enum):
    hopitaux = "Hopitaux cliniques"
    medico_social = "Médico-social handicap"
    personnes_agees = "Personnes âgées"
    prevention = "Prévention / Santé publique"
    social = "Social / Hébergement"
    enfance = "Enfance / Protection"
    soins_ville = "Soins de ville"
    centres_accueil = "Centres d'accueil"
    coordination = "Coordination / Administration"
    ecoles = "Ecoles"
    autres = "Autres"

class DepartementOccitanie(str, Enum):
    ariege = "09"
    aude = "11"
    aveyron = "12"
    gard = "30"
    haute_garonne = "31"
    gers = "32"
    herault = "34"
    lot = "46"
    lozere = "48"
    hautes_pyrenees = "65"
    pyrenees_orientales = "66"
    tarn = "81"
    tarn_et_garonne = "82"


# ---------------------------------------------------------------------------
# Modèles Pydantic (réponses documentées dans Swagger)
# ---------------------------------------------------------------------------

class Etablissement(BaseModel):
    nofinesset: str
    nofinessej: Optional[str]
    rs: str
    rslongue: Optional[str]
    departement: str
    commune: str
    code_insee:          str
    libcategetab:        str
    libcategagretab:     str
    numvoie:        Optional[str]
    typvoie:           Optional[str]
    voie:              Optional[str]
    compvoie:            Optional[str]
    complrs:             Optional[str]
    compldistrib:        Optional[str]
    lieuditbp:           Optional[str]
    longitude:           float
    latitude:            float
    nom_commune:         str
    population:          int
    groupe:              str


class StatsDepartement(BaseModel):
    departement: str
    libdepartement: Optional[str]
    nb_etablissements: int
    capacite_totale: int
    nb_avec_soins: int
    nb_avec_equipements: int


class StatsRegion(BaseModel):
    total_etablissements: int
    total_capacite: int
    nb_avec_soins: int
    nb_avec_equipements: int
    nb_departements: int


class Equipement(BaseModel):
    nofinesset: str
    type_equipement: Optional[str]
    mode_accueil: Optional[str]
    public_cible: Optional[str]
    capacite_installee: Optional[float]


class Activite(BaseModel):
    nofinesset: str
    activite: Optional[str]
    modalite: Optional[str]
    forme: Optional[str]
    datemeo: Optional[str]
    datefin: Optional[str]


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.get("/health", tags=["Système"])
def health_check():
    """Vérifie que l'API et la base de données sont disponibles."""
    db_ok = DB_PATH.exists()
    return {
        "status": "ok" if db_ok else "degraded",
        "database": str(DB_PATH),
        "database_exists": db_ok,
    }


@app.get(
    "/etablissements",
    response_model=list[Etablissement],
    tags=["Établissements"],
    summary="Liste des établissements Occitanie",
    dependencies=[Depends(verify_api_key)],
)
def get_etablissements(
    departement: Optional[str] = Query(None, description="Filtrer par code département (ex: 31)"),
    commune: Optional[str] = Query(None, description="Filtrer par commune (ex: Elne)"),
    groupe: GroupeEtablissement = Query(default=None, description ="Filtrer sur le type d'établissement"),
    limit: int = Query(100, ge=1, le=1000, description="Nombre max de résultats"),
    offset: int = Query(0, ge=0, description="Décalage pour pagination"),
):
    """
    Retourne la liste des établissements sanitaires et médico-sociaux
    de la région Occitanie, avec options de filtrage.
    """
    conditions = ["1=1"]
    params = []

    if departement:
        conditions.append("departement = ?")
        params.append(departement.zfill(2))


    if groupe:
        conditions.append("groupe = ?")
        params.append(groupe.value)

    if commune:
        conditions.append("nom_commune = ?")
        params.append(commune)


    where = " AND ".join(conditions)
    sql = f"""
        SELECT * FROM etablissements
        WHERE {where}
        ORDER BY departement, nofinesset
        LIMIT ? OFFSET ?
    """
    params.extend([limit, offset])
    return query_db(sql, tuple(params))


@app.get(
    "/etablissements/{nofinesset}",
    tags=["Établissements"],
    summary="Détail d'un établissement",
    dependencies=[Depends(verify_api_key)],
)
def get_etablissement(nofinesset: str):
    """Retourne le détail d'un établissement par son numéro FINESS, ses coordonnées, ses activités de soins et les équipements médico-sociaux s'ils existent."""
    nofinesset = nofinesset.zfill(9)
    
    etablissement = query_db("SELECT nofinesset,rs,rslongue,departement,numvoie, typvoie, voie, libdepartement, libcategetab, groupe, longitude, latitude, nom_commune, population FROM etablissements WHERE nofinesset = ?", (nofinesset,))
    
    # Si l'établissement n'existe pas du tout → 404
    if not etablissement:
        raise HTTPException(status_code=404, detail=f"Établissement {nofinesset} non trouvé")
    
    equipements = query_db("SELECT libde, libta, libclient,groupe_equipement FROM equipements WHERE nofinesset = ?", (nofinesset,))
    activites = query_db("SELECT libactivite, libmodalite FROM activites WHERE nofinesset = ?", (nofinesset,))
    
    return {
        "etablissement": etablissement[0],
        "equipements": equipements if equipements else [],        # liste vide si absent
        "activites": activites if activites else [],              # liste vide si absent
        "a_equipements": len(equipements) > 0,                   # booléen pratique
        "a_activites": len(activites) > 0,                       # booléen pratique
    }




@app.get(
    "/stats/region",
    tags=["Statistiques"],
    summary="Statistiques globales Occitanie",
    dependencies=[Depends(verify_api_key)],
)
def get_stats_region():
    """Statistiques agrégées pour toute la région Occitanie."""
    etablissement = query_db("""
        SELECT
            COUNT(*) as total_etablissements,
            COUNT(DISTINCT departement) as nb_departements
        FROM etablissements
    """)
    equipements = query_db("SELECT COUNT(*) as total_equipements FROM equipements")
    activites = query_db("SELECT COUNT(*) as total_soins FROM activites")
    population = query_db("SELECT SUM(population) as population_occitanie FROM departements")
    return {
        **etablissement[0],
        **equipements[0],
        **activites[0],
        **population[0]
    }
    

@app.get(
    "/stats/departements",
    tags=["Statistiques"],
    summary="Statistiques par département",
    dependencies=[Depends(verify_api_key)],
)
def get_stats_departements(
    departement: DepartementOccitanie = Query(default=None, description="Filtrer par département")
):
    """Statistiques agrégées par département d'Occitanie."""
    condition = "WHERE e.departement = ?" if departement else ""
    params = (departement.value,) if departement else ()

    etablissements = query_db(f"""
        SELECT
            e.departement,
            e.libdepartement,
            COUNT(*) as nb_etablissements,
            COUNT(DISTINCT eq.nofinesset) as nb_avec_equipements,
            COUNT(DISTINCT ac.nofinesset) as nb_avec_soins,
            p.population
        FROM etablissements e
        LEFT JOIN equipements eq ON e.nofinesset = eq.nofinesset
        LEFT JOIN activites ac ON e.nofinesset = ac.nofinesset
        LEFT JOIN departements p ON e.departement = p.code_dept
        {condition}
        GROUP BY e.departement
        ORDER BY nb_etablissements DESC
    """, params)

    return etablissements

@app.get(
    "/stats/communes",
    tags=["Statistiques"],
    summary="Statistiques par commune",
    dependencies=[Depends(verify_api_key)],
)
def get_stats_communes(
    commune: Optional[str] = Query(default=None, description="Filtrer par commune")
):
    """Statistiques agrégées par commune d'Occitanie."""
    condition = "WHERE e.nom_commune = ?" if commune else ""
    params = (commune,) if commune else ()

    etablissements = query_db(f"""
        SELECT
            e.nom_commune,
            e.libdepartement,
            COUNT(*) as nb_etablissements,
            COUNT(DISTINCT eq.nofinesset) as nb_avec_equipements,
            COUNT(DISTINCT ac.nofinesset) as nb_avec_soins,
            MAX(e.population) as population
        FROM etablissements e
        LEFT JOIN equipements eq ON e.nofinesset = eq.nofinesset
        LEFT JOIN activites ac ON e.nofinesset = ac.nofinesset
        {condition}
        GROUP BY e.nom_commune
        ORDER BY nb_etablissements DESC
    """, params)

    return etablissements

@app.get(
    "/activites/top",
    tags=["Activités de soins"],
    summary="Top activités les plus fréquentes",
    dependencies=[Depends(verify_api_key)],
)
def get_top_activites(limit: int = Query(10, ge=1, le=50)):
    """Retourne les activités de soins les plus représentées en Occitanie."""
    return query_db(
        """
        SELECT groupe_activites,
        COUNT(*) as nb_activites_de_soins
        FROM activites
        WHERE groupe_activites IS NOT NULL
        GROUP BY groupe_activites
        ORDER BY nb_activites_de_soins DESC
        LIMIT ?
        """,
        (limit,)
    )





# ---------------------------------------------------------------------------
# Point d'entrée
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("api:app", host="0.0.0.0", port=8000, reload=True)
