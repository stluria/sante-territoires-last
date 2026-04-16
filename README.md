# FINESS Occitanie — Pipeline ETL & API

Projet RNCP37827BC01 — Collecte, stockage et mise à disposition des données  
**Source** : Fichier National des Établissements Sanitaires et Sociaux (FINESS)  
**Périmètre** : Région Occitanie (13 départements)

---

## Structure du projet

```
finess_occitanie/
├── data/
│   ├── raw/                          # Fichiers bruts téléchargés
│   └── processed/                    # Données nettoyées (CSV)
│       └── finess_occitanie.db       # Base SQLite finale
├── src/
│   ├── collect.py                    # C1 — Extraction automatisée
│   ├── prepare.py                    # C2/C3 — Nettoyage & agrégation
│   └── store.py                      # C4 — Stockage SQLite
├── api/
│   └── api.py                        # C5 — API FastAPI sécurisée
├── tests/
│   └── test_pipeline.py              # Tests unitaires
├── run_pipeline.py                   # Orchestrateur ETL
└── requirements.txt
```

---

## Sources de données

| Fichier | Volume national | Source | Description |
|--------|----------------|--------|-------------|
| `finess_equipements_sociaux.csv` | 109 365 lignes | data.gouv.fr | Équipements sociaux et médico-sociaux, capacités |
| `finess_activites_soin.csv` | 10 291 lignes | data.gouv.fr | Activités de soins autorisées par établissement |
| `finess_etablissement.txt` | 102 553 lignes | local | Établissements géographiques (adresse, statut, type) |
| `v_commune_2025.csv` | 37 548 lignes | data.gouv.fr / INSEE | Référentiel communes 2025 |

> **Note** : Le fichier établissements présent en ligne présente une erreur d'architecture ; le fichier local est utilisé à la place.

---

## Installation

```bash
pip install -r requirements.txt
```

---

## Lancer le pipeline ETL

```bash
# Utilise les fichiers locaux (data/raw/)
python run_pipeline.py

# Force le téléchargement depuis data.gouv.fr
python run_pipeline.py --download
```

Le pipeline exécute dans l'ordre :
1. **collect.py** — téléchargement + vérification d'intégrité (MD5, comptage de lignes)
2. **prepare.py** — nettoyage, filtrage Occitanie, géoréférencement Lambert93 → WGS84, agrégation
3. **store.py** — création du schéma SQLite + injection

---

## Schéma de la base de données

La base SQLite (`finess_occitanie.db`) contient 5 tables :

| Table | Description | Clé |
|-------|-------------|-----|
| `etablissements` | Table centrale, 1 ligne par établissement Occitanie | `nofinesset` |
| `equipements` | Détail des équipements sociaux (N:1 → etablissements) | `nofinesset` |
| `activites` | Détail des activités de soins (N:1 → etablissements) | `nofinesset` |
| `departements` | Référentiel des 13 départements d'Occitanie | `code_dept` |


Index créés pour la performance :
```sql
CREATE INDEX idx_etab_dept      ON etablissements(departement);
CREATE INDEX idx_etab_soins     ON etablissements(a_activite_soins);
CREATE INDEX idx_equip_nofiness ON equipements(nofinesset);
CREATE INDEX idx_activ_nofiness ON activites(nofinesset);
CREATE INDEX idx_activ_libelle  ON activites(activite);
```

---

## Lancer l'API

```bash
cd api
uvicorn api:app --reload
```

- Swagger UI : http://localhost:8000/docs  
- ReDoc : http://localhost:8000/redoc

### Authentification

Toutes les routes nécessitent le header :
```
X-API-Key: finess-occitanie-2025-demo
```

En production, définir la variable d'environnement :
```bash
export FINESS_API_KEY=votre-cle-secrete
```

### Endpoints disponibles

| Endpoint | Méthode | Description |
|----------|---------|-------------|
| `/health` | GET | Statut de l'API et de la base de données |
| `/etablissements` | GET | Liste des établissements (filtres : département, groupe) |
| `/etablissements/{nofinesset}` | GET | Détail d'un établissement + équipements + activités |
| `/stats/region` | GET | Statistiques agrégées région Occitanie |
| `/stats/departements` | GET | Statistiques par département |
| `/stats/communes` | GET | Statistiques par commune |
| `/activites/top` | GET | Top activités de soins les plus fréquentes |

### Filtres disponibles sur `/etablissements`

| Paramètre | Type | Description |
|-----------|------|-------------|
| `departement` | string | Code département (ex : `31`) |
| `groupe` | enum | Type d'établissement (voir ci-dessous) |
| `limit` | int | Nombre max de résultats (défaut : 100, max : 1000) |
| `offset` | int | Décalage pour pagination |

**Valeurs du filtre `groupe` :**
`Hopitaux cliniques`, `Médico-social handicap`, `Personnes âgées`, `Prévention / Santé publique`, `Social / Hébergement`, `Enfance / Protection`, `Soins de ville`, `Centres d'accueil`, `Coordination / Administration`, `Ecoles`, `Autres`

### Exemples de requêtes

```bash
# Tous les établissements
curl -H "X-API-Key: finess-occitanie-2025-demo" http://localhost:8000/etablissements

# Filtrer par département (Haute-Garonne)
curl -H "X-API-Key: finess-occitanie-2025-demo" "http://localhost:8000/etablissements?departement=31"

# Filtrer par groupe
curl -H "X-API-Key: finess-occitanie-2025-demo" "http://localhost:8000/etablissements?groupe=Hopitaux%20cliniques"

# Détail d'un établissement (avec équipements et activités)
curl -H "X-API-Key: finess-occitanie-2025-demo" http://localhost:8000/etablissements/310000031

# Stats par département
curl -H "X-API-Key: finess-occitanie-2025-demo" http://localhost:8000/stats/departements

# Stats région
curl -H "X-API-Key: finess-occitanie-2025-demo" http://localhost:8000/stats/region

# Top activités de soins
curl -H "X-API-Key: finess-occitanie-2025-demo" http://localhost:8000/activites/top
```

---

## Lancer les tests

```bash
pytest tests/ -v
```

Couverture des tests :

| Catégorie | Test | Ce qui est vérifié |
|-----------|------|--------------------|
| Nettoyage | `test_clean_nofiness` | FINESS = exactement 9 caractères |
| Nettoyage | `test_clean_department` | Code dépt toujours sur 2 chiffres |
| Nettoyage | `test_occitanie_filter` | Aucun dépt hors Occitanie |
| Nettoyage | `test_no_null_nofinesset` | Pas de nofinesset NULL |
| Nettoyage | `test_no_duplicates_nofinesset` | Pas de doublons sur nofinesset |
| Stockage | `test_database_exists` | Fichier .db présent |
| Stockage | `test_tables_exist` | 4 tables attendues présentes |
| Stockage | `test_all_depts_occitanie` | Tous les dépts = Occitanie |
| API | `test_health_endpoint` | HTTP 200 sur /health |
| API | `test_auth_required` | HTTP 403 sans clé API |
| API | `test_auth_with_valid_key` | HTTP 200 avec clé valide |
| API | `test_stats_region` | Champs attendus présents |
| API | `test_filter_by_department` | Filtre département fonctionnel |
| API | `test_etablissement_not_found` | HTTP 404 pour nofinesset inconnu |

---

## Périmètre géographique

13 départements de la région Occitanie (code région 76) :

| Code | Département |
|------|------------|
| 09 | Ariège |
| 11 | Aude |
| 12 | Aveyron |
| 30 | Gard |
| 31 | Haute-Garonne |
| 32 | Gers |
| 34 | Hérault |
| 46 | Lot |
| 48 | Lozère |
| 65 | Hautes-Pyrénées |
| 66 | Pyrénées-Orientales |
| 81 | Tarn |
| 82 | Tarn-et-Garonne |

---

## Choix technologiques

| Composant | Choix | Justification |
|-----------|-------|---------------|
| ETL | Python + Pandas | Écosystème riche, reproductible, lisible |
| Géoconversion | pyproj | Conversion Lambert93 (EPSG:2154) → WGS84 (EPSG:4326) précise |
| Stockage | SQLite | Zéro config, fichier unique, performant jusqu'à ~100k lignes |
| API | FastAPI | Swagger auto-généré, validation Pydantic, async natif |
| Tests | pytest | Standard Python, simple, extensible |
| Exploration | BigQuery | Phase exploratoire (remplacé par SQLite pour la reproductibilité) |

SQLite a été préféré à PostgreSQL pour la **reproductibilité** : le jury peut cloner le repo et lancer le pipeline sans infrastructure. BigQuery (utilisé en phase exploratoire) a été remplacé pour cette raison.

---

## Nettoyage des données (résumé)

| Anomalie | Correction appliquée |
|----------|---------------------|
| Zéros manquants dans codes dépt | `zfill(2)` automatique |
| Espaces parasites | `strip()` sur toutes les colonnes texte |
| Chaînes vides | Remplacement par `NULL` |
| Coordonnées nulles (`coordxet = 0`) | Retour `(None, None)` |
| Capacité non numérique | `pd.to_numeric(errors='coerce')` |
| Numéros FINESS non formatés | Zéros à gauche sur 9 caractères |

Les 172 catégories d'établissements sont regroupées en **11 grands types** via un dictionnaire de mapping (ex. : CHR/CHU/CH → `Hopitaux cliniques`, EHPAD/Maison de Retraite → `Personnes âgées`).
