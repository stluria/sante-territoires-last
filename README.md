# FINESS Occitanie — Pipeline ETL & API

Projet RNCP37827BC01 — Collecte, stockage et mise à disposition des données  
**Source** : Fichier National des Établissements Sanitaires et Sociaux (FINESS)  
**Périmètre** : Région Occitanie (13 départements)

---

## Structure du projet

```
finess_occitanie/
├── data/
│   ├── raw/                              # Fichiers FINESS bruts téléchargés
│   │   ├── finess_equipements_sociaux.csv
│   │   ├── finess_activites_soins.csv
│   │   └── v_commune_2025.csv
│   └── processed/                        # Données nettoyées (CSV Occitanie)
│       ├── etablissements_occitanie.csv
│       ├── equipements_occitanie.csv
│       ├── activites_occitanie.csv
│       └── finess_occitanie.db           # Base SQLite finale
├── src/
│   ├── collect.py                        # C1 — Extraction automatisée
│   ├── prepare.py                        # C2/C3 — Nettoyage & agrégation
│   └── store.py                          # C4 — Stockage SQLite
├── api/
│   └── api.py                            # C5 — API FastAPI sécurisée
├── tests/
│   └── test_pipeline.py                  # Tests unitaires
├── run_pipeline.py                       # Orchestrateur ETL
├── logs/                                 # Logs de collecte horodatés
└── requirements.txt
```

> **Note** : `finess_etablissements.txt` (entités géographiques) peut être ajouté manuellement
> dans `data/raw/` si le téléchargement automatique n'est pas disponible.

---

## Installation

```bash
pip install -r requirements.txt
```

---

## Lancer le pipeline ETL

```bash
# Utilise les fichiers déjà présents dans data/raw/
python run_pipeline.py

# Force le re-téléchargement depuis data.gouv.fr / INSEE
python run_pipeline.py --download
```

Le pipeline exécute dans l'ordre :
1. **collect.py** — téléchargement + vérification d'intégrité (MD5, nombre de lignes)
2. **prepare.py** — nettoyage, géoréférencement Lambert93→WGS84, regroupement catégories, filtrage Occitanie
3. **store.py** — schéma SQLite + injection des 3 tables

---

## Logique de préparation (prepare.py)

### Sources chargées

| Fichier | Variable | Rôle |
|---------|----------|------|
| `finess_etablissements.txt` | `df_etab` | Base centrale (adresses, coordonnées, catégories) |
| `finess_activites_soins.csv` | `df_soins` | Activités autorisées par établissement |
| `finess_equipements_sociaux.csv` | `df_equi` | Équipements et capacités médicosociales |
| `v_commune_2025.csv` | `df_communes` | Enrichissement géographique (INSEE) |

### Traitements appliqués

- **Normalisation FINESS** : zéros à gauche → 9 caractères
- **Normalisation codes département** : `9` → `09`
- **Code INSEE** : `departement (2)` + `commune (3)` → 5 caractères
- **Regroupement équipements** : 11 groupes (Personnes âgées, Enfance, Handicap, Aide à domicile…)
- **Regroupement activités** : 13 groupes (Chirurgie, Médecine, Psychiatrie, Urgences…)
- **Géoréférencement** : Lambert93 (EPSG:2154) → WGS84 (latitude/longitude)
- **Jointures** : équipements ↔ établissements, soins ↔ établissements, puis ↔ communes
- **Filtrage Occitanie** : 13 codes département

### CSV exportés

| Fichier | Granularité |
|---------|-------------|
| `etablissements_occitanie.csv` | 1 ligne / établissement |
| `equipements_occitanie.csv` | 1 ligne / équipement (N par établissement) |
| `activites_occitanie.csv` | 1 ligne / activité (N par établissement) |

---

## Lancer l'API

```bash
cd api
uvicorn api:app --reload
```

- Swagger UI : http://localhost:8000/docs
- ReDoc      : http://localhost:8000/redoc

### Authentification

```
X-API-Key: finess-occitanie-2025-demo
```

En production :
```bash
export FINESS_API_KEY=votre-cle-secrete
```

### Endpoints principaux

| Route | Description |
|-------|-------------|
| `GET /etablissements` | Liste avec filtres (département, catégorie, géolocalisation) |
| `GET /etablissements/{id}` | Détail d'un établissement |
| `GET /etablissements/{id}/equipements` | Équipements d'un établissement |
| `GET /etablissements/{id}/activites` | Activités d'un établissement |
| `GET /equipements` | Filtres par département et groupe |
| `GET /activites` | Filtres par département et groupe |
| `GET /stats/region` | Statistiques globales Occitanie |
| `GET /stats/departements` | Statistiques par département |
| `GET /stats/groupes-equipements` | Répartition par groupe d'équipements |
| `GET /stats/groupes-activites` | Répartition par groupe d'activités |
| `GET /activites/top` | Top N activités les plus fréquentes |
| `GET /departements` | Référentiel des 13 départements |

### Exemples curl

```bash
# Stats région
curl -H "X-API-Key: finess-occitanie-2025-demo" http://localhost:8000/stats/region

# Établissements géolocalisés du Gard
curl -H "X-API-Key: finess-occitanie-2025-demo" \
  "http://localhost:8000/etablissements?departement=30&avec_coordonnees=true"

# Équipements pour personnes âgées
curl -H "X-API-Key: finess-occitanie-2025-demo" \
  "http://localhost:8000/equipements?groupe=Personnes%20âgées"

# Activités de chirurgie en Haute-Garonne
curl -H "X-API-Key: finess-occitanie-2025-demo" \
  "http://localhost:8000/activites?departement=31&groupe=Chirurgie"
```

---

## Lancer les tests

```bash
pytest tests/ -v
```

---

## Sources de données

| Fichier | URL | Description |
|--------|-----|-------------|
| `finess_equipements_sociaux.csv` | data.gouv.fr | Équipements sociaux et médico-sociaux |
| `finess_activites_soins.csv` | data.gouv.fr | Activités de soins autorisées |
| `v_commune_2025.csv` | insee.fr | Référentiel communes 2025 |

---

## Choix technologiques

| Composant | Choix | Justification |
|-----------|-------|---------------|
| ETL | Python + Pandas | Reproductible, lisible, écosystème riche |
| Géoréférencement | pyproj (EPSG:2154 → 4326) | Conversion Lambert93 → WGS84 précise |
| Stockage | SQLite | Zéro infrastructure, fichier unique, performant |
| API | FastAPI | Swagger auto-généré, typage Pydantic, async natif |
| Tests | pytest | Standard Python, simple et extensible |
