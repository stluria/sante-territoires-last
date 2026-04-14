"""
C2/C3 - Nettoyage et préparation des données FINESS
=====================================================
- Normalisation des colonnes (noms, types)
- Gestion des doublons
- Nettoyage des valeurs nulles et formats
- Filtrage Occitanie (région 76, départements 09,11,12,30,31,32,34,46,48,65,66,81,82)
- Règles d'agrégation pour combiner les 3 sources
- Géoréférencement (Lambert93 → WGS84)
"""
import logging
import sys
import unicodedata
from pathlib import Path

import pandas as pd
from pyproj import Transformer

# ---------------------------------------------------------------------------
# Chemins
# ---------------------------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent
RAW_DIR = BASE_DIR / "../data/raw"
PROCESSED_DIR = BASE_DIR / "../data/processed"

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
# Fonctions utilitaires
# ---------------------------------------------------------------------------

def clean_nofiness(series: pd.Series) -> pd.Series:
    return series.astype(str).str.zfill(9)


def clean_department(code) -> str:
    if pd.isna(code):
        return None
    return str(code).strip().zfill(2)[:2]


def clean_text(s: str) -> str:
    if not isinstance(s, str):
        return ""
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("utf-8")
    return " ".join(s.split()).lower()


def convert_lambert_to_wgs84(df, x_col="coordxet", y_col="coordyet",
                              lon_col="longitude", lat_col="latitude"):
    if x_col not in df.columns or y_col not in df.columns:
        raise ValueError(f"Colonnes manquantes : {x_col} ou {y_col}")
    df = df.copy()
    x = pd.to_numeric(df[x_col], errors="coerce")
    y = pd.to_numeric(df[y_col], errors="coerce")
    valid = x.notna() & y.notna() & (x != 0) & (y != 0)
    transformer = Transformer.from_crs("EPSG:2154", "EPSG:4326", always_xy=True)
    df[lon_col] = None
    df[lat_col] = None
    if valid.any():
        lons, lats = transformer.transform(x[valid].values, y[valid].values)
        df.loc[valid, lon_col] = lons
        df.loc[valid, lat_col] = lats
    return df


def create_code_insee(df, dep_col="departement", com_col="commune", new_col="code_insee"):
    if dep_col not in df.columns or com_col not in df.columns:
        raise ValueError(f"Colonnes manquantes : {dep_col} ou {com_col}")
    df = df.copy()
    df[new_col] = (
        df[dep_col].astype(str).str.zfill(2)
        + df[com_col].astype(str).str.zfill(3)
    )
    return df


def filter_occitanie(df, dep_col="departement"):
    DEPTS = {"09","11","12","30","31","32","34","46","48","65","66","81","82"}
    if dep_col not in df.columns:
        raise ValueError(f"Colonne '{dep_col}' absente.")
    df = df.copy()
    df[dep_col] = df[dep_col].astype(str).str.zfill(2)
    return df[df[dep_col].isin(DEPTS)]


def regrouper_activite(cat):
    if not isinstance(cat, str):
        return "Autres"
    cat = cat.lower()
    if "greffe" in cat:                                  return "Greffe"
    if "soins de suite" in cat:                          return "Soins de suite"
    if "chirurgie" in cat:                               return "Chirurgie"
    if "médecine d'urgence" in cat:                      return "Médecine d'urgence"
    if "médecine" in cat:                                return "Médecine"
    if "psychiatrie" in cat:                             return "Psychiatrie"
    if "amp dpn" in cat:                                 return "AMP DPN"
    if "cancer" in cat:                                  return "Cancer"
    if "gynécologie" in cat:                             return "Gynécologie"
    if "soins de longue durée" in cat:                   return "Soins de longue durée"
    if "insuffisance rénale" in cat:                     return "Insuffisance rénale"
    if "examen des caractéristiques génétiques" in cat:  return "Examen génétique"
    return "Autres"


def regrouper_equipement(cat):
    cat = clean_text(cat)
    if any(x in cat for x in ["ehpad","personnes agees","maison de retraite","hebergement pour personnes agees"]):
        return "Personnes âgées"
    if any(x in cat for x in ["ime","itep","sessad","enfance","enfant","educatif","pedagogique","cmpp","scolarisation"]):
        return "Enfance"
    if any(x in cat for x in ["handicap","handicape","adapte"]):
        return "Handicap"
    if any(x in cat for x in ["a domicile","aide a domicile","soins a domicile"]):
        return "Aide et soins à domicile"
    if any(x in cat for x in ["psychologique","psy","psychiatrie"]):
        return "Aide psychologique"
    if any(x in cat for x in ["hebergement","maison relais","pension de famille","accueil temporaire"]):
        return "Hébergement"
    if any(x in cat for x in ["social","service social","intervention educative","club prevention"]):
        return "Social"
    if any(x in cat for x in ["reinsertion","adaptation","rehabilitation","protection des majeurs"]):
        return "Réinsertion et adaptation"
    if "aidant" in cat:
        return "Aidants"
    if "cure" in cat:
        return "Cure"
    return "Autres"


def quality_report(df: pd.DataFrame) -> dict:
    return {
        "total_etablissements": len(df),
        "departements": df["departement"].value_counts().to_dict(),
        "valeurs_nulles": df.isnull().sum().to_dict(),
    }
# Regroupement des catégories de types d'établissements
def regrouper_categorie(cat):
    cat = cat.lower()

    if any(x in cat for x in ["hospital", "clinique", "soins", "dialyse", "ssr", "had", "cancer"]):
        return "Hopitaux cliniques"

    if any(x in cat for x in ["handicap", "ime", "itep", "mas", "fam", "esat", "sensoriel", "e.s.a.t.", "i.m.e.", "i.t.e.p.", "m.a.s.", "s.a.v.s.", "foyer de vie", "entreprise adaptée", "esat", "institut d'éducation motrice", "service mandataire judiciaire à la protection des majeurs", "c.a.m.s.p."]):
        return "Médico-social handicap"

    if any(x in cat for x in ["ehpad", "ehpa", "autonomie", "personnes âgées", "longue durée", "personnes agées"]):
        return "Personnes âgées"

    if any(x in cat for x in ["chrs", "casa", "c.a.d.a", "hébergement", "foyer", "maison relais"]):
        return "Social / Hébergement"

    if any(x in cat for x in ["pharmacie", "centre de santé", "maison de santé", "laboratoire", "c.m.p.", "cabinet", "infirmier", "kinésithérapeute", "dentiste", "opticien", "structure dispensatrice à domicile d'oxygène à usage médical", "maison médicale de garde (MMG)"]):
        return "Soins de ville"

    if any(x in cat for x in ['prévention', "vaccination", "dépistage", "csapa", "caarud" ]):
        return "Prévention / Santé publique"

    if any(x in cat for x in ["enfance", "camsp", "aemo", "aed", "pouponnière", "maison d'enfants", "educative", "protection maternelle et infantile", "protection infantile", "pmi", "p.m.i."]):
        return "Enfance / Protection"

    if any(x in cat for x in ["cpts", "mdph", "coordination", "groupement"]):
        return "Coordination / Administration"
    if any(x in cat for x in ["centre d'accueil", "accueil"]):
            return "Centres d'accueil"
    if any(x in cat for x in ["ecoles"]):
        return "Ecoles"
    return "Autres"


# ---------------------------------------------------------------------------
# Pipeline principal
# ---------------------------------------------------------------------------

def prepare() -> pd.DataFrame:
    """Pipeline complet de préparation des données FINESS Occitanie."""
    logger.info("=" * 60)
    logger.info("DÉMARRAGE DU NETTOYAGE ET PRÉPARATION")
    logger.info("=" * 60)

    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    # Chargement
    logger.info("Chargement des fichiers sources...")
    df_soins   = pd.read_csv(RAW_DIR / "finess_activites_soins.csv",      sep=";", encoding="utf-8", dtype=str)
    df_communes= pd.read_csv(RAW_DIR / "communes-france-2025.csv",         sep=",", encoding="utf-8", dtype=str)
    df_etab    = pd.read_csv(RAW_DIR / "finess_etablissements.txt",        sep=";", encoding="utf-8",
                             dtype={0:"string", 1:"string", 12:"string"})
    df_equi    = pd.read_csv(RAW_DIR / "finess_equipements_sociaux.csv",   sep=";", encoding="utf-8",
                             dtype={0:"string", 1:"string"})
    logger.info(f"  soins {len(df_soins):,} | communes {len(df_communes):,} | etab {len(df_etab):,} | equi {len(df_equi):,}")

    # Nettoyage activités soins
    cols = ["nofinesset","rsej","libactivite","libmodalite","libforme","datefin"]
    df_soins = df_soins[[c for c in cols if c in df_soins.columns]].copy()
    df_soins["nofinesset"]     = clean_nofiness(df_soins["nofinesset"])
    df_soins["groupe_activites"] = df_soins["libactivite"].apply(regrouper_activite)
    df_soins = df_soins.drop_duplicates()

    # Nettoyage établissements
    cols = ["nofinesset","nofinessej","rs","rslongue","departement","commune",
            "complrs","compldistrib","numvoie","typvoie","voie","compvoie",
            "lieuditbp","libdepartement","libcategetab","libcategagretab","coordxet","coordyet"]
    df_etab = df_etab[[c for c in cols if c in df_etab.columns]].copy()
    df_etab["nofinesset"]  = clean_nofiness(df_etab["nofinesset"])
    df_etab = df_etab.drop_duplicates()
    df_etab["departement"] = df_etab["departement"].apply(clean_department)
    df_etab["commune"]     = df_etab["commune"].astype("Int64").astype(str).str.zfill(3)
    df_etab["groupe"] = df_etab["libcategetab"].apply(regrouper_categorie)

    # Nettoyage équipements sociaux
    df_equi["groupe_equipement"] = df_equi["libde"].apply(regrouper_equipement)
    df_equi = df_equi.drop_duplicates()
    cols = ["nofinesset","nofinessej","libde","libta","libclient","capinstot","groupe_equipement"]
    df_equi = df_equi[[c for c in cols if c in df_equi.columns]].copy()
    df_equi["nofinesset"] = clean_nofiness(df_equi["nofinesset"])

    # Nettoyage communes
    keep = ["code_insee","nom_standard","population","latitude_centre","longitude_centre"]
    df_communes = df_communes[[c for c in keep if c in df_communes.columns]].copy()
    df_communes = df_communes.rename(columns={"nom_standard":"nom_commune",
                                               "latitude_centre":"latitude_commune",
                                               "longitude_centre":"longitude_commune"})
    df_communes["code_insee"] = df_communes["code_insee"].astype(str).str.zfill(5)

    # Jointures établissements ← équipements / soins
    df_equi_tot = df_equi.merge(df_etab, on="nofinesset", how="left")
    df_soins_tot = df_soins.merge(df_etab, on="nofinesset", how="left")

    # Code INSEE
    df_equi_tot  = create_code_insee(df_equi_tot,  "departement", "commune")
    df_soins_tot = create_code_insee(df_soins_tot, "departement", "commune")
    df_etab      = create_code_insee(df_etab,      "departement", "commune")

    # Lambert93 → WGS84
    df_soins_tot = convert_lambert_to_wgs84(df_soins_tot)
    df_equi_tot  = convert_lambert_to_wgs84(df_equi_tot)
    df_etab      = convert_lambert_to_wgs84(df_etab)

    # Jointure communes
    df_soins_tot = df_soins_tot.merge(df_communes, on="code_insee", how="left")
    df_equi_tot  = df_equi_tot.merge(df_communes,  on="code_insee", how="left")
    df_etab      = df_etab.merge(df_communes,       on="code_insee", how="left")

    # Filtrage Occitanie
    df_etab_occ  = filter_occitanie(df_etab)
    df_soins_occ = filter_occitanie(df_soins_tot)
    df_equi_occ  = filter_occitanie(df_equi_tot)

    # Rapport qualité
    report = quality_report(df_etab_occ)
    logger.info(f"\n  Établissements : {report['total_etablissements']:,}")
    logger.info(f"  Activités      : {len(df_soins_occ):,}")
    logger.info(f"  Équipements    : {len(df_equi_occ):,}")

    # Export CSV
    df_etab_occ.to_csv( PROCESSED_DIR / "etablissements_occitanie.csv", index=False, encoding="utf-8")
    df_equi_occ.to_csv( PROCESSED_DIR / "equipements_occitanie.csv",    index=False, encoding="utf-8")
    df_soins_occ.to_csv(PROCESSED_DIR / "activites_occitanie.csv",      index=False, encoding="utf-8")
    logger.info(f"\n✓ CSV exportés dans : {PROCESSED_DIR.resolve()}")
    logger.info("=" * 60)
    logger.info("PRÉPARATION TERMINÉE")
    logger.info("=" * 60)

    return df_etab_occ


if __name__ == "__main__":
    prepare()
