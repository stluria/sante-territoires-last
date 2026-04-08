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
import os
import plotly.express as px
import pandas as pd
import math
from pyproj import Transformer

from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent  # dossier du .py
df_soins = pd.read_csv(BASE_DIR / "../data/raw/finess_activites_soins.csv", sep=";", encoding="utf-8")
df_communes = pd.read_csv(BASE_DIR / "../data/geo/communes-france-2025.csv", sep=",", encoding="utf-8",dtype={
        1: "string",  # colonne 0
        9: "string",   # colonne 1
        11: "string",   # colonne 1
        13: "string",   # colonne 1
        17: "string"   # colonne 1
    })
df_etab = pd.read_csv(BASE_DIR / "../data/raw/finess_etablissements.txt", sep=";", encoding="utf-8", dtype={
        0: "string",  # colonne 0
        1: "string",   # colonne 1
        12: "string"   # colonne 1
    })
df_equi = pd.read_csv(BASE_DIR / "../data/raw/finess_equipements_sociaux.csv", sep=";", encoding="utf-8", dtype={
        0: "string",  # colonne 0
        1: "string"   # colonne 1
    })



# ---------------------------------------------------------------------------
# Fonctions de nettoyage
# ---------------------------------------------------------------------------

def clean_nofiness(series: pd.Series) -> pd.Series:
    """
    Normalise les numéros FINESS : chaîne de 9 caractères, zéros à gauche.
    Exemples : 10001246 → '010001246', 310001234 → '310001234'
    """
    return series.astype(str).str.zfill(9)


def clean_department(code: str) -> str:
    """Normalise un code département : '9' → '09', '31' → '31'."""
    if pd.isna(code):
        return None
    code = str(code).strip().zfill(2)
    return code[:2]  # Garde uniquement les 2 premiers chiffres


def lambert93_to_wgs84(x: float, y: float):
    """
    Convertit des coordonnées Lambert93 (EPSG:2154) en WGS84 (lat/lon).
    Implémentation pure Python basée sur les paramètres IAG GRS80.
    """
    try:
        if pd.isna(x) or pd.isna(y) or float(x) == 0 or float(y) == 0:
            return None, None
        x, y = float(x), float(y)

        # Paramètres ellipsoïde GRS80
        a = 6378137.0
        e = 0.0818191910428158

        # Paramètres projection Lambert93
        lc = math.radians(3.0)          # Longitude centrale
        phi0 = math.radians(46.5)       # Latitude d'origine
        phi1 = math.radians(44.0)       # Parallèle 1
        phi2 = math.radians(49.0)       # Parallèle 2
        x0 = 700000.0
        y0 = 6600000.0

        def _geo_lat(phi):
            sp = e * math.sin(phi)
            return math.tan(math.pi / 4 + phi / 2) * ((1 - sp) / (1 + sp)) ** (e / 2)

        m1 = math.cos(phi1) / math.sqrt(1 - (e * math.sin(phi1)) ** 2)
        m2 = math.cos(phi2) / math.sqrt(1 - (e * math.sin(phi2)) ** 2)
        t1 = _geo_lat(phi1)
        t2 = _geo_lat(phi2)
        t0 = _geo_lat(phi0)

        n = math.log(m1 / m2) / math.log(t1 / t2)
        F = m1 / (n * t1 ** n)
        r0 = a * F * t0 ** n

        dx, dy = x - x0, y - y0 + r0
        r = math.sqrt(dx ** 2 + dy ** 2) * math.copysign(1, n)
        theta = math.atan(dx / (r0 - (y - y0)))
        lon = math.degrees(theta / n + lc)

        t = (r / (a * F)) ** (1 / n)
        phi = math.pi / 2 - 2 * math.atan(t)
        for _ in range(10):
            sp = e * math.sin(phi)
            phi = math.pi / 2 - 2 * math.atan(t * ((1 - sp) / (1 + sp)) ** (e / 2))

        lat = math.degrees(phi)

        if 41 <= lat <= 52 and -5 <= lon <= 10:
            return round(lat, 6), round(lon, 6)
        return None, None
    except Exception:
        return None, None

   
#nettoyage activité soins
# Sélection des colonnes utiles
cols = ["nofinesset", "rsej", "libactivite", "libmodalite", "libforme", "datefin"]
df_soins = df_soins[[c for c in cols if c in df_soins.columns]].copy()
#nettoyage nofinesset
df_soins["nofinesset"] = clean_nofiness(df_soins["nofinesset"])

# Regroupement des catégories d'activités pour les soins
def regrouper_categorie(cat):
    cat = cat.lower()

    if any(x in cat for x in ["greffe"]):
        return "Greffe"

    if any(x in cat for x in ["soins de suite"]):
        return "Soins de suite"

    if any(x in cat for x in ["chirurgie"]):
        return "Chirurgie"
    if any(x in cat for x in ["médecine d\'urgence"]):
        return "Médecine d'urgence"
    if any(x in cat for x in ["médecine"]):
        return "Médecine"
    if any(x in cat for x in ["médecine"]):
        return "Médecine"
    if any(x in cat for x in ["psychiatrie"]):
        return "Psychiatrie"
    if any(x in cat for x in ["amp dpn"]):
        return "AMP DPN"
    if any(x in cat for x in ["cancer"]):
        return "Cancer"
    if any(x in cat for x in ["gynécologie"]):
        return "Gynécologie"
    if any(x in cat for x in ["soins de longue durée"]):
        return "Soins de longue durée"
    if any(x in cat for x in ["insuffisance rénale"]):
        return "Insuffisance rénale"
    if any(x in cat for x in ["examen des caractéristiques génétiques"]):
        return "Examen des caractéristiques génétiques"
    return "Autres"

# Application
df_soins["groupe_activites"] = df_soins["libactivite"].apply(regrouper_categorie)

#enlever les doublons 
df_soins = df_soins.drop_duplicates()


#nettoyage établissement
# Sélection des colonnes utiles
cols = ["nofinesset", "nofinessej","rs", "rslongue", "departement","commune","complrs","compldistrib","numvoie","typvoie","voie","compvoie","lieuditbp", "libdepartement", "libcategetab", "libcategagretab","coordxet", "coordyet"]
df_etab = df_etab[[c for c in cols if c in df_etab.columns]].copy()
#nettoyage nofinesset
df_etab["nofinesset"] = clean_nofiness(df_etab["nofinesset"])
#enlever les doublons 
df_etab = df_etab.drop_duplicates()
#normalise les départements
def clean_department(code: str) -> str:
    """Normalise un code département : '9' → '09', '31' → '31'."""
    if pd.isna(code):
        return None
    code = str(code).strip().zfill(2)
    return code[:2]  # Garde uniquement les 2 premiers chiffres

df_etab["departement"] = df_etab["departement"].apply(clean_department)
#vérification des codes communes : 3 chiffres 
df_etab["commune"] = (
    df_etab["commune"]
        .astype("Int64")  # gère les NA proprement
        .astype(str)
        .str.zfill(3)
)

#Nettoyage Equipements sociaux
#regrouper les catégories
import unicodedata

def clean_text(s):
    if not isinstance(s, str):
        return ""
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("utf-8")
    s = s.replace("’", "'")
    s = " ".join(s.split())
    return s.lower()

def regrouper_categorie(cat):
    cat = clean_text(cat)

    if any(x in cat for x in [
        "ehpad", "personnes agees", "hebergement pour personnes agees", "maison de retraite", "hbergement pour personnes agees dependantes"
    ]):
        return "Personnes âgées"

    if any(x in cat for x in [
        "ime", "itep", "sessad", "enfance", "enfant", "educatif", "pedagogique", "cmpp", "scolarisation", "educative", "c.m.p.p."
    ]):
        return "Enfance"

    if any(x in cat for x in [
        "handicap", "handicape", "handicapes", "handicapees", "adapte"
    ]):
        return "Handicap"

    if any(x in cat for x in [
        "soins a domicile", "aide a domicile", "service a domicile", "a domicile"
    ]):
        return "Aide et soins à domicile"

    if any(x in cat for x in [
        "psychologique", "psy", "psychiatrie", "cmpp"
    ]):
        return "Aide psychologique"

    if any(x in cat for x in [
        "hebergement", "maison relais", "pension de famille", "maisons relais", "accueil temporaire"
    ]):
        return "Hébergement"

    if any(x in cat for x in [
        "social", "clico", "service social", "intervention educative", "club prevention"
    ]):
        return "Social"

    if any(x in cat for x in [
        "reinsertion", "adaptation", "rehabilitation", "protection des majeurs"
    ]):
        return "Réinsertion et adaptation"
    
    if any(x in cat for x in [
        "aidant", "aidants"
    ]):
        return "Aidants"
    if any(x in cat for x in [
        "cure"
    ]):
        return "Cure"

    return "Autres"


# Application
df_equi["groupe_equipement"] = df_equi["libde"].apply(regrouper_categorie)

#supprimer les doublons
df_equi = df_equi.drop_duplicates()

# Sélection des colonnes utiles
cols = ["nofinesset", "nofinessej","libde", "libta", "libclient", "capinstot", "groupe_equipement"]
df_equi = df_equi[[c for c in cols if c in df_equi.columns]].copy()
#nettoyage nofinesset
df_equi["nofinesset"] = clean_nofiness(df_equi["nofinesset"])

#Nettoyage communes : verification des codes communes : 5 caractères
df_communes["code_insee"] = (
    df_communes["code_insee"]
        .astype(str)
        .str.zfill(5)
)


"""
    Règles d'agrégation pour construire les tables Occitanie.

    Logique :
    Afin de garder une possibilité de calcul sur les différents types de soins et d'équipements, je conserve 3 tables
    2. Transformation des coordonnées en latitudes et longitudes
    3. Jointure avec communes pour enrichissement géographique
    4. Filtrage sur Occitanie via code département
    5. Géoréférencement si coordonnées disponibles
    """
#jointure equipements
# LEFT JOIN - garde tout df1, complète avec df2 si existe
df_equi_tot = df_equi.merge(df_etab, on="nofinesset", how="left")

#jointure soins 
df_soins_tot = df_soins.merge(df_etab, on="nofinesset", how="left")

#fonction qui va permettre de mettre les codes insee des communes dans les tables
def create_code_insee(df, dep_col="departement", com_col="commune", new_col="code_insee"):
    """
    Crée un code INSEE à partir des colonnes département et commune.
    - dep_col : colonne du département (2 chiffres)
    - com_col : colonne de la commune (3 chiffres)
    - new_col : nom de la colonne créée
    """
    
    # Vérification des colonnes
    if dep_col not in df.columns or com_col not in df.columns:
        raise ValueError(f"Colonnes manquantes : {dep_col} ou {com_col}")
    
    df = df.copy()
    
    df[new_col] = (
        df[dep_col].astype(str).str.zfill(2)
        + df[com_col].astype(str).str.zfill(3)
    )
    
    return df

# application aux 3 DF
df_equi_tot = create_code_insee(df_equi_tot, "departement", "commune")
df_soins_tot = create_code_insee(df_soins_tot, "departement", "commune")
df_etab = create_code_insee(df_etab, "departement", "commune")


#fonction pour convertir en latitude et longitude 
def convert_lambert_to_wgs84(
    df,
    x_col="coordxet",
    y_col="coordyet",
    lon_col="longitude",
    lat_col="latitude"
):
    """
    Convertit des coordonnées Lambert 93 (EPSG:2154) en WGS84 (EPSG:4326).
    - x_col : colonne des X Lambert 93
    - y_col : colonne des Y Lambert 93
    - lon_col : nom de la colonne longitude créée
    - lat_col : nom de la colonne latitude créée
    """

    # Vérification des colonnes
    if x_col not in df.columns or y_col not in df.columns:
        raise ValueError(f"Colonnes manquantes : {x_col} ou {y_col}")

    df = df.copy()

    # Création du transformateur
    transformer = Transformer.from_crs("EPSG:2154", "EPSG:4326", always_xy=True)

    # Conversion
    df[lon_col], df[lat_col] = transformer.transform(
        df[x_col].astype(float).values,
        df[y_col].astype(float).values
    )

    return df

# application aux df 
df_soins_tot = convert_lambert_to_wgs84(df_soins_tot, "coordxet", "coordyet")
df_equi_tot = convert_lambert_to_wgs84(df_equi_tot, "coordxet", "coordyet")
df_etab = convert_lambert_to_wgs84(df_etab, "coordxet", "coordyet")

#jointures avec communes
df_soins_tot = df_soins_tot.merge(df_communes, on="code_insee", how="left")
df_equi_tot = df_equi_tot.merge(df_communes, on="code_insee", how="left")
df_etab = df_etab.merge(df_communes, on="code_insee", how="left")

#filtre occitanie
def filter_occitanie(df, dep_col="departement"):
    """
    Filtre un DataFrame sur les départements de la région Occitanie.
    - df : DataFrame à filtrer
    - dep_col : nom de la colonne contenant le code département
    """

    departements_occitanie = [
        "09","11","12","30","31","32","34",
        "46","48","65","66","81","82"
    ]

    if dep_col not in df.columns:
        raise ValueError(f"La colonne '{dep_col}' est absente du DataFrame.")

    df = df.copy()
    df[dep_col] = df[dep_col].astype(str).str.zfill(2)

    return df[df[dep_col].isin(departements_occitanie)]

df_etab_occ = filter_occitanie(df_etab)
df_soins_occ = filter_occitanie(df_soins_tot)
df_equi_occ = filter_occitanie(df_equi_tot)

# ---------------------------------------------------------------------------
# Rapport de qualité
# ---------------------------------------------------------------------------

def quality_report(df_etab_occ: pd.DataFrame) -> dict:
    """Génère un rapport sur la qualité du jeu de données en Occitanie."""
    report = {
        "total_etablissements": len(df_etab_occ),
        "colonnes": list(df_etab_occ.columns),
        "valeurs_nulles": df_etab_occ.isnull().sum().to_dict(),
        "departements": df_etab_occ["departement"].value_counts().to_dict(),
       
    }
    return report

# ---------------------------------------------------------------------------
# Pipeline principal
# ---------------------------------------------------------------------------

def prepare() -> pd.DataFrame:
    """Pipeline complet de préparation des données."""
    logger.info("=" * 60)
    logger.info("DÉMARRAGE DU NETTOYAGE ET PRÉPARATION")
    logger.info("=" * 60)

    

    # Rapport qualité
    report = quality_report(df_final)
    logger.info("\nRAPPORT QUALITÉ")
    logger.info(f"  Établissements Occitanie : {report['total_etablissements']}")
  

    # Export
    out_path = PROCESSED_DIR / "etablissements_occitanie.csv"
    df_etab_occ.to_csv(out_path, index=False, encoding="utf-8")
    logger.info(f"\n✓ Fichier exporté : {out_path}")

    # Export aussi des tables détail pour l'API

    df_equi_occ.to_csv(PROCESSED_DIR / "equipements_occitanie.csv", index=False, encoding="utf-8")

    
    df_soins_occ.to_csv(PROCESSED_DIR / "activites_occitanie.csv", index=False, encoding="utf-8")

    logger.info("=" * 60)
    logger.info("PRÉPARATION TERMINÉE")
    logger.info("=" * 60)

    return df_final


if __name__ == "__main__":
    prepare()