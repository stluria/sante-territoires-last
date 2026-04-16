"""
Tests unitaires - Pipeline FINESS Occitanie
============================================
Couvre : nettoyage (C2/C3), stockage (C4), API (C5)
"""

import sqlite3
import sys
from pathlib import Path

import pandas as pd
import pytest

# Ajoute src/ au path pour les imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
sys.path.insert(0, str(Path(__file__).parent.parent / "api"))

BASE_DIR = Path(__file__).parent.parent
DB_PATH = BASE_DIR / "data" / "finess_occitanie.db"
PROCESSED_DIR = BASE_DIR / "data" / "processed"

OCCITANIE_DEPTS = {"09", "11", "12", "30", "31", "32", "34", "46", "48", "65", "66", "81", "82"}


# ---------------------------------------------------------------------------
# Tests de nettoyage (C2/C3)
# ---------------------------------------------------------------------------

class TestNettoyage:

    def test_clean_nofiness(self):
        """Un numéro FINESS doit avoir exactement 9 caractères."""
        from prepare import clean_nofiness
        series = pd.Series([10001246, 310001234, 9999999])
        result = clean_nofiness(series)
        assert all(result.str.len() == 9)

    def test_clean_department(self):
        """Les codes département doivent être normalisés sur 2 chiffres."""
        from prepare import clean_department
        assert clean_department("9") == "09"
        assert clean_department("31") == "31"
        assert clean_department("066") == "06"

    def test_lambert93_to_wgs84_valid(self):
        """Coordonnées Lambert93 valides → lat/lon dans les bornes France."""
        from prepare import convert_lambert_to_wgs84
        import pandas as pd
        df = pd.DataFrame({"coordxet": [574960.0], "coordyet": [6277723.0]})
        df = convert_lambert_to_wgs84(df)
        assert df["latitude"].notna().all()
        assert df["longitude"].notna().all()
        assert 41 <= df["latitude"].iloc[0] <= 52
        assert -5 <= df["longitude"].iloc[0] <= 10

    def test_lambert93_to_wgs84_zero(self):
        """Coordonnées (0, 0) doivent retourner None."""
        from prepare import convert_lambert_to_wgs84
        import pandas as pd
        df = pd.DataFrame({"coordxet": [0.0], "coordyet": [0.0]})
        df = convert_lambert_to_wgs84(df)
        assert df["latitude"].isna().all()
        assert df["longitude"].isna().all()

    def test_occitanie_filter(self):
        """Seuls les 13 départements d'Occitanie doivent être présents."""
        path = PROCESSED_DIR / "etablissements_occitanie.csv"
        if not path.exists():
            pytest.skip("Pipeline non exécuté")
        df = pd.read_csv(path, dtype=str)
        invalid = set(df["departement"].dropna().unique()) - OCCITANIE_DEPTS
        assert not invalid, f"Départements hors Occitanie : {invalid}"

    def test_no_null_nofinesset(self):
        """nofinesset ne doit contenir aucune valeur nulle."""
        path = PROCESSED_DIR / "etablissements_occitanie.csv"
        if not path.exists():
            pytest.skip("Pipeline non exécuté")
        df = pd.read_csv(path, dtype=str)
        assert df["nofinesset"].isnull().sum() == 0

    def test_nofinesset_format(self):
        """Tous les numéros FINESS doivent faire exactement 9 caractères."""
        path = PROCESSED_DIR / "etablissements_occitanie.csv"
        if not path.exists():
            pytest.skip("Pipeline non exécuté")
        df = pd.read_csv(path, dtype=str)
        invalid = df[df["nofinesset"].str.len() != 9]
        assert len(invalid) == 0, f"{len(invalid)} numéros FINESS mal formés"

    def test_no_duplicates_nofinesset(self):
        """Pas de doublons sur nofinesset dans la table établissements."""
        path = PROCESSED_DIR / "etablissements_occitanie.csv"
        if not path.exists():
            pytest.skip("Pipeline non exécuté")
        df = pd.read_csv(path, dtype=str)
        assert df["nofinesset"].duplicated().sum() == 0

    def test_code_insee_format(self):
        """Le code INSEE doit faire 5 caractères."""
        path = PROCESSED_DIR / "etablissements_occitanie.csv"
        if not path.exists():
            pytest.skip("Pipeline non exécuté")
        df = pd.read_csv(path, dtype=str)
        if "code_insee" in df.columns:
            invalid = df[df["code_insee"].notna() & (df["code_insee"].str.len() != 5)]
            assert len(invalid) == 0, f"{len(invalid)} codes INSEE mal formés"

    def test_groupe_equipement_present(self):
        """La colonne groupe_equipement doit exister dans les équipements."""
        path = PROCESSED_DIR / "equipements_occitanie.csv"
        if not path.exists():
            pytest.skip("Pipeline non exécuté")
        df = pd.read_csv(path, dtype=str)
        assert "groupe_equipement" in df.columns

    def test_groupe_activites_present(self):
        """La colonne groupe_activites doit exister dans les activités."""
        path = PROCESSED_DIR / "activites_occitanie.csv"
        if not path.exists():
            pytest.skip("Pipeline non exécuté")
        df = pd.read_csv(path, dtype=str)
        assert "groupe_activites" in df.columns

    def test_coordonnees_wgs84_range(self):
        """Les lat/lon doivent être dans les bornes de la France métropolitaine."""
        path = PROCESSED_DIR / "etablissements_occitanie.csv"
        if not path.exists():
            pytest.skip("Pipeline non exécuté")
        df = pd.read_csv(path)
        if "latitude" in df.columns and "longitude" in df.columns:
            df_geo = df[df["latitude"].notna() & df["longitude"].notna()]
            if len(df_geo) > 0:
                pct_lat_ok = df_geo["latitude"].between(41, 52).mean()
                pct_lon_ok = df_geo["longitude"].between(-5, 10).mean()
                assert pct_lat_ok > 0.95, f"Trop de latitudes hors France : {1-pct_lat_ok:.1%}"
                assert pct_lon_ok > 0.95, f"Trop de longitudes hors France : {1-pct_lon_ok:.1%}"

    def test_communes_columns(self):
        """Le fichier communes doit contenir les colonnes attendues par prepare.py."""
        # Accepte les deux formats possibles
        path_riche = BASE_DIR / "data" / "raw" / "communes-france-2025.csv"
        path_insee  = BASE_DIR / "data" / "raw" / "v_commune_2025.csv"
        if path_riche.exists():
            df = pd.read_csv(path_riche, dtype=str, nrows=5)
            assert "code_insee" in df.columns, "Colonne code_insee manquante"
            assert "latitude_centre" in df.columns, "Colonne latitude_centre manquante"
        elif path_insee.exists():
            df = pd.read_csv(path_insee, dtype=str, nrows=5)
            assert "COM" in df.columns, "Colonne COM manquante dans v_commune_2025.csv"
            assert "DEP" in df.columns, "Colonne DEP manquante dans v_commune_2025.csv"
        else:
            pytest.skip("Aucun fichier communes trouvé")


# ---------------------------------------------------------------------------
# Tests de stockage (C4)
# ---------------------------------------------------------------------------

class TestStockage:

    def test_database_exists(self):
        assert DB_PATH.exists(), f"Base de données absente : {DB_PATH}"

    def test_tables_exist(self):
        if not DB_PATH.exists():
            pytest.skip("Base non créée")
        conn = sqlite3.connect(DB_PATH)
        tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
        conn.close()
        assert {"etablissements", "equipements", "activites", "departements"} <= tables

    def test_etablissements_count(self):
        if not DB_PATH.exists():
            pytest.skip("Base non créée")
        conn = sqlite3.connect(DB_PATH)
        count = conn.execute("SELECT COUNT(*) FROM etablissements").fetchone()[0]
        conn.close()
        assert count > 0

    def test_all_depts_occitanie(self):
        if not DB_PATH.exists():
            pytest.skip("Base non créée")
        conn = sqlite3.connect(DB_PATH)
        depts = {r[0] for r in conn.execute("SELECT DISTINCT departement FROM etablissements")}
        conn.close()
        invalid = depts - OCCITANIE_DEPTS
        assert not invalid, f"Départements hors Occitanie : {invalid}"

    def test_groupe_equipement_in_db(self):
        if not DB_PATH.exists():
            pytest.skip("Base non créée")
        conn = sqlite3.connect(DB_PATH)
        cols = {r[1] for r in conn.execute("PRAGMA table_info(equipements)").fetchall()}
        conn.close()
        assert "groupe_equipement" in cols

    def test_groupe_activites_in_db(self):
        if not DB_PATH.exists():
            pytest.skip("Base non créée")
        conn = sqlite3.connect(DB_PATH)
        cols = {r[1] for r in conn.execute("PRAGMA table_info(activites)").fetchall()}
        conn.close()
        assert "groupe_activites" in cols

    def test_latitude_longitude_in_db(self):
        if not DB_PATH.exists():
            pytest.skip("Base non créée")
        conn = sqlite3.connect(DB_PATH)
        cols = {r[1] for r in conn.execute("PRAGMA table_info(etablissements)").fetchall()}
        conn.close()
        assert "latitude" in cols and "longitude" in cols

    def test_ref_departements_complete(self):
        if not DB_PATH.exists():
            pytest.skip("Base non créée")
        conn = sqlite3.connect(DB_PATH)
        count = conn.execute("SELECT COUNT(*) FROM departements").fetchone()[0]
        conn.close()
        assert count == 13, f"Attendu 13 départements, trouvé {count}"


# ---------------------------------------------------------------------------
# Tests API (C5)
# ---------------------------------------------------------------------------

class TestAPI:

    @pytest.fixture
    def client(self):
        try:
            from fastapi.testclient import TestClient
            from api import app
            return TestClient(app)
        except Exception:
            pytest.skip("API non disponible")

    def test_health_endpoint(self, client):
        response = client.get("/health")
        assert response.status_code == 200
        assert "status" in response.json()

    def test_auth_required(self, client):
        assert client.get("/etablissements").status_code == 403

    def test_auth_with_valid_key(self, client):
        response = client.get("/etablissements", headers={"X-API-Key": "finess-occitanie-2025-demo"})
        assert response.status_code == 200
        assert isinstance(response.json(), list)

    def test_stats_region(self, client):
         response = client.get(
             "/stats/region",
             headers={"X-API-Key": "finess-occitanie-2025-demo"}
         )
         assert response.status_code == 200
         data = response.json()
         assert "total_etablissements" in data
         assert data["total_etablissements"] > 0

    def test_filter_by_department(self, client):
        response = client.get(
            "/etablissements?departement=31&limit=10",
            headers={"X-API-Key": "finess-occitanie-2025-demo"}
        )
        assert response.status_code == 200
        assert all(e["departement"] == "31" for e in response.json())

    def test_etablissement_not_found(self, client):
        assert client.get(
            "/etablissements/000000000",
            headers={"X-API-Key": "finess-occitanie-2025-demo"}
        ).status_code == 404

    

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
