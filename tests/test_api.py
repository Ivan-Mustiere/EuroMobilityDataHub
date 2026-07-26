import pathlib

import duckdb
import pytest
import transform
from fastapi.testclient import TestClient

import api.main as api_main

FIXTURES = pathlib.Path(__file__).parent / "fixtures"


@pytest.fixture
def client(tmp_path, monkeypatch):
    db_path = tmp_path / "test.duckdb"
    con = duckdb.connect(str(db_path))
    transform.build_harmonized_table(
        con,
        tgv_csv=FIXTURES / "regularite_tgv_sample.csv",
        ter_csv=FIXTURES / "regularite_ter_sample.csv",
        intercites_csv=FIXTURES / "regularite_intercites_sample.csv",
    )
    transform.build_dim_stations(con, gares_csv=FIXTURES / "gares_sample.csv")
    con.close()

    monkeypatch.setitem(api_main.DB_PATHS, "dev", db_path)
    monkeypatch.setattr(api_main, "APP_ENV", "dev")
    return TestClient(api_main.app)


def test_health(client):
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok", "env": "dev"}


def test_list_stations(client):
    response = client.get("/stations")
    assert response.status_code == 200
    names = {s["nom_gare"] for s in response.json()}
    assert names == {"Paris Montparnasse", "Lyon Part Dieu"}


def test_list_stations_filter(client):
    response = client.get("/stations", params={"q": "lyon"})
    assert response.status_code == 200
    stations = response.json()
    assert len(stations) == 1
    assert stations[0]["nom_gare"] == "Lyon Part Dieu"


def test_get_station_not_found(client):
    response = client.get("/stations/does-not-exist")
    assert response.status_code == 404


def test_get_station_found(client):
    response = client.get("/stations/uuid-paris-montparnasse")
    assert response.status_code == 200
    assert response.json()["trigramme"] == "PMP"


def test_list_regularite_filter_by_type_ligne(client):
    response = client.get("/regularite", params={"type_ligne": "grande_vitesse"})
    assert response.status_code == 200
    rows = response.json()
    assert len(rows) == 4
    assert all(row["type_ligne"] == "grande_vitesse" for row in rows)


def test_regularite_stats(client):
    response = client.get("/regularite/stats")
    assert response.status_code == 200
    types = {row["type_ligne"] for row in response.json()}
    assert types == {"grande_vitesse", "regional", "intercite"}
