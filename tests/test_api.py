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
    monkeypatch.setattr(api_main, "VALID_API_KEYS", {"test-key"})
    api_main.limiter.reset()
    return TestClient(api_main.app, headers={"X-API-Key": "test-key"})


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


def test_stations_without_api_key_rejected(client):
    response = client.get("/stations", headers={"X-API-Key": ""})
    assert response.status_code == 401


def test_stations_with_wrong_api_key_rejected(client):
    response = client.get("/stations", headers={"X-API-Key": "not-the-right-key"})
    assert response.status_code == 401


def test_health_and_metrics_do_not_require_api_key(client):
    response = client.get("/health", headers={"X-API-Key": ""})
    assert response.status_code == 200
    response = client.get("/metrics", headers={"X-API-Key": ""})
    assert response.status_code == 200


def test_stations_quota_enforced(client):
    for _ in range(30):
        assert client.get("/stations").status_code == 200
    response = client.get("/stations")
    assert response.status_code == 429


def test_regularite_stats(client):
    response = client.get("/regularite/stats")
    assert response.status_code == 200
    types = {row["type_ligne"] for row in response.json()}
    assert types == {"grande_vitesse", "regional", "intercite"}


@pytest.mark.parametrize(
    "raw_ip, expected",
    [
        ("82.45.12.7", "82.45.0.0"),
        ("127.0.0.1", "127.0.0.0"),
        ("2001:0db8:85a3:0000:0000:8a2e:0370:7334", "2001:0db8:85a3:0:0:0:0:0"),
        (None, "unknown"),
        ("not-an-ip", "unknown"),
    ],
)
def test_anonymize_ip(raw_ip, expected):
    assert api_main.anonymize_ip(raw_ip) == expected


def test_access_log_never_contains_raw_client_ip(client, caplog):
    import logging
    import re

    with caplog.at_level(logging.INFO, logger="euromobilitydatahub.access"):
        client.get("/health")

    assert len(caplog.records) == 1
    logged_message = caplog.records[0].getMessage()
    # aucun octet non masqué de type "x.x.x.NNN" (NNN != 0) ne doit apparaître dans le log
    assert not re.search(r"\b\d{1,3}\.\d{1,3}\.\d{1,3}\.(?!0\b)\d{1,3}\b", logged_message)
    assert '"GET /health" 200' in logged_message


def test_metrics_endpoint_exposes_prometheus_format(client):
    # les compteurs Prometheus sont un état global du process : on ne peut pas viser une valeur
    # exacte (d'autres tests de ce fichier appellent aussi ces endpoints avant celui-ci), on
    # vérifie juste la présence des séries attendues avec les bons labels.
    client.get("/health")
    client.get("/stations")
    client.get("/stations/does-not-exist")

    response = client.get("/metrics")
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/plain")
    body = response.text

    assert 'api_requests_total{method="GET",path="/health",status="200"}' in body
    assert 'api_requests_total{method="GET",path="/stations",status="200"}' in body
    # le gabarit de route ("{station_id}"), pas l'ID brut "does-not-exist" -> pas d'explosion de cardinalité
    assert 'path="/stations/{station_id}",status="404"' in body
    assert "does-not-exist" not in body
    assert "api_request_duration_seconds" in body
