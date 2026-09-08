import pathlib
import time

import duckdb
import pytest
import transform
from fastapi.testclient import TestClient

import apps.api.main as api_main

FIXTURES = pathlib.Path(__file__).parent / "fixtures"


@pytest.fixture
def client(tmp_path, monkeypatch):
    # apps/api/main.py interroge Snowflake MART en prod (get_connection() = connexion Snowflake
    # persistante, cf. commentaire dans main.py) — inutilisable en test (réseau, coût, credentials
    # réelles). On monkeypatch get_connection() pour renvoyer une connexion DuckDB locale à la
    # place : même interface .execute(sql, params).fetchall()/.close(), et DuckDB comprend le SQL
    # standard déjà utilisé par main.py sans traduction. DuckDB ici n'est qu'un double de test
    # léger et jetable, jamais utilisé en production (cf. plan d'élimination de DuckDB).
    db_path = tmp_path / "test.duckdb"
    con = duckdb.connect(str(db_path))
    transform.build_harmonized_table(
        con,
        tgv_csv=FIXTURES / "regularite_tgv_sample.csv",
        ter_csv=FIXTURES / "regularite_ter_sample.csv",
        intercites_csv=FIXTURES / "regularite_intercites_sample.csv",
    )
    transform.build_dim_stations(con, gares_csv=FIXTURES / "gares_sample.csv")
    # apps/api/main.py interroge dim_stations_multipays (Gold, 8 pays), pas dim_stations : même
    # id_gare = pays || ':' || station_id que dbt/models/marts/dim_stations_multipays.sql.
    con.execute("CREATE TABLE dim_stations_multipays AS SELECT pays || ':' || station_id AS id_gare, * FROM dim_stations")
    con.close()

    monkeypatch.setattr(api_main, "get_connection", lambda: duckdb.connect(str(db_path), read_only=True))
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
    response = client.get("/stations/FR:uuid-paris-montparnasse")
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
    # Fenêtre fixe basée sur l'horloge réelle (slowapi) : ce test peut très rarement échouer si
    # les 31 appels chevauchent exactement un changement de minute (le compteur redémarre alors
    # à 0). Flake connu et accepté plutôt que d'ajouter une dépendance de mock de temps pour un
    # cas aussi marginal.
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


@pytest.mark.parametrize(
    "stop_id, pays, expected",
    [
        ("StopPoint:OCETGV INOUI-87688887", "FR", "FR:87688887"),
        ("StopArea:OCE83045013", "FR", "FR:83045013"),
        ("StopPoint:OCESN-8748100", "FR", "FR:8748100"),
        ("", "FR", None),
        (None, "FR", None),
    ],
)
def test_uic_from_stop_id(stop_id, pays, expected):
    assert api_main._uic_from_stop_id(stop_id, pays) == expected


def test_uic_from_stop_id_ch_sloid_fallback(monkeypatch):
    # Le flux GTFS-RT suisse encode presque jamais le code UIC dans le stop_id lui-même (format
    # "sloid") : la regex française échoue, on doit retomber sur dim_stop_uic_ch (cf. plan Suisse).
    monkeypatch.setitem(api_main._ch_stop_uic_cache, "map", {"ch:1:sloid:6302:1:1": "8506302"})
    monkeypatch.setitem(api_main._ch_stop_uic_cache, "fetched_at", time.time())
    assert api_main._uic_from_stop_id("ch:1:sloid:6302:1:1", "CH") == "CH:8506302"
    assert api_main._uic_from_stop_id("ch:1:sloid:inconnu", "CH") is None


def test_uic_from_stop_id_no_cross_country_collision(monkeypatch):
    # Régression : l'Italie/la Finlande/la Pologne/l'Allemagne/la Suède résolvent leur stop_id par
    # identité (pas de code UIC, directement sur dim_stations via _stations_by_uic — pas de table
    # dim_stop_uic_xx séparée, cf. _uic_from_stop_id) et leurs plages numériques se chevauchent
    # (ex. stop_id "1" existe à la fois en Finlande et en Allemagne) — sans le préfixe pays, un
    # référentiel bâti après un autre écraserait silencieusement le bon résultat (bug constaté :
    # trains finlandais affichés avec des gares allemandes, l'Allemagne étant chargée en dernier).
    monkeypatch.setitem(api_main._stations_by_uic_cache, "map", {
        "FI:1": {"nom_gare": "Helsinki", "latitude": 60.17, "longitude": 24.94},
        "DE:1": {"nom_gare": "Berlin", "latitude": 52.52, "longitude": 13.40},
    })
    monkeypatch.setitem(api_main._stations_by_uic_cache, "fetched_at", time.time())
    assert api_main._uic_from_stop_id("1", "FI") == "FI:1"
    assert api_main._uic_from_stop_id("1", "DE") == "DE:1"


def test_uic_from_stop_id_se(monkeypatch):
    # Suède : résolution par identité comme l'Italie/la Finlande/la Pologne/l'Allemagne (stop_id au
    # format NeTEx suédois, trop long pour la regex UIC — cf. build_se_reference), directement sur
    # dim_stations (pas de table dim_stop_uic_se séparée, cf. _uic_from_stop_id).
    monkeypatch.setitem(api_main._stations_by_uic_cache, "map", {
        "SE:9022050025317002": {"nom_gare": "København Østerport", "latitude": 55.69, "longitude": 12.59},
    })
    monkeypatch.setitem(api_main._stations_by_uic_cache, "fetched_at", time.time())
    assert api_main._uic_from_stop_id("9022050025317002", "SE") == "SE:9022050025317002"
    assert api_main._uic_from_stop_id("inconnu", "SE") is None


def _stop(stop_id, arrival_time=None, arrival_delay=None, departure_time=None, departure_delay=None):
    return {
        "stop_id": stop_id,
        "arrival_time": arrival_time,
        "arrival_delay": arrival_delay,
        "departure_time": departure_time,
        "departure_delay": departure_delay,
    }


def test_interpolate_trip_en_route_midpoint():
    stops = [
        _stop("A", departure_time=1000),
        _stop("B", arrival_time=1100, arrival_delay=300),
    ]
    result = api_main._interpolate_trip(stops, now=1050)
    assert result["statut"] == "en_route"
    assert result["stop_id_precedent"] == "A"
    assert result["stop_id_suivant"] == "B"
    assert result["progression"] == 0.5
    assert result["retard_s"] == 300


def test_interpolate_trip_not_yet_departed():
    stops = [
        _stop("A", departure_time=1000, departure_delay=0),
        _stop("B", arrival_time=1100),
    ]
    result = api_main._interpolate_trip(stops, now=900)
    assert result["statut"] == "a_quai"
    assert result["stop_id_precedent"] == result["stop_id_suivant"] == "A"
    assert result["progression"] == 0.0


def test_interpolate_trip_finished_returns_none():
    stops = [
        _stop("A", departure_time=1000),
        _stop("B", arrival_time=1100),
    ]
    assert api_main._interpolate_trip(stops, now=1200) is None


def test_interpolate_trip_insufficient_schedule_returns_none():
    assert api_main._interpolate_trip([_stop("A")], now=1000) is None
    assert api_main._interpolate_trip([], now=1000) is None


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
    assert 'path="/stations/{id_gare}",status="404"' in body
    assert "does-not-exist" not in body
    assert "api_request_duration_seconds" in body
