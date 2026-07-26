import pathlib

import duckdb
import pytest
import transform

FIXTURES = pathlib.Path(__file__).parent / "fixtures"


@pytest.fixture
def con():
    connection = duckdb.connect()
    transform.build_harmonized_table(
        connection,
        tgv_csv=FIXTURES / "regularite_tgv_sample.csv",
        ter_csv=FIXTURES / "regularite_ter_sample.csv",
        intercites_csv=FIXTURES / "regularite_intercites_sample.csv",
    )
    yield connection
    connection.close()


def test_row_count(con):
    assert con.execute("SELECT COUNT(*) FROM fact_regularite").fetchone()[0] == 4 + 4 + 3


def test_type_ligne_values(con):
    rows = con.execute("SELECT DISTINCT type_ligne FROM fact_regularite ORDER BY 1").fetchall()
    assert [r[0] for r in rows] == ["grande_vitesse", "intercite", "regional"]


def test_axe_type_per_source(con):
    axe_types = dict(con.execute("SELECT DISTINCT type_ligne, axe_type FROM fact_regularite ORDER BY 1").fetchall())
    assert axe_types == {
        "grande_vitesse": "liaison",
        "intercite": "liaison",
        "regional": "region",
    }


def test_ter_has_region_but_no_gare(con):
    row = con.execute(
        "SELECT gare_depart, gare_arrivee, region FROM fact_regularite WHERE type_ligne = 'regional' AND axe_label = 'Bretagne' LIMIT 1"
    ).fetchone()
    assert row == (None, None, "Bretagne")


def test_taux_ponctualite_tgv(con):
    taux = con.execute(
        "SELECT taux_ponctualite FROM fact_regularite "
        "WHERE type_ligne='grande_vitesse' AND mois='2024-01' AND axe_label='PARIS -> LYON'"
    ).fetchone()[0]
    assert taux == 90.0


def test_taux_ponctualite_ter(con):
    # (195 circulés - 20 en retard) / 195 * 100
    taux = con.execute(
        "SELECT taux_ponctualite FROM fact_regularite WHERE type_ligne='regional' AND mois='2024-01'"
    ).fetchone()[0]
    assert taux == 89.74


def test_taux_ponctualite_intercites(con):
    # (48 circulés - 5 en retard) / 48 * 100
    taux = con.execute(
        "SELECT taux_ponctualite FROM fact_regularite WHERE type_ligne='intercite' AND mois='2024-01'"
    ).fetchone()[0]
    assert taux == 89.58


def test_taux_annulation(con):
    # 10 annulés / 200 programmés * 100
    taux = con.execute(
        "SELECT taux_annulation FROM fact_regularite WHERE type_ligne='regional' AND mois='2024-02'"
    ).fetchone()[0]
    assert taux == 5.0


def test_taux_null_when_no_circulation_or_programmation(con):
    # ligne fictive à 0 train programmé/circulé (cas réel rencontré, ex. mois COVID à 0 circulation)
    row = con.execute(
        "SELECT taux_ponctualite, taux_annulation FROM fact_regularite "
        "WHERE type_ligne='grande_vitesse' AND axe_label='MARSEILLE -> NICE'"
    ).fetchone()
    assert row == (None, None)


def test_retard_moyen_only_available_for_tgv(con):
    tgv_retard = con.execute(
        "SELECT retard_moyen_tous_trains_arrivee_min FROM fact_regularite "
        "WHERE type_ligne='grande_vitesse' AND mois='2024-01' AND axe_label='PARIS -> LYON'"
    ).fetchone()[0]
    assert tgv_retard == 5.5

    ter_retard = con.execute(
        "SELECT retard_moyen_tous_trains_arrivee_min FROM fact_regularite WHERE type_ligne='regional' LIMIT 1"
    ).fetchone()[0]
    assert ter_retard is None

    intercite_retard = con.execute(
        "SELECT retard_moyen_tous_trains_arrivee_min FROM fact_regularite WHERE type_ligne='intercite' LIMIT 1"
    ).fetchone()[0]
    assert intercite_retard is None


def test_dev_sample_excludes_stale_axis_and_keeps_all_line_types(con):
    # Régression : "Alsace" n'a de données qu'en 2013 (région disparue lors de la fusion des
    # régions de 2016). Un tri purement alphabétique la sélectionnerait avant "Bretagne" ;
    # apply_dev_sample doit l'exclure car absente du dernier mois disponible pour le TER.
    transform.apply_dev_sample(con, {"liaisons_max": 1, "mois_max": 3})
    axes_by_type = dict(con.execute("SELECT DISTINCT type_ligne, axe_label FROM fact_regularite ORDER BY 1").fetchall())
    assert set(axes_by_type) == {"grande_vitesse", "intercite", "regional"}
    assert axes_by_type["regional"] == "Bretagne"


def test_dev_sample_keeps_only_most_recent_months(con):
    transform.apply_dev_sample(con, {"liaisons_max": 1, "mois_max": 2})
    months = {r[0] for r in con.execute("SELECT DISTINCT mois FROM fact_regularite").fetchall()}
    assert months == {"2024-02", "2024-03"}
