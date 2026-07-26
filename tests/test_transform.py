import transform

from conftest import FIXTURES


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


def test_dim_stations_parses_lat_lon(con):
    transform.build_dim_stations(con, gares_csv=FIXTURES / "gares_sample.csv")
    assert con.execute("SELECT COUNT(*) FROM dim_stations").fetchone()[0] == 2
    row = con.execute(
        "SELECT nom_gare, nom_gare_norm, latitude, longitude FROM dim_stations WHERE trigramme = 'PMP'"
    ).fetchone()
    assert row == ("Paris Montparnasse", "PARIS MONTPARNASSE", 48.8422, 2.3219)


def test_dim_liaisons_computes_haversine_distance_when_matched(con):
    # "Paris"/"Lyon" (génériques) rapprochent exactement les gares PARIS/LYON du fixture TGV
    transform.build_dim_stations(con, gares_csv=FIXTURES / "gares_liaisons_sample.csv")
    transform.build_dim_liaisons(con)
    row = con.execute(
        "SELECT distance_km FROM dim_liaisons WHERE gare_depart = 'PARIS' AND gare_arrivee = 'LYON'"
    ).fetchone()
    assert row is not None
    # distance Paris-Lyon à vol d'oiseau ≈ 392 km
    assert 380 < row[0] < 400


def test_dim_liaisons_null_when_station_not_matched(con):
    # gares_sample.csv ne contient que "Paris Montparnasse"/"Lyon Part Dieu" : ne matche pas les
    # libellés bruts PARIS/LYON/MARSEILLE/NICE/TOULOUSE des fixtures fact_regularite -> NULL partout
    transform.build_dim_stations(con, gares_csv=FIXTURES / "gares_sample.csv")
    transform.build_dim_liaisons(con)
    rows = con.execute("SELECT distance_km FROM dim_liaisons").fetchall()
    assert len(rows) > 0
    assert all(r[0] is None for r in rows)


def test_fact_fares_type_ligne_mapping_and_prix_moyen(con):
    transform.build_dim_stations(con, gares_csv=FIXTURES / "gares_liaisons_sample.csv")
    transform.build_dim_liaisons(con)
    transform.build_fact_fares(
        con,
        tgv_fares_csv=FIXTURES / "tarifs_tgv_sample.csv",
        intercites_fares_csv=FIXTURES / "tarifs_intercites_sample.csv",
    )
    rows = con.execute(
        "SELECT type_ligne, transporteur, prix_moyen FROM fact_fares ORDER BY transporteur"
    ).fetchall()
    assert ("intercite", "Intercités de jour à réservation obligatoire", 60.0) in rows
    assert ("grande_vitesse", "OUIGO", 25.0) in rows
    assert ("grande_vitesse", "TGV INOUI", 60.0) in rows


def test_fact_fares_computes_prix_moyen_km_when_matched(con):
    # "Paris"/"Lyon"/"Toulouse" (génériques) rapprochent exactement les libellés des fixtures tarifs
    transform.build_dim_stations(con, gares_csv=FIXTURES / "gares_liaisons_sample.csv")
    transform.build_dim_liaisons(con)
    transform.build_fact_fares(
        con,
        tgv_fares_csv=FIXTURES / "tarifs_tgv_sample.csv",
        intercites_fares_csv=FIXTURES / "tarifs_intercites_sample.csv",
    )
    tgv_row = con.execute(
        "SELECT distance_km, prix_moyen_km FROM fact_fares WHERE transporteur = 'TGV INOUI'"
    ).fetchone()
    assert tgv_row[0] is not None and 380 < tgv_row[0] < 400
    assert tgv_row[1] == round(60.0 / tgv_row[0], 3)

    ic_row = con.execute(
        "SELECT distance_km, prix_moyen_km FROM fact_fares WHERE type_ligne = 'intercite'"
    ).fetchone()
    assert ic_row[0] is not None and 550 < ic_row[0] < 620
    assert ic_row[1] == round(60.0 / ic_row[0], 3)


def test_fact_fares_null_prix_km_when_liaison_not_matched(con):
    # gares_sample.csv ne contient pas PARIS/LYON/TOULOUSE tels quels -> pas de distance -> NULL
    transform.build_dim_stations(con, gares_csv=FIXTURES / "gares_sample.csv")
    transform.build_dim_liaisons(con)
    transform.build_fact_fares(
        con,
        tgv_fares_csv=FIXTURES / "tarifs_tgv_sample.csv",
        intercites_fares_csv=FIXTURES / "tarifs_intercites_sample.csv",
    )
    rows = con.execute("SELECT prix_moyen_km FROM fact_fares").fetchall()
    assert len(rows) > 0
    assert all(r[0] is None for r in rows)
