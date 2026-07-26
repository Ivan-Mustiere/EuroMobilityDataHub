import export_results
import transform

from conftest import FIXTURES


def test_query_returns_one_row_per_axe_with_enough_history(con):
    transform.build_dim_stations(con, gares_csv=FIXTURES / "gares_sample.csv")
    transform.build_dim_liaisons(con)
    transform.build_fact_fares(
        con,
        tgv_fares_csv=FIXTURES / "tarifs_tgv_sample.csv",
        intercites_fares_csv=FIXTURES / "tarifs_intercites_sample.csv",
    )
    # les fixtures fact_regularite ont 3 mois par axe -> sous le seuil >= 12, donc vide ici ;
    # on vérifie surtout que la requête s'exécute et renvoie le bon schéma de colonnes
    result = con.execute(export_results.QUERY)
    columns = [d[0] for d in result.description]
    assert columns == [
        "type_ligne", "axe_label", "nb_mois", "taux_ponctualite_moyen",
        "taux_annulation_moyen", "distance_km", "prix_moyen_km",
    ]
    assert result.fetchall() == []


def test_query_includes_distance_and_prix_km_when_available(con):
    transform.build_dim_stations(con, gares_csv=FIXTURES / "gares_liaisons_sample.csv")
    transform.build_dim_liaisons(con)
    transform.build_fact_fares(
        con,
        tgv_fares_csv=FIXTURES / "tarifs_tgv_sample.csv",
        intercites_fares_csv=FIXTURES / "tarifs_intercites_sample.csv",
    )
    # abaisse artificiellement le seuil d'historique pour tester sur les fixtures (3 mois)
    query = export_results.QUERY.replace("HAVING COUNT(*) >= 12", "HAVING COUNT(*) >= 1")
    rows = con.execute(query).fetchall()
    by_axe = {r[1]: r for r in rows}
    assert by_axe["PARIS -> LYON"][5] is not None  # distance_km
    assert by_axe["PARIS -> LYON"][6] is not None  # prix_moyen_km
