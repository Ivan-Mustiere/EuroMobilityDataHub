import transform
import stats_tests

from conftest import FIXTURES


def test_compute_h1_anova_structure(con):
    result = stats_tests.compute_h1_anova(con)
    # Agrégé par liaison/région (une valeur = une moyenne sur la période) :
    # grande_vitesse = PARIS -> LYON (MARSEILLE -> NICE exclue, 0 circulation prévue -> NULL),
    # regional = Bretagne + Alsace, intercite = PARIS -> TOULOUSE.
    assert result["n_par_groupe"] == {"grande_vitesse": 1, "regional": 2, "intercite": 1}
    assert isinstance(result["f_stat"], float)
    assert isinstance(result["p_value"], float)
    assert isinstance(result["eta2"], float)
    assert result["significatif"] == (result["p_value"] < stats_tests.ALPHA)


def test_compute_eta_squared_no_effect_when_groups_equal():
    groups = {"a": [50.0, 50.0], "b": [50.0, 50.0], "c": [50.0, 50.0]}
    assert stats_tests.compute_eta_squared(groups) == 0.0


def test_compute_eta_squared_full_effect_when_no_within_group_variance():
    groups = {"a": [10.0, 10.0], "b": [50.0, 50.0], "c": [90.0, 90.0]}
    assert stats_tests.compute_eta_squared(groups) == 1.0


def test_compute_h2_spearman_too_small_without_fares(con):
    # aucune table fact_fares construite : la jointure échoue -> il faut la construire d'abord
    transform.build_dim_stations(con, gares_csv=FIXTURES / "gares_sample.csv")
    transform.build_dim_liaisons(con)
    transform.build_fact_fares(
        con,
        tgv_fares_csv=FIXTURES / "tarifs_tgv_sample.csv",
        intercites_fares_csv=FIXTURES / "tarifs_intercites_sample.csv",
    )
    # gares_sample.csv ne matche pas PARIS/LYON/TOULOUSE -> aucun prix_moyen_km non NULL
    result = stats_tests.compute_h2_spearman(con)
    assert result["n"] == 0
    assert result["rho"] is None
    assert result["significatif"] is None


def test_compute_h2_spearman_with_matched_fares(con):
    transform.build_dim_stations(con, gares_csv=FIXTURES / "gares_liaisons_sample.csv")
    transform.build_dim_liaisons(con)
    transform.build_fact_fares(
        con,
        tgv_fares_csv=FIXTURES / "tarifs_tgv_sample.csv",
        intercites_fares_csv=FIXTURES / "tarifs_intercites_sample.csv",
    )
    result = stats_tests.compute_h2_spearman(con)
    assert result["n"] == 3  # TGV INOUI + OUIGO Paris->Lyon, Intercités Paris->Toulouse
    assert isinstance(result["rho"], float)
    assert isinstance(result["p_value"], float)
