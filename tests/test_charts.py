import charts
import transform

from conftest import FIXTURES


def test_chart_bar_ponctualite_par_type(con, tmp_path):
    path = charts.chart_bar_ponctualite_par_type(con, tmp_path)
    assert path.exists()
    assert path.stat().st_size > 0


def test_chart_histogramme_ponctualite(con, tmp_path):
    path = charts.chart_histogramme_ponctualite(con, tmp_path)
    assert path.exists()
    assert path.stat().st_size > 0


def test_chart_heatmap_ponctualite_par_annee(con, tmp_path):
    path = charts.chart_heatmap_ponctualite_par_annee(con, tmp_path)
    assert path.exists()
    assert path.stat().st_size > 0


def test_chart_scatter_prix_ponctualite(con, tmp_path):
    transform.build_dim_stations(con, gares_csv=FIXTURES / "gares_liaisons_sample.csv")
    transform.build_dim_liaisons(con)
    transform.build_fact_fares(
        con,
        tgv_fares_csv=FIXTURES / "tarifs_tgv_sample.csv",
        intercites_fares_csv=FIXTURES / "tarifs_intercites_sample.csv",
    )
    path = charts.chart_scatter_prix_ponctualite(con, tmp_path)
    assert path.exists()
    assert path.stat().st_size > 0


def test_run_generates_all_four_charts(con, tmp_path, monkeypatch):
    transform.build_dim_stations(con, gares_csv=FIXTURES / "gares_liaisons_sample.csv")
    transform.build_dim_liaisons(con)
    transform.build_fact_fares(
        con,
        tgv_fares_csv=FIXTURES / "tarifs_tgv_sample.csv",
        intercites_fares_csv=FIXTURES / "tarifs_intercites_sample.csv",
    )

    monkeypatch.setattr(charts, "get_connection", lambda env: con)
    monkeypatch.setattr(charts, "OUTPUT_DIR", tmp_path)
    paths = charts.run("preprod")
    assert len(paths) == 4
    assert all(p.exists() and p.stat().st_size > 0 for p in paths)
