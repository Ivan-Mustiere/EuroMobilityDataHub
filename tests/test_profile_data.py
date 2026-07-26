import pandas as pd
import profile_data


def test_profile_dataset_counts_nulls_and_duplicates():
    df = pd.DataFrame({
        "a": [1, 2, 2, None],
        "b": ["x", "y", "y", "z"],
    })
    report = profile_data.profile_dataset(df, "test.csv")
    assert report["source"] == "test.csv"
    assert report["total_rows"] == 4
    assert report["total_columns"] == 2
    assert report["null_counts"] == {"a": 1}
    assert report["duplicate_rows"] == 1


def test_profile_dataset_no_nulls_or_duplicates_when_clean():
    df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
    report = profile_data.profile_dataset(df, "clean.csv")
    assert report["null_counts"] == {}
    assert report["duplicate_rows"] == 0


def test_profile_dataset_detects_price_outliers():
    prices = [10.0] * 99 + [10000.0]  # un seul outlier évident
    df = pd.DataFrame({"Prix maximum": prices})
    report = profile_data.profile_dataset(df, "tarifs.csv", outlier_col="Prix maximum")
    assert report["outlier_column"] == "Prix maximum"
    assert report["outliers_99e_percentile"] == 1


def test_profile_dataset_skips_outliers_without_column():
    df = pd.DataFrame({"a": [1, 2, 3]})
    report = profile_data.profile_dataset(df, "no_price.csv", outlier_col="Prix maximum")
    assert "outlier_column" not in report
    assert "outliers_99e_percentile" not in report
