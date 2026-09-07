import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "apps" / "pipeline"))
sys.path.insert(0, str(ROOT / "analysis"))
sys.path.insert(0, str(ROOT))

import duckdb  # noqa: E402
import pytest  # noqa: E402
import transform  # noqa: E402

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
