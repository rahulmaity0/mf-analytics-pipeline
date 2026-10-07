import os
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
# Spark's Python worker processes don't see sys.path changes, only PYTHONPATH.
os.environ["PYTHONPATH"] = os.pathsep.join(filter(None, [str(ROOT / "src"), os.environ.get("PYTHONPATH")]))

FIXTURES = ROOT / "tests" / "fixtures"


@pytest.fixture(scope="session")
def spark():
    pyspark = pytest.importorskip("pyspark")  # noqa: F841 - API tests run without Spark installed
    from mfpipeline.config import get_settings
    from mfpipeline.spark import build_spark

    session = build_spark(get_settings(), "mf-tests", with_s3=False)
    yield session
    session.stop()


@pytest.fixture
def fixture_paths() -> list[str]:
    return [p.resolve().as_uri() for p in sorted(FIXTURES.glob("amfi_nav_*.txt"))]
