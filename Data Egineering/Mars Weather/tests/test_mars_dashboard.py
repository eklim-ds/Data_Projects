from pathlib import Path

import pytest

pytest.importorskip("duckdb")
pytest.importorskip("dbt.cli.main")
AppTest = pytest.importorskip("streamlit.testing.v1").AppTest

APP = Path(__file__).resolve().parents[1] / "dashboard" / "app.py"


@pytest.fixture
def run_app(monkeypatch):
    def _run(warehouse):
        monkeypatch.setenv("WAREHOUSE_PATH", str(warehouse))
        return AppTest.from_file(str(APP), default_timeout=60).run()

    return _run


def test_page_renders_the_latest_sol_and_its_tabs(run_app, rolling_window_warehouse):
    at = run_app(rolling_window_warehouse)
    assert not at.exception
    assert {m.label: m.value for m in at.metric}["Latest sol"] == "681"
    assert len(at.tabs) == 4
    assert not at.info  # the last run brought new sols, so no "feed is frozen" banner


def test_a_frozen_feed_is_explained_rather_than_presented_as_live(run_app, frozen_feed_warehouse):
    at = run_app(frozen_feed_warehouse)
    assert not at.exception
    assert len(at.info) == 1
    assert "none were new" in at.info[0].value
