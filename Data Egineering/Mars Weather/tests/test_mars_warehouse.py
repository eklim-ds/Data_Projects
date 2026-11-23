"""Warehouse tests.

The `*_warehouse` fixtures (conftest.py) land realistic payloads and run the real dbt project, including
all of its data tests. If any dbt test fails, the fixture fails with the dbt output, so these tests also
cover the dbt checks.
"""
import pytest

duckdb = pytest.importorskip("duckdb")
pytest.importorskip("dbt.cli.main")


def query(warehouse, sql, params=None):
    con = duckdb.connect(str(warehouse), read_only=True)
    try:
        return con.execute(sql, params or []).fetchall()
    finally:
        con.close()


def test_overlapping_pulls_collapse_to_one_row_per_sol_with_the_latest_revision(rolling_window_warehouse):
    rows = query(rolling_window_warehouse, "select sol, temp_avg_c, times_ingested from fct_sol_weather order by sol")
    assert [r[0] for r in rows] == [675, 676, 677, 678, 679, 680, 681]

    by_sol = {r[0]: r for r in rows}
    assert by_sol[679][1] == -50.0  # revised on day 2 (day 1 said -62.551)
    assert by_sol[679][2] == 2  # delivered twice
    assert by_sol[675][2] == 1


def test_business_rules_flag_high_wind_and_insufficient_data(rolling_window_warehouse):
    statuses = dict(query(rolling_window_warehouse, "select sol, ops_status from mart_ops_briefing"))
    assert statuses == {
        675: "CAUTION",  # peak 22.5 m/s > 20
        676: "NO_GO",  # peak 26.9 m/s > 25
        677: "CAUTION",  # peak 23.2 m/s > 20
        678: "NO_DATA",  # only 3 hours of wind coverage in the day-2 copy
        679: "GO",
        680: "CAUTION",  # peak 24.2 m/s > 20
        681: "GO",
    }
    (reason,) = query(rolling_window_warehouse, "select ops_status_reason from mart_ops_briefing where sol = 676")[0]
    assert reason == "Peak wind 26.9 m/s exceeds the 25 m/s no-go limit"


def test_a_frozen_feed_adds_no_new_data_and_the_audit_reports_it(frozen_feed_warehouse):
    assert query(frozen_feed_warehouse, "select count(*) from fct_sol_weather") == [(7,)]
    assert query(frozen_feed_warehouse, "select new_sols, repeated_sols from mart_ingestion_audit order by ingested_at") == [
        (7, 0),
        (0, 7),
    ]
