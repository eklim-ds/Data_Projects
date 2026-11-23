"""Read-only queries behind the dashboard.

Kept separate from the Streamlit script so they are unit-testable without a browser. The dashboard
only ever reads the dbt *marts*; it never touches raw or staging tables.
"""
from __future__ import annotations

from pathlib import Path
from typing import Optional

import duckdb
import pandas as pd

REQUIRED_TABLES = ("mart_ops_briefing", "fct_sol_weather", "fct_sol_wind_rose", "mart_ingestion_audit")


class WarehouseNotReady(RuntimeError):
    """The warehouse file or its marts do not exist yet (the pipeline has not run)."""


def connect(path: Path) -> duckdb.DuckDBPyConnection:
    """Open the warehouse read-only, checking that the marts are there."""
    path = Path(path)
    if not path.exists():
        raise WarehouseNotReady(f"No warehouse at {path}. Run `make pipeline` first.")
    con = duckdb.connect(str(path), read_only=True)
    existing = {row[0] for row in con.execute("select table_name from information_schema.tables").fetchall()}
    missing = [name for name in REQUIRED_TABLES if name not in existing]
    if missing:
        con.close()
        raise WarehouseNotReady(f"Warehouse is missing {', '.join(missing)}. Run `make pipeline` first.")
    return con


def ops_briefing(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    return con.execute("select * from mart_ops_briefing order by sol").df()


def wind_rose(con: duckdb.DuckDBPyConnection, sol: Optional[int] = None) -> pd.DataFrame:
    """16 compass bins for one sol, or aggregated across all sols when ``sol`` is None."""
    where, params = ("where sol = ?", [sol]) if sol is not None else ("", [])
    return con.execute(
        f"""
        select
            direction_bin,
            compass_point,
            compass_degrees,
            cast(sum(sample_count) as bigint) as sample_count,
            sum(sample_count) * 1.0 / nullif(sum(sum(sample_count)) over (), 0) as share_of_samples
        from fct_sol_wind_rose
        {where}
        group by direction_bin, compass_point, compass_degrees
        order by direction_bin
        """,
        params,
    ).df()


def ingestion_audit(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    return con.execute("select * exclude (source_file) from mart_ingestion_audit order by ingested_at").df()
