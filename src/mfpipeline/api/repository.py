"""Read-only queries against the dbt marts and the MongoDB scheme registry."""
from typing import Any

import psycopg
from psycopg.rows import dict_row
from pymongo import MongoClient

from mfpipeline.config import Settings

SCORECARD_SORT_COLUMNS = {"ret_1y", "cagr_3y", "cagr_5y", "sharpe_1y", "vol_1y"}


class FundRepository:
    def __init__(self, settings: Settings):
        self.dsn = (
            f"host={settings.warehouse_host} port={settings.warehouse_port} dbname={settings.warehouse_db} "
            f"user={settings.warehouse_user} password={settings.warehouse_password}"
        )
        self.mongo = MongoClient(settings.mongo_uri, serverSelectionTimeoutMS=3000)

    def _query(self, sql: str, params: dict | None = None) -> list[dict[str, Any]]:
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            return conn.execute(sql, params or {}).fetchall()

    def list_funds(self, category: str | None, fund_house: str | None, plan: str | None,
                   sort_by: str, limit: int) -> list[dict]:
        if sort_by not in SCORECARD_SORT_COLUMNS:
            raise ValueError(f"sort_by must be one of {sorted(SCORECARD_SORT_COLUMNS)}")
        # sort_by is checked against an allow-list above; every value is a bound parameter.
        return self._query(
            f"""
            select * from marts.mart_fund_scorecard
            where (%(category)s::text is null or category = %(category)s)
              and (%(fund_house)s::text is null or fund_house = %(fund_house)s)
              and (%(plan)s::text is null or plan = %(plan)s)
            order by {sort_by} desc nulls last, scheme_code
            limit %(limit)s
            """,
            {"category": category, "fund_house": fund_house, "plan": plan, "limit": limit},
        )

    def get_fund(self, scheme_code: int) -> dict | None:
        rows = self._query("select * from marts.mart_fund_scorecard where scheme_code = %(code)s",
                           {"code": scheme_code})
        return rows[0] if rows else None

    def get_registry_entry(self, scheme_code: int) -> dict | None:
        return self.mongo["mf_registry"]["schemes"].find_one({"_id": scheme_code}, {"_id": 0, "updated_at": 0})

    def get_history(self, scheme_code: int, months: int) -> list[dict]:
        return self._query(
            """
            select month, as_of_date, nav, ret_1m, ret_1y, cagr_3y, vol_1y, max_drawdown_1y, sharpe_1y
            from marts.fct_fund_metrics_monthly
            where scheme_code = %(code)s
            order by month desc
            limit %(months)s
            """,
            {"code": scheme_code, "months": months},
        )

    def list_categories(self) -> list[dict]:
        return self._query("select * from marts.mart_category_summary order by asset_class, category, plan")
