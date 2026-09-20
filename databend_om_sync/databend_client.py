from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Iterator

from databend_driver import BlockingDatabendClient

logger = logging.getLogger(__name__)


def _ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _lit(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


@dataclass
class TableRow:
    catalog: str
    database: str
    name: str
    table_type: str  # BASE TABLE | VIEW
    engine: str
    comment: str
    is_transient: bool
    is_external: bool
    view_query: str | None


@dataclass
class ColumnRow:
    database: str
    table: str
    name: str
    data_type: str
    comment: str | None


class DatabendClient:
    def __init__(self, dsn: str):
        self._conn = BlockingDatabendClient(dsn).get_conn()

    def query(self, sql: str) -> Iterator[tuple[Any, ...]]:
        logger.debug("databend sql: %s", sql)
        for row in self._conn.query_iter(sql):
            yield row.values()

    def exec(self, sql: str) -> None:
        logger.debug("databend sql: %s", sql)
        self._conn.exec(sql)

    def catalogs(self) -> list[str]:
        return [r[0] for r in self.query("SHOW CATALOGS") if r and r[0]]

    def use_catalog(self, catalog: str) -> None:
        self.exec(f"USE CATALOG {_ident(catalog)}")

    def databases(self, catalog: str, exclude: set[str]) -> list[str]:
        self.use_catalog(catalog)
        return [r[0] for r in self.query("SELECT name FROM system.databases ORDER BY name") if r[0].lower() not in exclude]

    def tables(self, catalog: str, databases: list[str]) -> list[TableRow]:
        if not databases:
            return []
        self.use_catalog(catalog)
        in_list = ", ".join(_lit(d) for d in databases)
        sql = f"""
            SELECT t.database, t.name, t.table_type, t.engine, t.comment,
                   t.is_transient, t.is_external, v.view_query
            FROM system.tables t
            LEFT JOIN system.views v ON v.database = t.database AND v.name = t.name
            WHERE t.database IN ({in_list})
            ORDER BY t.database, t.name
        """
        return [
            TableRow(
                catalog=catalog,
                database=r[0],
                name=r[1],
                table_type=(r[2] or "BASE TABLE").upper(),
                engine=r[3] or "",
                comment=r[4] or "",
                is_transient=str(r[5]).lower() in ("true", "1"),
                is_external=bool(r[6]),
                view_query=r[7],
            )
            for r in self.query(sql)
        ]

    def columns(self, catalog: str, databases: list[str]) -> list[ColumnRow]:
        if not databases:
            return []
        self.use_catalog(catalog)
        in_list = ", ".join(_lit(d) for d in databases)
        sql = f"""
            SELECT database, table, name, data_type, comment
            FROM system.columns
            WHERE database IN ({in_list})
            ORDER BY database, table
        """
        return [ColumnRow(r[0], r[1], r[2], r[3] or "", r[4] or None) for r in self.query(sql)]

    # ---- lineage ---------------------------------------------------------------------------
    #
    # Both endpoints are restricted to TABLE/VIEW: OM has no stage entity. Names are the snapshot
    # recorded at event time; IDs are deliberately not resolved (eventual consistency is enough).

    _LINEAGE_COLUMNS = """
        updated_on, lineage_kind,
        source_lineage_key, target_lineage_key,
        source_catalog, source_database, source_name,
        target_catalog, target_database, target_name,
        column_lineage, query_info
    """

    @staticmethod
    def _lineage_base_where(kinds: list[str]) -> list[str]:
        kind_list = ", ".join(_lit(k) for k in kinds)
        return [
            "source_object_type IN ('TABLE', 'VIEW')",
            "target_object_type IN ('TABLE', 'VIEW')",
            f"lineage_kind IN ({kind_list})",
        ]

    def _paged_lineage(self, where: list[str], page_size: int) -> Iterator[tuple[Any, ...]]:
        offset = 0
        while True:
            sql = f"""
                SELECT {self._LINEAGE_COLUMNS}
                FROM system_history.lineage_history
                WHERE {' AND '.join(where)}
                ORDER BY updated_on, source_lineage_key, target_lineage_key, column_lineage_hash
                LIMIT {page_size} OFFSET {offset}
            """
            count = 0
            for row in self.query(sql):
                count += 1
                yield row
            if count < page_size:
                return
            offset += page_size

    def lineage_rows(
        self, since: datetime | None, kinds: list[str], page_size: int
    ) -> Iterator[tuple[Any, ...]]:
        where = self._lineage_base_where(kinds)
        if since is not None:
            where.append(f"updated_on > {_lit(since.strftime('%Y-%m-%d %H:%M:%S.%f'))}")
        return self._paged_lineage(where, page_size)

    def lineage_rows_for_keys(
        self, keys: list[tuple[str, str]], kinds: list[str], page_size: int
    ) -> Iterator[tuple[Any, ...]]:
        """All rows for the given (source_lineage_key, target_lineage_key) pairs, regardless of age.

        Needed because OM's PUT /lineage overwrites lineageDetails instead of merging column
        mappings, so every touched edge must be rebuilt from its complete Databend state.
        """
        if not keys:
            return iter(())
        pairs = ", ".join(f"({_lit(s)}, {_lit(t)})" for s, t in keys)
        where = self._lineage_base_where(kinds) + [f"(source_lineage_key, target_lineage_key) IN ({pairs})"]
        return self._paged_lineage(where, page_size)
