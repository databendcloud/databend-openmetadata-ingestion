"""Databend catalog/database/table -> OM Database/DatabaseSchema/Table.

Mapping follows the upstream connector PR (open-metadata/OpenMetadata#33387):
catalog -> Database, Databend database -> DatabaseSchema, table/view -> Table.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass, field

from . import fqn
from .config import Config
from .databend_client import ColumnRow, DatabendClient, TableRow
from .om_client import OMError, OpenMetadataClient
from .types import to_om_column

logger = logging.getLogger(__name__)


@dataclass
class MetadataStats:
    databases: int = 0
    schemas: int = 0
    tables: int = 0
    failed: int = 0
    deleted: int = 0
    errors: list[str] = field(default_factory=list)


def _table_type(row: TableRow) -> str:
    if row.table_type == "VIEW":
        return "View"
    if row.is_external:
        return "External"
    if row.is_transient:
        return "Transient"
    if row.engine.upper() == "ICEBERG":
        return "Iceberg"
    return "Regular"


def build_table_payload(row: TableRow, columns: list[ColumnRow]) -> dict:
    payload: dict = {
        "name": row.name,
        "tableType": _table_type(row),
        "columns": [
            to_om_column(c.name, c.data_type, ordinal, c.comment) for ordinal, c in enumerate(columns, start=1)
        ],
    }
    if row.comment:
        payload["description"] = row.comment
    if row.view_query:
        payload["schemaDefinition"] = row.view_query
    return payload


class MetadataSync:
    def __init__(self, cfg: Config, db: DatabendClient, om: OpenMetadataClient):
        self.cfg = cfg
        self.db = db
        self.om = om
        self.service = cfg.service.name

    def run(self) -> MetadataStats:
        stats = MetadataStats()
        exclude = {d.lower() for d in self.cfg.metadata.exclude_databases}
        catalogs = self.cfg.metadata.catalogs or self.db.catalogs()
        seen: dict[str, dict[str, set[str]]] = {}

        for catalog in catalogs:
            try:
                databases = self.db.databases(catalog, exclude)
            except Exception as exc:  # noqa: BLE001 — one bad catalog must not abort the rest
                stats.failed += 1
                stats.errors.append(f"catalog {catalog}: {exc}")
                logger.warning("skip catalog %s: %s", catalog, exc)
                continue

            self.om.upsert_database(self.service, catalog)
            stats.databases += 1
            db_fqn = fqn.build(self.service, catalog)
            seen[catalog] = {}

            tables = self.db.tables(catalog, databases)
            cols_by_table: dict[tuple[str, str], list[ColumnRow]] = defaultdict(list)
            for c in self.db.columns(catalog, databases):
                cols_by_table[(c.database, c.table)].append(c)

            tables_by_db: dict[str, list[TableRow]] = defaultdict(list)
            for t in tables:
                tables_by_db[t.database].append(t)

            for database in databases:
                self.om.upsert_schema(db_fqn, database)
                stats.schemas += 1
                schema_fqn = fqn.build(self.service, catalog, database)
                seen[catalog][database] = set()
                for row in tables_by_db.get(database, []):
                    payload = build_table_payload(row, cols_by_table.get((database, row.name), []))
                    try:
                        self.om.upsert_table(schema_fqn, payload)
                        stats.tables += 1
                        seen[catalog][database].add(row.name)
                    except OMError as exc:
                        stats.failed += 1
                        stats.errors.append(f"{schema_fqn}.{row.name}: {exc}")
                        logger.warning("failed table %s.%s: %s", schema_fqn, row.name, exc)

        if self.cfg.metadata.mark_deleted:
            stats.deleted = self._mark_deleted(seen)
        return stats

    def _mark_deleted(self, seen: dict[str, dict[str, set[str]]]) -> int:
        """Soft-delete OM entities absent from this run. Catalogs that failed are left untouched."""
        deleted = 0
        for database in list(self.om.list_databases(self.service)):
            catalog = database["name"]
            if catalog not in seen:
                continue
            for schema in list(self.om.list_schemas(database["fullyQualifiedName"])):
                if schema["name"] not in seen[catalog]:
                    self.om.soft_delete("databaseSchemas", schema["id"])
                    deleted += 1
                    continue
                for table in list(self.om.list_tables(schema["fullyQualifiedName"])):
                    if table["name"] not in seen[catalog][schema["name"]]:
                        self.om.soft_delete("tables", table["id"])
                        deleted += 1
        return deleted
