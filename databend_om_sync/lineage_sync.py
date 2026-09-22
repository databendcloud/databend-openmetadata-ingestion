"""system_history.lineage_history -> OM table lineage (with column lineage).

Design (agreed for the interim tool):
* Edge endpoints are built from the catalog/database/name snapshot in lineage_history. Nothing is
  resolved per edge; a 404 from OM means the endpoint is not (yet / any more) in OM and the edge is
  skipped. The next metadata sync + next DML on that table heal it.
* OM's PUT /lineage overwrites lineageDetails, so an incremental run first collects the
  (source_key, target_key) pairs touched since the watermark, then rebuilds each of those edges from
  *all* their rows so column mappings from older DMLs are not lost.
* One OM edge per (from_fqn, to_fqn): CREATE_VIEW wins for `source`, column mappings are unioned,
  sqlQuery is the most recent statement.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Iterable

from . import fqn
from .config import Config, StagesConfig
from .databend_client import DatabendClient
from .om_client import OpenMetadataClient
from .state import Watermark

logger = logging.getLogger(__name__)

_MANAGED_SOURCES = {"ViewLineage", "QueryLineage"}
_KEY_BATCH = 200


@dataclass
class LineageRow:
    updated_on: datetime
    kind: str
    source_key: str
    target_key: str
    from_fqn: str
    to_fqn: str
    column_lineage: dict | None
    query_id: str | None
    query_text: str | None

    @classmethod
    def from_tuple(cls, service: str, stages: StagesConfig, r: tuple[Any, ...]) -> "LineageRow":
        (
            updated_on, kind, s_key, t_key,
            s_type, s_cat, s_db, s_name,
            t_type, t_cat, t_db, t_name,
            column_lineage, query_info,
        ) = r
        info = _variant(query_info) or {}
        return cls(
            updated_on=updated_on,
            kind=kind,
            source_key=s_key,
            target_key=t_key,
            from_fqn=endpoint_fqn(service, stages, s_type, s_cat, s_db, s_name),
            to_fqn=endpoint_fqn(service, stages, t_type, t_cat, t_db, t_name),
            column_lineage=_variant(column_lineage),
            query_id=info.get("query_id"),
            query_text=info.get("query_text"),
        )


def endpoint_fqn(service: str, stages: StagesConfig, obj_type: str, cat: str, db: str, name: str) -> str:
    """STAGE rows carry empty catalog/database; they are mounted under the configured schema."""
    if obj_type == "STAGE":
        return fqn.table_fqn(service, stages.database, stages.schema, name)
    return fqn.table_fqn(service, cat, db, name)


def _variant(value: Any) -> dict | None:
    if value is None:
        return None
    if isinstance(value, (bytes, bytearray)):
        value = value.decode()
    if isinstance(value, str):
        if not value.strip():
            return None
        try:
            value = json.loads(value)
        except ValueError:
            return None
    return value if isinstance(value, dict) else None


@dataclass
class Edge:
    from_fqn: str
    to_fqn: str
    kinds: set[str] = field(default_factory=set)
    columns: dict[str, set[str]] = field(default_factory=dict)
    latest: datetime | None = None
    query_text: str | None = None

    def absorb(self, row: LineageRow) -> None:
        self.kinds.add(row.kind)
        for m in (row.column_lineage or {}).get("mappings", []):
            target = (m.get("target") or {}).get("name")
            if not target:
                continue
            sources = {s["name"] for s in m.get("sources", []) if s.get("name")}
            self.columns.setdefault(target, set()).update(sources)
        if row.query_text and (self.latest is None or row.updated_on >= self.latest):
            self.latest = row.updated_on
            self.query_text = row.query_text

    def to_lineage_details(self) -> dict:
        details: dict[str, Any] = {
            "source": "ViewLineage" if "CREATE_VIEW" in self.kinds else "QueryLineage",
        }
        if self.query_text:
            details["sqlQuery"] = self.query_text
        cols = [
            {
                "toColumn": fqn.column_fqn(self.to_fqn, to_col),
                "fromColumns": sorted(fqn.column_fqn(self.from_fqn, c) for c in from_cols),
            }
            for to_col, from_cols in sorted(self.columns.items())
            if from_cols
        ]
        if cols:
            details["columnsLineage"] = cols
        return details


def _latest_view_definitions(rows: list[LineageRow]) -> dict[str, str | None]:
    """to_fqn -> query_id of the newest CREATE VIEW statement for that view.

    Databend does not emit DELETE_EDGE on CREATE OR REPLACE VIEW (only REFRESH LINEAGE does), so
    lineage_history keeps edges from superseded definitions. A view has exactly one definition and
    all its upstream edges come from that single statement, so older statements are dropped here.
    """
    latest: dict[str, tuple[datetime, str | None]] = {}
    for r in rows:
        if r.kind != "CREATE_VIEW":
            continue
        cand = (r.updated_on, r.query_id or "")
        if r.to_fqn not in latest or cand > latest[r.to_fqn]:
            latest[r.to_fqn] = cand
    return {v: qid for v, (_, qid) in latest.items()}


def aggregate(rows: Iterable[LineageRow]) -> dict[tuple[str, str], Edge]:
    rows = list(rows)
    latest_view = _latest_view_definitions(rows)
    edges: dict[tuple[str, str], Edge] = {}
    for row in rows:
        if row.from_fqn == row.to_fqn:
            continue
        if row.kind == "CREATE_VIEW" and (row.query_id or "") != latest_view.get(row.to_fqn):
            continue
        key = (row.from_fqn, row.to_fqn)
        edge = edges.get(key)
        if edge is None:
            edge = edges[key] = Edge(row.from_fqn, row.to_fqn)
        edge.absorb(row)
    return edges


@dataclass
class LineageStats:
    rows: int = 0
    edges: int = 0
    written: int = 0
    skipped_missing: int = 0
    pruned: int = 0
    watermark: datetime | None = None


class LineageSync:
    def __init__(self, cfg: Config, db: DatabendClient, om: OpenMetadataClient):
        self.cfg = cfg
        self.db = db
        self.om = om
        self.service = cfg.service.name
        self.watermark = Watermark(cfg.lineage.state_file)

    def run(self, full: bool = False) -> LineageStats:
        stats = LineageStats()
        lc = self.cfg.lineage
        since = None if full else self.watermark.read()
        if since is not None:
            since = since - timedelta(seconds=lc.lookback_seconds)

        changed_rows = [
            LineageRow.from_tuple(self.service, self.cfg.stages, r)
            for r in self.db.lineage_rows(since, lc.include_kinds, lc.page_size)
        ]
        stats.rows = len(changed_rows)
        if not changed_rows:
            logger.info("no lineage rows since %s", since)
            return stats
        stats.watermark = max(r.updated_on for r in changed_rows)

        if since is None:
            edges = aggregate(changed_rows)
        else:
            edges = self._rebuild_touched_edges(changed_rows)
        stats.edges = len(edges)

        for edge in edges.values():
            if self.om.put_table_lineage(edge.from_fqn, edge.to_fqn, edge.to_lineage_details()):
                stats.written += 1
            else:
                stats.skipped_missing += 1
                logger.info("skip edge (endpoint missing in OM): %s -> %s", edge.from_fqn, edge.to_fqn)

        if lc.prune_view_upstreams:
            stats.pruned = self._prune_view_upstreams(edges)

        self.watermark.write(stats.watermark, {"rows": stats.rows, "edges": stats.edges})
        return stats

    def _rebuild_touched_edges(self, changed: list[LineageRow]) -> dict[tuple[str, str], Edge]:
        keys = sorted({(r.source_key, r.target_key) for r in changed})
        lc = self.cfg.lineage
        rows: list[LineageRow] = []
        for i in range(0, len(keys), _KEY_BATCH):
            rows.extend(
                LineageRow.from_tuple(self.service, self.cfg.stages, r)
                for r in self.db.lineage_rows_for_keys(keys[i : i + _KEY_BATCH], lc.include_kinds, lc.page_size)
            )
        return aggregate(rows)

    def _prune_view_upstreams(self, edges: dict[tuple[str, str], Edge]) -> int:
        """For views redefined in this batch, delete OM upstream edges Databend no longer reports.

        Only edges this tool manages (ViewLineage/QueryLineage) are candidates; Manual edges stay.
        """
        views = {e.to_fqn for e in edges.values() if "CREATE_VIEW" in e.kinds}
        if not views:
            return 0
        expected: dict[str, set[str]] = {v: set() for v in views}
        for edge in edges.values():
            if edge.to_fqn in expected:
                expected[edge.to_fqn].add(edge.from_fqn)

        pruned = 0
        for view, keep in expected.items():
            current = self.om.upstream_tables(view)
            if not current:
                continue
            for upstream, source in current.items():
                if upstream not in keep and source in _MANAGED_SOURCES:
                    self.om.delete_table_lineage(upstream, view)
                    pruned += 1
                    logger.info("pruned stale view edge %s -> %s", upstream, view)
        return pruned
