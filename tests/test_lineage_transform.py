import json
from datetime import datetime

from databend_openmetadata_ingestion import fqn
from databend_openmetadata_ingestion.config import StagesConfig
from databend_openmetadata_ingestion.lineage_sync import LineageRow, aggregate

STAGES = StagesConfig(enabled=True, database="default", schema="stages")

SVC = "databend_prod"


def _row(kind, src, tgt, mappings, query, ts, keys=None, query_id="q"):
    s_cat, s_db, s_name = src
    t_cat, t_db, t_name = tgt
    s_key, t_key = keys or (f"{s_cat}.{s_db}.{s_name}", f"{t_cat}.{t_db}.{t_name}")
    column_lineage = json.dumps(
        {
            "source_column_address_kind": "ID",
            "target_column_address_kind": "ID",
            "mappings": [
                {"target": {"name": t, "id": i}, "sources": [{"name": s, "id": 100 + j} for j, s in enumerate(ss)]}
                for i, (t, ss) in enumerate(mappings.items())
            ],
        }
    )
    query_info = json.dumps({"query_id": query_id, "query_text": query})
    s_type = "STAGE" if s_cat == "" else "TABLE"
    t_type = "STAGE" if t_cat == "" else "TABLE"
    return LineageRow.from_tuple(
        SVC,
        STAGES,
        (ts, kind, s_key, t_key, s_type, s_cat, s_db, s_name, t_type, t_cat, t_db, t_name, column_lineage, query_info),
    )


def test_fqn_quotes_parts_with_dots():
    assert fqn.table_fqn("svc", "default", "db", "t") == "svc.default.db.t"
    assert fqn.table_fqn("svc", "default", "my.db", "t") == 'svc.default."my.db".t'
    assert fqn.column_fqn("svc.default.db.t", "a.b") == 'svc.default.db.t."a.b"'


def test_rows_for_same_edge_are_unioned_and_view_kind_wins():
    t1 = datetime(2026, 1, 1, 10, 0, 0)
    t2 = datetime(2026, 1, 1, 11, 0, 0)
    rows = [
        _row("DML", ("default", "db", "src"), ("default", "db", "tgt"), {"a": ["x"]}, "insert 1", t1),
        _row("DML", ("default", "db", "src"), ("default", "db", "tgt"), {"a": ["y"], "b": ["z"]}, "insert 2", t2),
        _row("CREATE_VIEW", ("default", "db", "src"), ("default", "db", "tgt"), {}, "create view", t1),
    ]
    edges = aggregate(rows)
    assert len(edges) == 1
    edge = edges[("databend_prod.default.db.src", "databend_prod.default.db.tgt")]
    details = edge.to_lineage_details()
    assert details["source"] == "ViewLineage"
    assert details["sqlQuery"] == "insert 2"
    assert details["columnsLineage"] == [
        {
            "toColumn": "databend_prod.default.db.tgt.a",
            "fromColumns": ["databend_prod.default.db.src.x", "databend_prod.default.db.src.y"],
        },
        {"toColumn": "databend_prod.default.db.tgt.b", "fromColumns": ["databend_prod.default.db.src.z"]},
    ]


def test_self_edges_are_dropped_and_missing_columns_omitted():
    ts = datetime(2026, 1, 1)
    rows = [
        _row("DML", ("default", "db", "t"), ("default", "db", "t"), {"a": ["a"]}, "insert self", ts),
        _row("CTAS", ("default", "db", "s"), ("default", "db", "t"), {}, "create table as", ts),
    ]
    edges = aggregate(rows)
    assert list(edges) == [("databend_prod.default.db.s", "databend_prod.default.db.t")]
    details = next(iter(edges.values())).to_lineage_details()
    assert details == {"source": "QueryLineage", "sqlQuery": "create table as"}


def test_only_latest_create_view_statement_is_kept():
    t1 = datetime(2026, 1, 1, 10)
    t2 = datetime(2026, 1, 1, 11)
    v = ("default", "db", "v")
    rows = [
        _row("CREATE_VIEW", ("default", "db", "old"), v, {}, "create view v as select from old", t1, query_id="q1"),
        _row("CREATE_VIEW", ("default", "db", "a"), v, {}, "create view v as select from a,b", t2, query_id="q2"),
        _row("CREATE_VIEW", ("default", "db", "b"), v, {}, "create view v as select from a,b", t2, query_id="q2"),
        _row("DML", ("default", "db", "old"), ("default", "db", "t"), {}, "insert", t1, query_id="q0"),
    ]
    edges = aggregate(rows)
    assert {k[0].rsplit(".", 1)[1] for k in edges if k[1].endswith(".v")} == {"a", "b"}
    assert ("databend_prod.default.db.old", "databend_prod.default.db.t") in edges


def test_variant_columns_accept_bytes_and_none():
    ts = datetime(2026, 1, 1)
    row = LineageRow.from_tuple(
        SVC,
        STAGES,
        (ts, "DML", "k1", "k2", "TABLE", "default", "db", "a", "TABLE", "default", "db", "b", None, b'{"query_text": "q"}'),
    )
    assert row.column_lineage is None
    assert row.query_text == "q"


def test_stage_endpoints_are_mounted_under_configured_schema():
    ts = datetime(2026, 1, 1, 10)
    rows = [
        _row("DML", ("default", "shop", "orders"), ("", "", "landing"), {}, "copy into @landing from orders", ts),
        _row("DML", ("", "", "landing"), ("default", "shop", "t2"), {}, "copy into t2 from @landing", ts),
    ]
    edges = aggregate(rows)
    assert ("databend_prod.default.shop.orders", "databend_prod.default.stages.landing") in edges
    assert ("databend_prod.default.stages.landing", "databend_prod.default.shop.t2") in edges
    assert "columnsLineage" not in edges[("databend_prod.default.stages.landing", "databend_prod.default.shop.t2")].to_lineage_details()
