import pytest

from databend_openmetadata_ingestion.types import parse_type, to_om_column


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("INT NULL", {"dataType": "INT"}),
        ("UInt64", {"dataType": "BIGINT"}),
        ("BIGINT UNSIGNED NULL", {"dataType": "BIGINT"}),
        ("TINYINT UNSIGNED", {"dataType": "TINYINT"}),
        ("VARCHAR", {"dataType": "STRING"}),
        ("BINARY NULL", {"dataType": "BYTES"}),
        ("DECIMAL(18, 4)", {"dataType": "DECIMAL", "precision": 18, "scale": 4}),
        ("Decimal128(38, 0) NULL", {"dataType": "DECIMAL", "precision": 38, "scale": 0}),
        ("TIMESTAMP", {"dataType": "TIMESTAMP"}),
        ("VARIANT NULL", {"dataType": "VARIANT"}),
        ("ARRAY(INT32 NULL)", {"dataType": "ARRAY", "arrayDataType": "INT"}),
        ("ARRAY(ARRAY(STRING))", {"dataType": "ARRAY", "arrayDataType": "ARRAY"}),
        ("MAP(STRING, INT32 NULL)", {"dataType": "MAP"}),
        ("VECTOR(1536)", {"dataType": "ARRAY", "arrayDataType": "FLOAT"}),
        ("Nullable(Int32)", {"dataType": "INT"}),
        ("GEOMETRY", {"dataType": "GEOMETRY"}),
        ("FOOBAR", {"dataType": "UNKNOWN"}),
    ],
)
def test_scalar_and_container_mapping(raw, expected):
    col = to_om_column("c", raw, 1)
    for k, v in expected.items():
        assert col[k] == v
    assert col["dataTypeDisplay"] == raw
    assert col["name"] == "c"
    assert col["ordinalPosition"] == 1


def test_named_tuple_becomes_struct_with_children():
    col = to_om_column("t", "TUPLE(a INT32 NULL, b STRING NULL, c DECIMAL(10, 2))", 3)
    assert col["dataType"] == "STRUCT"
    assert [c["name"] for c in col["children"]] == ["a", "b", "c"]
    assert [c["dataType"] for c in col["children"]] == ["INT", "STRING", "DECIMAL"]
    assert col["children"][2]["precision"] == 10


def test_positional_tuple_children_are_numbered():
    col = to_om_column("t", "TUPLE(INT32, STRING)", 1)
    assert [c["name"] for c in col["children"]] == ["1", "2"]


def test_array_of_tuple_keeps_children():
    col = to_om_column("t", "ARRAY(TUPLE(x INT32, y INT32))", 1)
    assert col["arrayDataType"] == "STRUCT"
    assert [c["name"] for c in col["children"]] == ["x", "y"]


def test_parse_type_splits_only_top_level_commas():
    parsed = parse_type("MAP(STRING, TUPLE(a INT32, b DECIMAL(10, 2)))")
    assert parsed.name == "MAP"
    assert parsed.args == ["STRING", "TUPLE(a INT32, b DECIMAL(10, 2))"]


def test_comment_becomes_description():
    assert to_om_column("c", "INT", 1, "user id")["description"] == "user id"
    assert "description" not in to_om_column("c", "INT", 1, "")
