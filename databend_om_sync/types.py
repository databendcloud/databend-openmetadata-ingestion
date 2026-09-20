"""Databend `system.columns.data_type` -> OpenMetadata Column payload."""

from __future__ import annotations

import re
from dataclasses import dataclass, field

_SCALAR = {
    "BOOLEAN": "BOOLEAN",
    "BOOL": "BOOLEAN",
    "TINYINT": "TINYINT",
    "INT8": "TINYINT",
    "UINT8": "TINYINT",
    "SMALLINT": "SMALLINT",
    "INT16": "SMALLINT",
    "UINT16": "SMALLINT",
    "INT": "INT",
    "INTEGER": "INT",
    "INT32": "INT",
    "UINT32": "INT",
    "BIGINT": "BIGINT",
    "INT64": "BIGINT",
    "UINT64": "BIGINT",
    "FLOAT": "FLOAT",
    "FLOAT32": "FLOAT",
    "DOUBLE": "DOUBLE",
    "FLOAT64": "DOUBLE",
    "STRING": "STRING",
    "VARCHAR": "STRING",
    "TEXT": "STRING",
    "CHAR": "STRING",
    # OM rejects BINARY without dataLength; Databend BINARY is unbounded.
    "BINARY": "BYTES",
    "VARBINARY": "BYTES",
    "DATE": "DATE",
    "TIMESTAMP": "TIMESTAMP",
    "DATETIME": "TIMESTAMP",
    "INTERVAL": "INTERVAL",
    "VARIANT": "VARIANT",
    "JSON": "JSON",
    "BITMAP": "BITMAP",
    "GEOMETRY": "GEOMETRY",
    "GEOGRAPHY": "GEOGRAPHY",
}

_DECIMAL = re.compile(r"^DECIMAL(?:128|256)?$")


@dataclass
class ParsedType:
    name: str
    args: list[str] = field(default_factory=list)


def _strip_nullable(s: str) -> str:
    s = s.strip()
    for suffix in (" NOT NULL", " NULL"):
        if s.upper().endswith(suffix):
            s = s[: -len(suffix)].strip()
    if s.upper().startswith("NULLABLE(") and s.endswith(")"):
        s = s[len("NULLABLE(") : -1].strip()
    return s


def _split_top_level(s: str) -> list[str]:
    parts, depth, cur = [], 0, []
    for ch in s:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        if ch == "," and depth == 0:
            parts.append("".join(cur).strip())
            cur = []
        else:
            cur.append(ch)
    if cur:
        parts.append("".join(cur).strip())
    return [p for p in parts if p]


def parse_type(raw: str) -> ParsedType:
    s = _strip_nullable(raw)
    if "(" not in s:
        return ParsedType(s.upper())
    head, _, rest = s.partition("(")
    if not rest.endswith(")"):
        return ParsedType(s.upper())
    return ParsedType(head.strip().upper(), _split_top_level(rest[:-1]))


def _split_named_field(arg: str) -> tuple[str | None, str]:
    """`a INT32 NULL` -> ('a', 'INT32 NULL'); `INT32` -> (None, 'INT32')."""
    m = re.match(r'^("(?:[^"]|"")*"|`[^`]*`|[A-Za-z_][A-Za-z0-9_]*)\s+(.+)$', arg.strip())
    if not m:
        return None, arg
    name, typ = m.group(1), m.group(2)
    if name.upper() in _SCALAR or name.upper() in {"ARRAY", "MAP", "TUPLE", "DECIMAL", "VECTOR", "NULLABLE"}:
        return None, arg
    return name.strip('"`'), typ


def to_om_column(name: str, raw_type: str, ordinal: int, comment: str | None = None) -> dict:
    col = _map(parse_type(raw_type), raw_type)
    col["name"] = name
    col["ordinalPosition"] = ordinal
    if comment:
        col["description"] = comment
    return col


def _map(parsed: ParsedType, display: str) -> dict:
    n = parsed.name
    out: dict = {"dataTypeDisplay": display.strip()}

    if n in _SCALAR:
        out["dataType"] = _SCALAR[n]
        return out

    if _DECIMAL.match(n):
        out["dataType"] = "DECIMAL"
        if len(parsed.args) >= 1 and parsed.args[0].isdigit():
            out["precision"] = int(parsed.args[0])
        if len(parsed.args) >= 2 and parsed.args[1].isdigit():
            out["scale"] = int(parsed.args[1])
        return out

    if n == "ARRAY":
        inner = _map(parse_type(parsed.args[0]), parsed.args[0]) if parsed.args else {"dataType": "UNKNOWN"}
        out["dataType"] = "ARRAY"
        out["arrayDataType"] = inner["dataType"]
        if inner.get("children"):
            out["children"] = inner["children"]
        return out

    if n == "VECTOR":
        out["dataType"] = "ARRAY"
        out["arrayDataType"] = "FLOAT"
        return out

    if n == "MAP":
        out["dataType"] = "MAP"
        return out

    if n == "TUPLE":
        out["dataType"] = "STRUCT"
        children = []
        for idx, arg in enumerate(parsed.args, start=1):
            fname, ftype = _split_named_field(arg)
            child = _map(parse_type(ftype), ftype)
            child["name"] = fname if fname is not None else str(idx)
            children.append(child)
        if children:
            out["children"] = children
        return out

    out["dataType"] = "UNKNOWN"
    return out
