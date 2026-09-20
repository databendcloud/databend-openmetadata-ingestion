"""OpenMetadata fully-qualified-name construction.

Mirrors `org.openmetadata.schema.utils.FullyQualifiedName`: a part containing a dot is wrapped in
double quotes, embedded quotes are escaped.
"""

from __future__ import annotations


def quote(part: str) -> str:
    if '"' in part:
        part = part.replace('"', '\\"')
        return f'"{part}"'
    if "." in part:
        return f'"{part}"'
    return part


def build(*parts: str) -> str:
    return ".".join(quote(p) for p in parts)


def table_fqn(service: str, catalog: str, database: str, table: str) -> str:
    return build(service, catalog, database, table)


def column_fqn(table: str, column: str) -> str:
    return f"{table}.{quote(column)}"
