from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

_ENV_REF = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")


def _expand_env(value: Any) -> Any:
    if isinstance(value, str):
        return _ENV_REF.sub(lambda m: os.environ.get(m.group(1), ""), value)
    if isinstance(value, dict):
        return {k: _expand_env(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_expand_env(v) for v in value]
    return value


@dataclass
class DatabendConfig:
    dsn: str


@dataclass
class OpenMetadataConfig:
    host: str
    jwt_token: str
    timeout_seconds: int = 30


@dataclass
class ServiceConfig:
    name: str
    display_name: str = "Databend"
    description: str = ""
    icon_url: str = ""


@dataclass
class MetadataConfig:
    catalogs: list[str] = field(default_factory=list)
    exclude_databases: list[str] = field(
        default_factory=lambda: ["system", "information_schema", "system_history"]
    )
    mark_deleted: bool = True


@dataclass
class LineageConfig:
    state_file: str = ".state/lineage_watermark.json"
    include_kinds: list[str] = field(default_factory=lambda: ["CTAS", "DML", "CREATE_VIEW"])
    lookback_seconds: int = 120
    prune_view_upstreams: bool = True
    page_size: int = 5000


@dataclass
class Config:
    databend: DatabendConfig
    openmetadata: OpenMetadataConfig
    service: ServiceConfig
    metadata: MetadataConfig
    lineage: LineageConfig

    @classmethod
    def load(cls, path: str | Path) -> "Config":
        raw = _expand_env(yaml.safe_load(Path(path).read_text()) or {})
        return cls(
            databend=DatabendConfig(**raw["databend"]),
            openmetadata=OpenMetadataConfig(**raw["openmetadata"]),
            service=ServiceConfig(**raw["service"]),
            metadata=MetadataConfig(**raw.get("metadata", {})),
            lineage=LineageConfig(**raw.get("lineage", {})),
        )
