from __future__ import annotations

import argparse
import logging
import sys

from .config import Config
from .databend_client import DatabendClient
from .lineage_sync import LineageSync
from .metadata_sync import MetadataSync
from .om_client import OpenMetadataClient


def _clients(cfg: Config) -> tuple[DatabendClient, OpenMetadataClient]:
    return (
        DatabendClient(cfg.databend.dsn),
        OpenMetadataClient(cfg.openmetadata.host, cfg.openmetadata.jwt_token, cfg.openmetadata.timeout_seconds),
    )


def cmd_init_service(cfg: Config) -> int:
    _, om = _clients(cfg)
    svc = om.upsert_custom_database_service(
        cfg.service.name, cfg.service.display_name, cfg.service.description, cfg.service.icon_url
    )
    print(f"service {svc['fullyQualifiedName']} ({svc['serviceType']}) id={svc['id']}")
    return 0


def cmd_metadata(cfg: Config) -> int:
    db, om = _clients(cfg)
    stats = MetadataSync(cfg, db, om).run()
    print(
        f"metadata: databases={stats.databases} schemas={stats.schemas} tables={stats.tables} "
        f"failed={stats.failed} deleted={stats.deleted}"
    )
    for err in stats.errors[:20]:
        print(f"  ! {err}")
    return 1 if stats.failed and not stats.tables else 0


def cmd_lineage(cfg: Config, full: bool) -> int:
    db, om = _clients(cfg)
    stats = LineageSync(cfg, db, om).run(full=full)
    print(
        f"lineage: rows={stats.rows} edges={stats.edges} written={stats.written} "
        f"skipped_missing={stats.skipped_missing} pruned={stats.pruned} watermark={stats.watermark}"
    )
    return 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="databend-om-sync")
    p.add_argument("-c", "--config", default="config.yaml")
    p.add_argument("-v", "--verbose", action="store_true")
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("init-service", help="create/update the CustomDatabase service in OM")
    sub.add_parser("metadata", help="full sync of catalogs/databases/tables/columns")
    lin = sub.add_parser("lineage", help="incremental sync of lineage_history (use --full to ignore the watermark)")
    lin.add_argument("--full", action="store_true")
    both = sub.add_parser("all", help="init-service + metadata + lineage")
    both.add_argument("--full", action="store_true")

    args = p.parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    cfg = Config.load(args.config)

    if args.cmd == "init-service":
        return cmd_init_service(cfg)
    if args.cmd == "metadata":
        return cmd_metadata(cfg)
    if args.cmd == "lineage":
        return cmd_lineage(cfg, args.full)
    rc = cmd_init_service(cfg)
    rc |= cmd_metadata(cfg)
    rc |= cmd_lineage(cfg, args.full)
    return rc


if __name__ == "__main__":
    sys.exit(main())
