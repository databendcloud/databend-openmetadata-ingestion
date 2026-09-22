from __future__ import annotations

import argparse
import getpass
import logging
import os
import sys
from pathlib import Path

from .bot_bootstrap import bootstrap_bot
from .config import Config
from .databend_client import DatabendClient
from .lineage_sync import LineageSync
from .metadata_sync import MetadataSync
from .om_client import OpenMetadataClient


def _clients(cfg: Config) -> tuple[DatabendClient, OpenMetadataClient]:
    return (
        DatabendClient(cfg.databend.dsn, include_stages=cfg.stages.enabled),
        OpenMetadataClient(cfg.openmetadata.host, cfg.openmetadata.jwt_token, cfg.openmetadata.timeout_seconds),
    )


def cmd_init_bot(cfg: Config, args: argparse.Namespace) -> int:
    """Needs an admin credential: OM_ADMIN_TOKEN, or OM_ADMIN_EMAIL + OM_ADMIN_PASSWORD (basic auth)."""
    om_cfg = cfg.openmetadata
    admin_token = os.environ.get("OM_ADMIN_TOKEN")
    if not admin_token:
        email = os.environ.get("OM_ADMIN_EMAIL") or input("OM admin email: ")
        password = os.environ.get("OM_ADMIN_PASSWORD") or getpass.getpass("OM admin password: ")
        admin_token = OpenMetadataClient.login_basic(om_cfg.host, email, password, om_cfg.timeout_seconds)
    om = OpenMetadataClient(om_cfg.host, admin_token, om_cfg.timeout_seconds)

    res = bootstrap_bot(om, cfg.bot.name, cfg.bot.token_expiry, cfg.bot.email_domain)
    print(f"bot={res.bot_name} user_id={res.bot_user_id} role={res.role} policy={res.policy}")
    if args.write_token:
        path = Path(args.write_token)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(res.token)
        path.chmod(0o600)
        print(f"token written to {path} (mode 600); export OM_JWT_TOKEN=$(cat {path})")
    else:
        print("OM_JWT_TOKEN=" + res.token)
    return 0


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
    bot = sub.add_parser(
        "init-bot",
        help="(admin) create a least-privilege bot + policy + role and print/write its JWT",
    )
    bot.add_argument("--write-token", metavar="PATH", help="write the JWT to PATH instead of stdout")
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

    if args.cmd == "init-bot":
        return cmd_init_bot(cfg, args)
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
