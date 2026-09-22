"""One-shot creation of a least-privilege OM bot for this tool.

Creates: policy -> role -> bot user (with the role) -> bot -> JWT. Idempotent (all PUTs); rerunning
regenerates the token, which invalidates the previous one.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from .om_client import OpenMetadataClient

logger = logging.getLogger(__name__)

# EditAll covers EditLineage/EditDescription/...; Delete is needed for mark_deleted soft deletes.
_RULES = [
    {
        "name": "databend-sync-allow-metadata",
        "description": "Create/update/soft-delete Databend services, databases, schemas, tables and their lineage.",
        "resources": ["databaseService", "database", "databaseSchema", "table"],
        "operations": ["Create", "ViewAll", "EditAll", "Delete"],
        "effect": "allow",
    },
    {
        "name": "databend-sync-deny-displayname",
        "description": "Bots must not rename entities.",
        "resources": ["All"],
        "operations": ["EditDisplayName"],
        "effect": "deny",
    },
]


@dataclass
class BotResult:
    bot_name: str
    bot_user_id: str
    role: str
    policy: str
    token: str


def bootstrap_bot(om_admin: OpenMetadataClient, name: str, token_expiry: str, email_domain: str) -> BotResult:
    policy = om_admin.upsert_policy(
        f"{name}-policy", "Least-privilege policy for databend-openmetadata-ingestion.", _RULES
    )
    role = om_admin.upsert_role(f"{name}-role", "Role for databend-openmetadata-ingestion bot.", [policy["name"]])
    user = om_admin.upsert_bot_user(name, f"{name}@{email_domain}", [role["id"]], token_expiry)
    om_admin.upsert_bot(name, user["name"], "Bot used by databend-openmetadata-ingestion to push Databend metadata and lineage.")
    token = om_admin.generate_bot_token(user["id"], token_expiry)
    return BotResult(bot_name=name, bot_user_id=user["id"], role=role["name"], policy=policy["name"], token=token)
