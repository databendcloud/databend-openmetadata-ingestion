from __future__ import annotations

import logging
from typing import Any, Iterator
from urllib.parse import quote

import requests

logger = logging.getLogger(__name__)


class OMError(RuntimeError):
    def __init__(self, status: int, message: str):
        super().__init__(f"HTTP {status}: {message}")
        self.status = status


class OpenMetadataClient:
    def __init__(self, host: str, jwt_token: str, timeout: int = 30):
        self._base = host.rstrip("/")
        self._timeout = timeout
        self._s = requests.Session()
        # OM is normally reachable directly; do not let HTTP(S)_PROXY from the shell hijack calls.
        self._s.trust_env = False
        self._s.headers.update(
            {"Authorization": f"Bearer {jwt_token}", "Content-Type": "application/json"}
        )

    # ---- low level -------------------------------------------------------------------------

    def _req(self, method: str, path: str, *, json: Any = None, params: dict | None = None) -> Any:
        url = f"{self._base}{path}"
        resp = self._s.request(method, url, json=json, params=params, timeout=self._timeout)
        if resp.status_code >= 400:
            raise OMError(resp.status_code, resp.text[:2000])
        if not resp.content:
            return None
        try:
            return resp.json()
        except ValueError:
            return resp.text

    @staticmethod
    def _path_fqn(fqn: str) -> str:
        return quote(fqn, safe="")

    def _paged(self, path: str, params: dict) -> Iterator[dict]:
        params = dict(params, limit=params.get("limit", 100))
        after: str | None = None
        while True:
            if after:
                params["after"] = after
            page = self._req("GET", path, params=params)
            yield from page.get("data", [])
            after = (page.get("paging") or {}).get("after")
            if not after:
                return

    # ---- admin / bot bootstrap ---------------------------------------------------------------

    @classmethod
    def login_basic(cls, host: str, email: str, password: str, timeout: int = 30) -> str:
        """Basic-auth login (OM's built-in auth provider); returns a short-lived access token."""
        import base64

        resp = requests.post(
            f"{host.rstrip('/')}/v1/users/login",
            json={"email": email, "password": base64.b64encode(password.encode()).decode()},
            timeout=timeout,
        )
        if resp.status_code >= 400:
            raise OMError(resp.status_code, resp.text[:2000])
        return resp.json()["accessToken"]

    def upsert_policy(self, name: str, description: str, rules: list[dict]) -> dict:
        return self._req(
            "PUT", "/v1/policies", json={"name": name, "description": description, "rules": rules}
        )

    def upsert_role(self, name: str, description: str, policy_names: list[str]) -> dict:
        return self._req(
            "PUT", "/v1/roles", json={"name": name, "description": description, "policies": policy_names}
        )

    def upsert_bot_user(self, name: str, email: str, role_ids: list[str], token_expiry: str) -> dict:
        return self._req(
            "PUT",
            "/v1/users",
            json={
                "name": name,
                "email": email,
                "isBot": True,
                "botName": name,
                "roles": role_ids,
                "authenticationMechanism": {"authType": "JWT", "config": {"JWTTokenExpiry": token_expiry}},
            },
        )

    def upsert_bot(self, name: str, bot_user_name: str, description: str) -> dict:
        return self._req(
            "PUT", "/v1/bots", json={"name": name, "botUser": bot_user_name, "description": description}
        )

    def generate_bot_token(self, bot_user_id: str, expiry: str) -> str:
        """expiry: OneHour | 1 | 7 | 30 | 60 | 90 | Unlimited (days)."""
        mech = self._req("PUT", f"/v1/users/generateToken/{bot_user_id}", json={"JWTTokenExpiry": expiry})
        return mech["JWTToken"]

    # ---- service ---------------------------------------------------------------------------

    def upsert_custom_database_service(
        self, name: str, display_name: str, description: str, icon_url: str
    ) -> dict:
        body: dict[str, Any] = {
            "name": name,
            "displayName": display_name,
            "description": description,
            "serviceType": "CustomDatabase",
            "connection": {
                "config": {
                    "type": "CustomDatabase",
                    "sourcePythonClass": "databend_openmetadata_ingestion.noop.NoopSource",
                    "connectionOptions": {"engine": "databend", "syncedBy": "databend-openmetadata-ingestion"},
                }
            },
        }
        if icon_url:
            body["style"] = {"iconURL": icon_url}
        return self._req("PUT", "/v1/services/databaseServices", json=body)

    # ---- entities --------------------------------------------------------------------------

    def upsert_database(self, service: str, name: str, description: str = "") -> dict:
        body = {"name": name, "service": service}
        if description:
            body["description"] = description
        return self._req("PUT", "/v1/databases", json=body)

    def upsert_schema(self, database_fqn: str, name: str, description: str = "") -> dict:
        body = {"name": name, "database": database_fqn}
        if description:
            body["description"] = description
        return self._req("PUT", "/v1/databaseSchemas", json=body)

    def upsert_table(self, schema_fqn: str, table: dict) -> dict:
        return self._req("PUT", "/v1/tables", json={**table, "databaseSchema": schema_fqn})

    def list_databases(self, service: str) -> Iterator[dict]:
        return self._paged("/v1/databases", {"service": service, "fields": "", "include": "non-deleted"})

    def list_schemas(self, database_fqn: str) -> Iterator[dict]:
        return self._paged(
            "/v1/databaseSchemas", {"database": database_fqn, "fields": "", "include": "non-deleted"}
        )

    def list_tables(self, schema_fqn: str) -> Iterator[dict]:
        return self._paged(
            "/v1/tables", {"databaseSchema": schema_fqn, "fields": "", "include": "non-deleted"}
        )

    def soft_delete(self, entity: str, entity_id: str, recursive: bool = True) -> None:
        self._req(
            "DELETE",
            f"/v1/{entity}/{entity_id}",
            params={"hardDelete": "false", "recursive": str(recursive).lower()},
        )

    # ---- lineage ---------------------------------------------------------------------------

    def put_table_lineage(self, from_fqn: str, to_fqn: str, details: dict) -> bool:
        """Returns False when either endpoint does not exist in OM (404) — caller skips the edge."""
        path = f"/v1/lineage/table/name/{self._path_fqn(from_fqn)}/table/name/{self._path_fqn(to_fqn)}"
        try:
            self._req("PUT", path, json=details)
            return True
        except OMError as exc:
            if exc.status == 404:
                return False
            raise

    def delete_table_lineage(self, from_fqn: str, to_fqn: str) -> None:
        path = f"/v1/lineage/table/name/{self._path_fqn(from_fqn)}/table/name/{self._path_fqn(to_fqn)}"
        try:
            self._req("DELETE", path)
        except OMError as exc:
            if exc.status != 404:
                raise

    def upstream_tables(self, table_fqn: str) -> dict[str, str | None] | None:
        """Direct upstream table FQN -> lineageDetails.source, or None when the table is missing in OM."""
        try:
            graph = self._req(
                "GET",
                f"/v1/lineage/table/name/{self._path_fqn(table_fqn)}",
                params={"upstreamDepth": 1, "downstreamDepth": 0},
            )
        except OMError as exc:
            if exc.status == 404:
                return None
            raise
        entity = graph.get("entity") or {}
        nodes = {n["id"]: n for n in graph.get("nodes", [])}
        nodes[entity.get("id")] = entity
        result: dict[str, str | None] = {}
        for edge in graph.get("upstreamEdges", []):
            if edge.get("toEntity") != entity.get("id"):
                continue
            node = nodes.get(edge.get("fromEntity"))
            if node and node.get("type") == "table" and node.get("fullyQualifiedName"):
                result[node["fullyQualifiedName"]] = (edge.get("lineageDetails") or {}).get("source")
        return result
