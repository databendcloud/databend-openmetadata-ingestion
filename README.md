# databend-om-sync

Interim tool that pushes Databend metadata **and** lineage into OpenMetadata over the REST API,
for use until the native connector (open-metadata/OpenMetadata#33387) is released. No changes to
the OM server or ingestion image are required.

## What it does

| Databend | OpenMetadata |
|---|---|
| service | `DatabaseService` of type `CustomDatabase` (`displayName` = Databend) |
| catalog (`SHOW CATALOGS`) | `Database` |
| database (`system.databases`) | `DatabaseSchema` |
| table / view (`system.tables`, `system.views`) | `Table` (`tableType` Regular/View/External/Transient/Iceberg, `schemaDefinition` = view SQL) |
| column (`system.columns.data_type`) | `Column` (see `databend_om_sync/types.py` for the type map) |
| `system_history.lineage_history` row | table→table lineage edge with `columnsLineage`, `sqlQuery`, `source` = `ViewLineage` for `CREATE_VIEW`, else `QueryLineage` |

The FQN scheme (`service.catalog.database.table`) is identical to the upstream connector, so the
same lineage script keeps working once you switch to a real `Databend` service.

### Lineage semantics (eventual consistency)

* Endpoints are the catalog/database/name **snapshot** stored in `lineage_history`; IDs are not
  resolved. If OM returns 404 for an endpoint the edge is skipped and logged. Run `metadata` before
  `lineage`; renamed tables heal on their next DML.
* `STAGE` endpoints are skipped unless `stages.enabled` (see "Named stages").
* Incremental runs use a watermark on `updated_on` (with `lookback_seconds` overlap). Because OM's
  `PUT /lineage` overwrites `lineageDetails`, each touched `(source_key, target_key)` edge is
  rebuilt from **all** its rows in Databend, so older column mappings survive.
* Databend-side facts verified against v1.2.947 that shape the cleanup logic:
  * `DROP TABLE` only emits `DELETE_OBJECT` for transient tables (regular tables are undroppable
    until vacuum), so the dropped table's edges stay in `lineage_history`. Here
    `metadata.mark_deleted` soft-deletes the table and the edge is skipped (404) afterwards.
  * `CREATE OR REPLACE VIEW` does **not** emit `DELETE_EDGE` (only `REFRESH LINEAGE` does), so the
    superseded definition's edges stay in `lineage_history`. The aggregator keeps only the
    `CREATE_VIEW` rows of the newest statement (`query_info.query_id`) per view, and
    `lineage.prune_view_upstreams` deletes OM upstream edges of those views that Databend no longer
    reports (only `ViewLineage`/`QueryLineage` edges; `Manual` edges are kept).
  * Stale `DML` edges between two live tables remain until Databend's `lineage_retention` ages
    them out — they are not pruned here.
* Known cosmetic issue: `system.columns.data_type` is rendered by `TableDataType::sql_name()`, which
  upper-cases nested TUPLE field names (`TUPLE(AGE INT32, CITY STRING)` for `tuple(age, city)`), so
  STRUCT children in OM are upper-cased. `SHOW CREATE TABLE` keeps the case; fixing `sql_name()` in
  Databend is the right place.

### Named stages (optional)

Databend stages are tenant-level (no catalog/database), while OM only has `Table` with
`tableType=Stage` living under a schema (that is how OM's Snowflake connector models stages). With
`stages.enabled: true` every non-internal stage from `system.stages` is mounted as
`<service>.<stages.database>.<stages.schema>.<stage>` (description = type + URL + comment, no
columns) and STAGE endpoints in `lineage_history` are mapped to that FQN, so `COPY INTO @stage FROM t`,
`COPY INTO t FROM @stage` and `CREATE TABLE t AS SELECT FROM @stage` all show up. Databend records
no column lineage for stage edges. External stages could alternatively be mapped to an OM
Storage Container by URL (as Snowflake does for `COPY_HISTORY`), which is out of scope here.

## Requirements

* Python ≥ 3.10; `pip install -e .`
* Databend started with `--lineage-on=true` and history tables enabled (`system_history.lineage_history` exists).
* An OM bot JWT. Use `init-bot` (below) to create a least-privilege bot; the built-in
  `ingestion-bot` token also works but is over-privileged.
* Databend session timezone must be UTC (the default); the watermark is compared as a naive
  timestamp literal.

## Usage

```bash
cp config.example.yaml config.yaml          # edit, or export DATABEND_PASSWORD / OM_JWT_TOKEN

# once, by an OM admin: bot + policy + role, prints the JWT (or --write-token PATH, mode 600)
OM_ADMIN_EMAIL=admin@example.com OM_ADMIN_PASSWORD=... databend-om-sync -c config.yaml init-bot
#   (with SSO instead of basic auth: OM_ADMIN_TOKEN=<any admin JWT> databend-om-sync init-bot)
export OM_JWT_TOKEN=...                     # the bot token from the step above

databend-om-sync -c config.yaml init-service
databend-om-sync -c config.yaml metadata     # full sync, idempotent
databend-om-sync -c config.yaml lineage      # incremental; --full ignores the watermark
databend-om-sync -c config.yaml all          # the three above
```

The bot's policy allows `Create/ViewAll/EditAll/Delete` on `databaseService`, `database`,
`databaseSchema`, `table` only, and denies `EditDisplayName`. Re-running `init-bot` rotates the
token (the previous one stops working). Token lifetime: `bot.token_expiry` (default 90 days).

Schedule `metadata` (e.g. hourly) and `lineage` (e.g. every 5–15 min) with cron/Airflow. Run
`lineage --full` once after the first `metadata` sync and after any long outage.

## Switching to the native connector

Service `serviceType` is immutable, so when the Databend connector ships: create the new
`Databend` service in OM, set `service.name` to it, delete the watermark file, run
`metadata` (or the OM ingestion pipeline) and `lineage --full`, then delete the old
`CustomDatabase` service.

## Development

```bash
pip install -e ".[dev]" && pytest
```

### Local E2E stack

`docker/` contains an untouched copy of the official OM compose plus an override that renames
containers/ports (coexists with an OM dev checkout, drops the Airflow ingestion container) and adds
a single-node Databend with `[lineage] on = true` and history tables enabled.

```bash
cd docker
echo 'QUERY_DATABEND_ENTERPRISE_LICENSE=<ee-license>' > .env   # lineage is an EE feature; .env is git-ignored
docker compose -p bendom -f om-official.yml -f override.yml up -d
bendsql --dsn 'databend://root:@localhost:8000/?sslmode=disable' < fixture.sql
# OM: http://localhost:8585 (admin/admin)
OM_ADMIN_EMAIL=admin@open-metadata.org OM_ADMIN_PASSWORD=admin \
  databend-om-sync -c ../config.yaml init-bot --write-token ../.state/bot_token
export OM_JWT_TOKEN=$(cat ../.state/bot_token)
databend-om-sync -c ../config.yaml all
```

In Databend's own CI the license comes from the `DATABEND_ENTERPRISE_LICENSE_*` GitHub secrets
(`.github/workflows/release.yml`, `cloud.yml`); release builds embed it at build time.
