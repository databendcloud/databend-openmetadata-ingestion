# databend-om-sync

Pushes Databend metadata **and** lineage into OpenMetadata over the REST API. Works with any
OpenMetadata 1.x/2.x server as-is: no server plugin, no custom ingestion image.

## What it does

| Databend | OpenMetadata |
|---|---|
| service | `DatabaseService` of type `CustomDatabase` (`displayName` = Databend) |
| catalog (`SHOW CATALOGS`) | `Database` |
| database (`system.databases`) | `DatabaseSchema` |
| table / view (`system.tables`, `system.views`) | `Table` (`tableType` Regular/View/External/Transient/Iceberg, `schemaDefinition` = view SQL) |
| column (`system.columns.data_type`) | `Column` (type map in `databend_om_sync/types.py`) |
| named stage (`system.stages`, optional) | `Table` with `tableType=Stage` under a configurable schema |
| `system_history.lineage_history` row | table→table lineage edge with `columnsLineage`, `sqlQuery`, `source` = `ViewLineage` for `CREATE_VIEW`, else `QueryLineage` |

FQNs are `service.catalog.database.table`.

## Requirements

* Python ≥ 3.10; `pip install -e .`
* Databend with lineage enabled (`[lineage] on = true` / `--lineage-on=true`) and history tables
  enabled, so that `system_history.lineage_history` exists. Lineage is an Enterprise feature; the
  Databend server needs a valid license — this tool does not.
* An OpenMetadata bot JWT. `init-bot` (below) creates a least-privilege bot; the built-in
  `ingestion-bot` token also works but is over-privileged.
* The Databend user needs `SELECT` on `system.*` and `system_history.lineage_history`.
* Databend session timezone must be UTC (the default); the watermark is compared as a naive
  timestamp literal.

## Usage

```bash
cp config.example.yaml config.yaml          # edit; ${ENV_VAR} placeholders are expanded

# once, by an OM admin: creates bot + policy + role and prints the bot JWT
OM_ADMIN_EMAIL=admin@example.com OM_ADMIN_PASSWORD=... databend-om-sync -c config.yaml init-bot
#   with SSO (no basic-auth login):  OM_ADMIN_TOKEN=<any admin JWT> databend-om-sync init-bot
#   write to a file instead of stdout: --write-token /path/to/token   (mode 600)
export OM_JWT_TOKEN=...                     # the bot token

databend-om-sync -c config.yaml init-service   # create/update the service (idempotent)
databend-om-sync -c config.yaml metadata       # full sync of catalogs/databases/tables/columns
databend-om-sync -c config.yaml lineage        # incremental; --full ignores the watermark
databend-om-sync -c config.yaml all            # the three above
```

Schedule `metadata` (e.g. hourly) and `lineage` (e.g. every 5–15 min) with cron/Airflow. Run
`lineage --full` once after the first `metadata` sync and after any long outage.

The bot's policy allows `Create/ViewAll/EditAll/Delete` on `databaseService`, `database`,
`databaseSchema`, `table` only, and denies `EditDisplayName`. Re-running `init-bot` rotates the
token (the previous one stops working). Token lifetime: `bot.token_expiry` (default 90 days).

## Configuration

See `config.example.yaml` for every option. The ones that matter operationally:

| Key | Meaning |
|---|---|
| `service.name` | OM service name; part of every FQN. Changing it means a fresh sync into a new service. |
| `metadata.catalogs` | Catalogs to sync (empty = all from `SHOW CATALOGS`). |
| `metadata.mark_deleted` | Soft-delete OM schemas/tables that disappeared from Databend (default `true`). |
| `lineage.state_file` | **Watermark file** — the only state this tool keeps. See below. |
| `lineage.lookback_seconds` | Overlap re-read before the watermark to absorb late `lineage_history` merges. |
| `lineage.prune_view_upstreams` | Remove OM upstream edges of a redefined view that Databend no longer reports. |
| `stages.enabled` | Also sync named stages and stage↔table lineage. |

### The watermark file (`lineage.state_file`)

Incremental `lineage` runs read rows with `updated_on > watermark - lookback_seconds` and, after a
successful run, store the batch's `max(updated_on)` in this JSON file. It is the *Databend* data
timestamp, not wall-clock time, so machine clocks do not matter.

* It must be on **persistent storage** and the scheduler must always run from the same path
  (or point `state_file` at a shared location). Losing it just means the next run is a full replay.
* Only one `lineage` process should use a given file at a time.
* Delete it (or run `lineage --full`) to replay everything, e.g. after re-creating the service.
* `metadata` and `init-*` never touch it.

### Lineage semantics

* Edges are written by name as recorded in `lineage_history`; if either endpoint does not exist in
  OM (404) the edge is skipped and logged. Run `metadata` before `lineage`.
* OM's `PUT /lineage` replaces an edge's details, so every edge touched in a run is rebuilt from
  **all** of its rows in Databend — older column mappings are preserved.
* One OM edge per table pair: `CREATE_VIEW` sets `source=ViewLineage`, otherwise `QueryLineage`;
  column mappings are unioned; `sqlQuery` is the most recent statement.
* A view's upstream edges come from its newest `CREATE VIEW` statement only; with
  `prune_view_upstreams` the tool also deletes OM upstream edges of that view which Databend no
  longer reports (only `ViewLineage`/`QueryLineage` edges — manually drawn edges are kept).
* Tables dropped in Databend are soft-deleted by `metadata` (`mark_deleted`), which hides their
  lineage in OM. Edges between two tables that both still exist are never deleted by this tool.

### Named stages (optional)

Databend stages are tenant-level (no catalog/database), while OM only has `Table` with
`tableType=Stage` living under a schema (the same model OM uses for Snowflake stages). With
`stages.enabled: true` every non-internal stage from `system.stages` is created as
`<service>.<stages.database>.<stages.schema>.<stage>` (description = type + URL + comment, no
columns), and stage endpoints in `lineage_history` map to that FQN, so `COPY INTO @stage FROM t`,
`COPY INTO t FROM @stage` and `CREATE TABLE t AS SELECT ... FROM @stage` all appear. Databend records
no column lineage for stage edges. `stages.schema` must not clash with a real database in
`stages.database`.

## Notes

* Nested `TUPLE` field names appear upper-cased in OM (`STRUCT` children `AGE`, `CITY` for
  `tuple(age int32, city string)`) because that is how `system.columns.data_type` renders them.
* `style.iconURL` on the service is honoured only by OM versions whose UI reads it; on others the
  default CustomDatabase icon is shown.

## Development

```bash
pip install -e ".[dev]" && pytest
```

### Local E2E stack

`docker/` contains an untouched copy of the official OM compose plus an override that renames
containers/ports (so it coexists with another OM checkout, and drops the Airflow ingestion
container) and adds a single-node Databend with lineage and history tables enabled.

```bash
cd docker
echo 'QUERY_DATABEND_ENTERPRISE_LICENSE=<ee-license>' > .env   # .env is git-ignored
docker compose -p bendom -f om-official.yml -f override.yml up -d
bendsql --dsn 'databend://root:@localhost:8000/?sslmode=disable' < fixture.sql
# OM: http://localhost:8585 (admin/admin)
OM_ADMIN_EMAIL=admin@open-metadata.org OM_ADMIN_PASSWORD=admin \
  databend-om-sync -c ../config.yaml init-bot --write-token ../.state/bot_token
export OM_JWT_TOKEN=$(cat ../.state/bot_token)
databend-om-sync -c ../config.yaml all
```
