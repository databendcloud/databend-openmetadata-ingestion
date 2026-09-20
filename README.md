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
* `STAGE` endpoints are skipped (OM has no stage entity).
* Incremental runs use a watermark on `updated_on` (with `lookback_seconds` overlap). Because OM's
  `PUT /lineage` overwrites `lineageDetails`, each touched `(source_key, target_key)` edge is
  rebuilt from **all** its rows in Databend, so older column mappings survive.
* Edge removals in Databend (`CREATE OR REPLACE VIEW`, `REFRESH LINEAGE`, drops) are not visible in
  `lineage_history`. Two mitigations are built in:
  * `metadata.mark_deleted` soft-deletes tables that disappeared, which hides their edges.
  * `lineage.prune_view_upstreams` — for each view whose `CREATE_VIEW` edges appear in the batch,
    OM upstream edges not reported by Databend are deleted (only `ViewLineage`/`QueryLineage`
    edges; `Manual` edges are kept).
  Stale `DML` edges between two live tables remain until Databend's `lineage_retention` ages
  them out on the Databend side — they are not pruned here.

## Requirements

* Python ≥ 3.10; `pip install -e .`
* Databend started with `--lineage-on=true` and history tables enabled (`system_history.lineage_history` exists).
* An OM bot JWT with permission to create services/databases/tables and edit lineage (the default
  `ingestion-bot` token works).
* Databend session timezone must be UTC (the default); the watermark is compared as a naive
  timestamp literal.

## Usage

```bash
cp config.example.yaml config.yaml          # edit, or export DATABEND_PASSWORD / OM_JWT_TOKEN
databend-om-sync -c config.yaml init-service
databend-om-sync -c config.yaml metadata     # full sync, idempotent
databend-om-sync -c config.yaml lineage      # incremental; --full ignores the watermark
databend-om-sync -c config.yaml all          # the three above
```

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
