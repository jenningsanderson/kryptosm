## Design Tenets

<div class="two-col">
<div markdown="1">

#### Everything is SQL

Business logic lives in SQL strings that build temp views. No pandas, no Python UDFs. A reader who knows SQL can audit the full pipeline without knowing Spark internals. Functions are transparent — their docstrings list input and output columns.

#### No CLI

This is a library. The caller owns the Spark session — a cloud deployment (EMR, Glue, Databricks) provides its own. The E2E tests *are* the sample scripts; a production Glue cron job looks nearly identical.

#### Atomic, one-at-a-time updates

`next_osc_path` returns the next pending file. `apply_osc` applies exactly one. Per-table `last-applied-osc-sequence` stamps mean a mid-batch crash resumes cleanly — no re-applying what's already done, no partial state.

#### Per-type tables, not a unified partition

`nodes`, `ways`, and `relations` have different shapes, row counts, and join patterns. Separate tables enable separate Iceberg tuning: bloom filter budgets, sort orders, and distribution modes that fit each type's actual access pattern.

</div>
<div markdown="1">

#### Only rebuild what changed

Reverse-index tables make dirty-set computation O(dirty features). Changed features are MERGEd directly — no full partition rewrites, no table scans.

#### Views, not materializations

Each step registers a `createOrReplaceTempView`. Spark plans the whole DAG and materializes only at write/MERGE time. Between types (nodes → ways → relations), the pipeline re-binds from Iceberg so downstream phases read materialized data rather than re-executing upstream views.

#### No `COUNT()` / `COLLECT()` for progress

Those force eager evaluation and ruin scaling. Every progress signal comes from table properties and logging — zero extra Spark jobs.

#### Delete what's unused

No backwards-compatibility shims. No deprecation layers. If something is wrong, fix it.

</div>
</div>

Note:
The per-type table split was the single biggest architectural change from v0.4 to v0.5. A unified table with `type` as partition key forced bloom filter compromises that hurt every type. Separate tables pay a small operational cost (more tables to manage) but pay back in query performance and tuning flexibility.
