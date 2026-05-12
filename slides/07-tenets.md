## Design Tenets

<div class="two-col">
<div markdown="1">

#### Everything is SQL

Business logic lives in SQL strings that build temp views. No pandas, no Python UDFs. Scan a function's SQL and understand the transformation.

#### No CLI

This is a library. The caller owns the Spark session — a cloud deployment (EMR, Glue, Databricks) provides its own.

#### Atomic, one-at-a-time updates

`next_osc_path` returns the next pending file. `apply_osc` applies exactly one. A crash mid-batch resumes cleanly.

</div>
<div markdown="1">

#### Only rebuild what changed

Reverse-index tables make dirty-set computation O(dirty features). Changed features are MERGEd directly — no full partition rewrites.

#### Views, not materializations

Each step registers a `createOrReplaceTempView`. Spark plans the whole DAG and materializes only at write/MERGE time.

#### Delete what's unused

No backwards-compatibility shims. No deprecation layers. If something is wrong, fix it.

</div>
</div>

Note:
The E2E tests are the sample scripts — a production cron job looks nearly identical.
