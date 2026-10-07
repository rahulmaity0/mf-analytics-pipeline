# Design notes

Why the pipeline is built the way it is, and what each choice costs.

## 1. Keep raw files unchanged (bronze)

AMFI's files are stored byte-for-byte as downloaded, under
`bronze/amfi_nav/period=YYYY-MM/amfi_nav_<start>_<end>.txt`.

- **Replayable.** When the parser had to change (for example, splitting zero NAVs out of
  "non-positive"), silver was rebuilt from bronze in seconds. Re-downloading 20 years
  from a free public endpoint would take hours and be impolite.
- **Idempotent.** The object key depends only on the requested window, so re-running a
  download overwrites the same object instead of creating a duplicate.
- **Month-aligned windows.** Downloads never cross a month boundary, so each file belongs
  to exactly one `period=` folder, which is also the unit the silver job rebuilds.

## 2. Parse whole files, not lines

Category and fund-house names are not columns in AMFI's format; they are header lines, and
every row belongs to the last header above it. Splitting a file at arbitrary byte offsets
(what `spark.read.text` does) would give some tasks rows with no header context.

So the silver job uses `wholeTextFiles` (one task per file) and a sequential parser. The
cost: parallelism is limited to the number of files, and each file must fit in a task's
memory. At ~25 MB per monthly file, that is a non-issue, and a 20-year backfill still has
240 files to spread across cores.

## 3. Validate before writing, and fail loudly

Typing happens in Spark with `try_cast` and a CASE expression of quarantine rules, so a bad
value becomes a tagged row rather than a crashed job. Then, in order:

1. Quarantined rows are written to `quarantine/` with their reason (for inspection).
2. The unexpected-quarantine ratio is checked; above 2%, the job raises **before** writing silver.
3. Only then is silver written, with dynamic partition overwrite (only the rebuilt months change).

*Expected* quarantines (zero NAVs from segregated portfolios) are still quarantined but don't
count toward the limit. Without that split, the threshold would either be too loose to catch
real breakage or would fail healthy runs as more side pockets appear.

## 4. De-duplication: newest window wins

The daily run deliberately re-fetches 5 days, so the same (scheme, date) arrives several times.
Silver keeps the row from the window with the latest end date. If AMFI corrected a NAV, the
correction wins. The job also counts *conflicting* duplicates (same key, different NAV), so a
sudden rise in corrections is visible in the metrics, not hidden by the dedup.

## 5. Spark for volume, pandas for path-dependent maths

Rolling volatility could be written as a Spark window with `rangeBetween`, but max drawdown
(fall from the running peak *within* the window) and "NAV on or before exactly one year ago,
but only if within 7 days" are clumsy in SQL and easy to get subtly wrong.

`groupBy("scheme_code").applyInPandas(...)` sends each scheme's full history (at most about
5,000 rows) to one task, where the maths is plain NumPy with `searchsorted` lookups, and
unit-testable without Spark. The trade-off is Arrow serialisation per group, which is cheap
at this group size. It would not suit a single group of millions of rows.

## 6. Daily data in the lake, monthly data in Postgres

The full daily history stays in Parquet. Postgres gets month-end metrics (~200k rows for two
years, ~2M for twenty). That keeps the warehouse small enough for interactive dbt builds and
API queries, while the lake remains the source of truth for anything finer-grained.

## 7. Scheme registry in MongoDB

A scheme is naturally a document: a current view plus an ordered list of every name,
category and fund house it has carried. In SQL that would be a slowly changing dimension
table plus a join for every lookup. The API's `/funds/{code}` endpoint returns the document
as-is, and a flattened "current" view is exported to Postgres for dbt.

## 8. Versioned cache instead of invalidation

Cache keys embed a version number (`mf:v{N}:...`). After each dbt build, the pipeline
increments `N`. Old entries are simply never read again and expire by TTL. There is no "find
every key affected by this load" logic to get wrong.

## Scaling notes: if this ran on pandas alone

A common failure mode with data this size is a pandas job that runs out of memory. If the 20 GB
of raw text had to be processed without Spark, these are the levers, roughly in order of payoff:

1. **Find what's using the memory first.** `df.memory_usage(deep=True)` usually shows that
   object (string) columns dominate. A repeated string like a fund-house name is stored once
   per row as a Python object.
2. **Read less.** `usecols=` to skip columns that aren't needed; filter rows while reading
   (`pyarrow.dataset` with a filter, or chunked reading) instead of loading and then filtering.
3. **Smaller dtypes.** `category` for low-cardinality strings (fund house, category, plan),
   `int32` for scheme codes, `float32` where the precision is enough. That is often a 5–10×
   reduction.
4. **Don't hold everything at once.** `read_csv(chunksize=...)` or a per-file loop, writing
   each chunk's result to Parquet, then aggregating the much smaller intermediate output.
   Partitioning by the grouping key (here, scheme or month) makes each piece independent.
5. **Columnar storage.** Convert the text to Parquet once; later reads load only the needed
   columns and skip row groups using statistics.
6. **Then change tools.** Polars or DuckDB (out-of-core, multi-threaded) on one machine, or
   Spark/Dask when it has to scale out, which is the route this project takes.
