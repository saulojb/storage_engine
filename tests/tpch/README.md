TPC-H Benchmark
===============

This directory contains scripts to execute the [TPC-H] benchmark queries with
the same data in ClickHouse and Postgres, and generates a table comparing
PostgreSQL performance to pg_clickhouse performance.

The scripts run each query in [queries](queries) three times each for native
PostgreSQL and pg_clickhouse performance and produces a Markdown table
reporting the averaged times for each, as well as whether the pg_clickhouse
query pushed down to ClickHouse. Times exceeding 60s will not be recorded, and
result in a `-`. An example:

```md
|    Query   | PostgreSQL | pg_clickhouse | Pushdown |
| ----------:| ----------:| -------------:|:--------:|
|  [Query 1] |    4693 ms |        268 ms |     ✔︎    |
|  [Query 2] |     458 ms |       3446 ms |          |
|  [Query 3] |     742 ms |        111 ms |     ✔︎    |
|  [Query 4] |     270 ms |        130 ms |     ✔︎    |
|  [Query 5] |     337 ms |       1460 ms |     ✔︎    |
|  [Query 6] |     764 ms |         53 ms |     ✔︎    |
|  [Query 7] |     619 ms |         96 ms |     ✔︎    |
|  [Query 8] |     342 ms |        156 ms |     ✔︎    |
|  [Query 9] |    3094 ms |        298 ms |     ✔︎    |
| [Query 10] |     581 ms |        197 ms |     ✔︎    |
| [Query 11] |     212 ms |         24 ms |          |
| [Query 12] |    1116 ms |         84 ms |     ✔︎    |
| [Query 13] |     958 ms |       1368 ms |          |
| [Query 14] |     181 ms |         73 ms |     ✔︎    |
| [Query 15] |    1118 ms |        557 ms |          |
| [Query 16] |     497 ms |       1714 ms |          |
| [Query 17] |    1846 ms |      32709 ms |          |
| [Query 18] |    5823 ms |      10649 ms |          |
| [Query 19] |      53 ms |        206 ms |     ✔︎    |
| [Query 20] |     421 ms |             - |          |
| [Query 21] |    1349 ms |       4434 ms |          |
| [Query 22] |     258 ms |       1415 ms |          |
```

## Setup & Execution

To use it, set up ClickHouse and Postgres running locally and execute these
commands.

### ClickHouse

```sh
make ch
```

This command connects to ClickHouse and runs [tpch-ch.sql](tpch-ch.sql), which
creates a database named `tpch`, creates the [ClickHouse TPC-H] tables, and
loads them with scaling factor 1 data from ClickHouse S3 buckets. Export the
[ClickHouse client environment variables][chenv] `CLICKHOUSE_USER`,
`CLICKHOUSE_PASSWORD` and `CLICKHOUSE_HOST` as appropriate to configure the
ClickHouse server and user.

### PostgreSQL

```sh
make pg
```

This command connects to PostgreSQL and runs [tpch-pg.sql](tpch-pg.sql), which
creates a schema named `pg`, creates the [PostgreSQL TPC-H] tables, and loads
them with scaling factor 1 data from ClickHouse S3 buckets. It also loads
pg_clickhouse (which must already be installed in the cluster), creates a
schema named `ch`, and imports the the tables from the ClickHouse database
created above.

Export the [PostgreSQL client environment variables][pgenv] as appropriate to
configure the PostgreSQL server and user, and the [ClickHouse client
environment variables][chenv] to configure the server to which pg_clickhouse
connects.

### Run

Run this command to run the benchmark:

```sh
make run
```

This will save the results in the `result` directory and produce a Markdown
table report with the average time between three runs of each query in
[queries](queries) for both PostgreSQL and pg_clickhouse.

### Cleanup

Run this command to clean up artifacts from running the benchmark:

```sh
make clean
```

### Links

Use this command to create link references for the Markdown links to each
query in the table output:

```sh
make links
```

## Queries

The [queries](queries) duplicate those from the [pgtpc project]. The only
changes are to explicitly compare against dates, rather than timestamps, since
ClickHouse does not implicitly cast them for the comparison. The changes are
all from something like this (as in [Query 1](queries/1.sql)):

```sql
l_shipdate <= date '1998-12-01' - interval '90d'
```

To

```sql
l_shipdate <= date(date '1998-12-01' - interval '90d')
```

Reveal the complete diff below for details.

<details>
<summary>Complete Queries Diff</summary>

```diff
diff --git queries/1.sql b/tpch/queries/1.sql
index 20cecac..e4f841b 100644
--- queries/1.sql
+++ b/tpch/queries/1.sql
@@ -15,7 +15,7 @@ select
 from
 	lineitem
 where
-	l_shipdate <= date '1998-12-01' - interval '90d'
+	l_shipdate <= date(date '1998-12-01' - interval '90d')
 group by
 	l_returnflag,
 	l_linestatus
diff --git queries/10.sql b/tpch/queries/10.sql
index d063981..5cd8cd4 100644
--- queries/10.sql
+++ b/tpch/queries/10.sql
@@ -20,7 +20,7 @@ where
 	c_custkey = o_custkey
 	and l_orderkey = o_orderkey
 	and o_orderdate >= date '1993-10-01'
-	and o_orderdate < date '1993-10-01' + interval '3month'
+	and o_orderdate < date(date '1993-10-01' + interval '3month')
 	and l_returnflag = 'R'
 	and c_nationkey = n_nationkey
 group by
diff --git queries/12.sql b/tpch/queries/12.sql
index 83dd6b4..b2bbd38 100644
--- queries/12.sql
+++ b/tpch/queries/12.sql
@@ -25,7 +25,7 @@ where
 	and l_commitdate < l_receiptdate
 	and l_shipdate < l_commitdate
 	and l_receiptdate >= date '1994-01-01'
-	and l_receiptdate < date '1994-01-01' + interval '1y'
+	and l_receiptdate < date(date '1994-01-01' + interval '1y')
 group by
 	l_shipmode
 order by
diff --git queries/14.sql b/tpch/queries/14.sql
index b8949b6..e5642ff 100644
--- queries/14.sql
+++ b/tpch/queries/14.sql
@@ -14,4 +14,4 @@ from
 where
 	l_partkey = p_partkey
 	and l_shipdate >= date '1995-09-01'
-	and l_shipdate < date '1995-09-01' + interval '1month';
+	and l_shipdate < date(date '1995-09-01' + interval '1month');
diff --git queries/15.sql b/tpch/queries/15.sql
index c4fba55..f0e2166 100644
--- queries/15.sql
+++ b/tpch/queries/15.sql
@@ -8,7 +8,7 @@ create or replace view revenue0 (supplier_no, total_revenue) as
 		lineitem
 	where
 		l_shipdate >= date '1996-01-01'
-		and l_shipdate < date '1996-01-01' + interval '3month'
+		and l_shipdate < date(date '1996-01-01' + interval '3month')
 	group by
 		l_suppkey;

diff --git queries/20.sql b/tpch/queries/20.sql
index babc01c..29de2f3 100644
--- queries/20.sql
+++ b/tpch/queries/20.sql
@@ -32,7 +32,7 @@ where
 					l_partkey = ps_partkey
 					and l_suppkey = ps_suppkey
 					and l_shipdate >= date '1994-01-01'
-					and l_shipdate < date '1994-01-01' + interval '1' year
+					and l_shipdate < date(date '1994-01-01' + interval '1' year)
 			)
 	)
 	and s_nationkey = n_nationkey
diff --git queries/4.sql b/tpch/queries/4.sql
index 7f7011e..75df4ab 100644
--- queries/4.sql
+++ b/tpch/queries/4.sql
@@ -8,7 +8,7 @@ select
 from
 	orders
 where
-	o_orderdate >= date '1993-07-01'and o_orderdate < date '1993-07-01' + interval '3month'
+	o_orderdate >= date '1993-07-01'and o_orderdate < date(date '1993-07-01' + interval '3month')
 	and exists (select * from lineitem where l_orderkey = o_orderkey and l_commitdate < l_receiptdate)
 group by
 	o_orderpriority
diff --git queries/5.sql b/tpch/queries/5.sql
index b085c1b..2ccd73e 100644
--- queries/5.sql
+++ b/tpch/queries/5.sql
@@ -21,7 +21,7 @@ where
 	and n_regionkey = r_regionkey
 	and r_name = 'ASIA'
 	and o_orderdate >= date '1994-01-01'
-	and o_orderdate < date '1994-01-01' + interval '1year'
+	and o_orderdate < date(date '1994-01-01' + interval '1year')
 group by
 	n_name
 order by
diff --git queries/6.sql b/tpch/queries/6.sql
index 053d79e..0e06313 100644
--- queries/6.sql
+++ b/tpch/queries/6.sql
@@ -8,6 +8,6 @@ from
 	lineitem
 where
 	l_shipdate >= date '1994-01-01'
-	and l_shipdate < date '1994-01-01' + interval '1year'
+	and l_shipdate < date(date '1994-01-01' + interval '1year')
 	and l_discount between .06 - 0.01 and .06 + 0.01
 	and l_quantity < 24;
```

</details>

  [TPC-H]: https://www.tpc.org/tpch/
  [ClickHouse TPC-H]: https://clickhouse.com/docs/getting-started/example-datasets/tpch
  [chenv]: https://clickhouse.com/docs/interfaces/cli#environment-variable-options
    "ClickHouse Client Docs: Environment variable options"
  [PostgreSQL TPC-H]: https://github.com/Vonng/pgtpc/tree/master/tpch/ddl
  [pgenv]: https://www.postgresql.org/docs/current/libpq-envars.html
    "PostgreSQL libpq Docs: Environment Variables"
  [pgtpc project]: https://github.com/Vonng/pgtpc/tree/master/tpch
