#!/usr/bin/env python3
"""
storage_engine comprehensive test suite.

Covers all features before PGXN publication:
  - Extension lifecycle (install, schema, catalog objects)
  - colcompress and rowcompress DML
  - Compression codec options (zstd, lz4, pglz)
  - Sort key (orderby) option
  - Vectorized aggregates — correctness (VEC OFF == VEC ON) for every type
      smallint, integer, bigint, float8, numeric, money, date
  - count(*) and count(col)
  - NULL handling (empty table, all-NULL column, mixed NULLs)
  - EXPLAIN plan verification (StorageEngineVectorAgg node)
  - EXPLAIN ANALYZE correctness (not blocked by IsExplainQuery)
  - Parallel safety (AGGSPLIT_SIMPLE guard)
  - Multi-column aggregate query
  - Upgrade path chain traversability

Usage:
    python3 tests/test_suite.py                     # PG@5432 only
    python3 tests/test_suite.py --pg19              # PG@5432 + PG@5433
    python3 tests/test_suite.py --port 5433         # single custom port
    python3 tests/test_suite.py --port 5432 --pg19 --pg19-port 5433
"""

import argparse
import subprocess
import sys

GREEN  = "\033[32m"
RED    = "\033[31m"
YELLOW = "\033[33m"
BOLD   = "\033[1;36m"
RESET  = "\033[0m"

PASS_LABEL = f"{GREEN}PASS{RESET}"
FAIL_LABEL = f"{RED}FAIL{RESET}"


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

class TestRunner:
    def __init__(self, port: int, label: str = ""):
        self.port    = port
        self.label   = label or f"PG@{port}"
        self.dbname  = "storage_engine_test"
        self.passed  = 0
        self.failed  = 0
        self.failures: list[str] = []

    # ------------------------------------------------------------------ psql

    def _run(self, sql: str, dbname: str | None = None,
             tuples_only: bool = True) -> tuple[str, int, str]:
        """Execute SQL via psql. Returns (stdout, returncode, stderr)."""
        db   = dbname or self.dbname
        flag = ["-Atc"] if tuples_only else ["-v", "ON_ERROR_STOP=1", "-c"]
        cmd  = ["sudo", "-u", "postgres", "psql",
                "-p", str(self.port), "-d", db] + flag + [sql]
        r    = subprocess.run(cmd, capture_output=True, text=True)
        # Use rstrip('\n') not strip(): preserves the empty line that represents
        # a NULL value in tuples-only mode (strip() would destroy it).
        return r.stdout.rstrip("\n"), r.returncode, r.stderr.strip()

    def q(self, sql: str, dbname: str | None = None) -> str:
        """Return all output rows joined by newline."""
        out, _rc, _err = self._run(sql, dbname=dbname)
        return out

    def q1(self, sql: str, dbname: str | None = None) -> str:
        """Return first column of LAST row (empty string for NULL).

        Using the last line handles the common pattern of prepending SET
        statements to a SELECT: psql outputs one 'SET' line per GUC before
        the actual query result, so taking the last line gives the result.
        """
        out = self.q(sql, dbname=dbname)
        if not out:
            return ""
        lines = out.split("\n")
        return lines[-1].split("|")[0]

    def exec(self, sql: str, dbname: str | None = None) -> tuple[int, str]:
        """Execute DDL/DML. Returns (returncode, stderr)."""
        _out, rc, err = self._run(sql, dbname=dbname, tuples_only=False)
        return rc, err

    # ------------------------------------------------------------------ assertions

    def check(self, name: str, cond: bool, detail: str = "") -> None:
        if cond:
            print(f"  [{PASS_LABEL}] {name}")
            self.passed += 1
        else:
            detail_str = f"\n         {detail}" if detail else ""
            print(f"  [{FAIL_LABEL}] {name}{detail_str}")
            self.failed += 1
            tag = f"{self.label}: {name}"
            self.failures.append(tag + (f" — {detail}" if detail else ""))

    def check_eq(self, name: str, got: str, expected: str) -> None:
        self.check(name, got.strip() == expected.strip(),
                   f"got={got!r}  expected={expected!r}")

    def agg_ok(self, label: str, sql: str) -> None:
        """Assert that VEC OFF and VEC ON produce identical output."""
        pfx_off = ("SET max_parallel_workers_per_gather=0; "
                   "SET storage_engine.enable_vectorization=off;")
        pfx_on  = ("SET max_parallel_workers_per_gather=0; "
                   "SET storage_engine.enable_vectorization=on;")
        off = self.q(f"{pfx_off} {sql}")
        on  = self.q(f"{pfx_on}  {sql}")
        self.check(label, off == on, f"OFF={off!r}  ON={on!r}")

    def section(self, title: str) -> None:
        print(f"\n{BOLD}{'─' * 62}{RESET}")
        print(f"{BOLD}  {self.label}: {title}{RESET}")
        print(f"{BOLD}{'─' * 62}{RESET}")

    # ------------------------------------------------------------------ setup / teardown

    def setup(self) -> None:
        self._run(f"DROP DATABASE IF EXISTS {self.dbname}", dbname="postgres")
        self._run(f"CREATE DATABASE {self.dbname}", dbname="postgres")
        rc, err = self.exec("CREATE EXTENSION storage_engine")
        if rc != 0:
            print(f"{RED}FATAL: CREATE EXTENSION failed — {err}{RESET}")
            sys.exit(1)

    def teardown(self) -> None:
        self._run(f"DROP DATABASE IF EXISTS {self.dbname}", dbname="postgres")

    # ================================================================== tests

    def test_extension_lifecycle(self) -> None:
        self.section("Extension Lifecycle")

        # Version
        ver = self.q1("SELECT extversion FROM pg_extension WHERE extname='storage_engine'")
        self.check("extension version = 1.2.8", ver == "1.2.8", f"got {ver!r}")

        # Schema
        ns = self.q1("SELECT nspname FROM pg_namespace WHERE nspname='engine'")
        self.check("schema engine exists", ns == "engine")

        # Catalog tables
        for tbl in ("col_options", "stripe", "chunk_group", "chunk"):
            cnt = self.q1(
                f"SELECT count(*) FROM pg_class c "
                f"JOIN pg_namespace n ON n.oid = c.relnamespace "
                f"WHERE c.relname = '{tbl}' AND n.nspname = 'engine'"
            )
            self.check(f"catalog table engine.{tbl} exists", cnt == "1")

        # Table access methods
        for am in ("colcompress", "rowcompress"):
            cnt = self.q1(f"SELECT count(*) FROM pg_am WHERE amname = '{am}'")
            self.check(f"access method {am} registered", cnt == "1")

        # Key aggregate names (spot-check: one per type group)
        spot_aggs = [
            ("vcount",  "count"),
            ("vmin",    "integer"),
            ("vmax",    "smallint"),
            ("vsum",    "bigint"),
            ("vavg",    "bigint"),
            ("vmin",    "date"),
            ("vsum",    "double precision"),
            ("vavg",    "double precision"),
            ("vmin",    "numeric"),
            ("vavg",    "numeric"),
            ("vsum",    "money"),
            ("vmin",    "money"),
            ("vmax",    "money"),
        ]
        for agg_name, arg_type in spot_aggs:
            arg_clause = (
                "p.pronargs = 0"
                if arg_type == "count"
                else f"p.proargtypes::regtype[]::text LIKE '%{arg_type}%'"
            )
            cnt = self.q1(
                f"SELECT count(*) FROM pg_proc p "
                f"JOIN pg_namespace n ON n.oid = p.pronamespace "
                f"WHERE n.nspname = 'engine' AND p.proname = '{agg_name}' "
                f"  AND {arg_clause} "
                f"  AND p.oid IN (SELECT aggfnoid FROM pg_aggregate)"
            )
            lbl = f"aggregate engine.{agg_name}({arg_type}) registered"
            self.check(lbl, cnt != "0", f"count={cnt}")

        # No engine.vavg(money) — PostgreSQL has no avg(money)
        cnt = self.q1(
            "SELECT count(*) FROM pg_proc p "
            "JOIN pg_namespace n ON n.oid = p.pronamespace "
            "WHERE n.nspname = 'engine' AND p.proname = 'vavg' "
            "  AND p.proargtypes::regtype[]::text LIKE '%money%' "
            "  AND p.oid IN (SELECT aggfnoid FROM pg_aggregate)"
        )
        self.check("no engine.vavg(money) — PostgreSQL has no avg(money)", cnt == "0", f"count={cnt}")

    # ------------------------------------------------------------------ DML

    def test_colcompress_dml(self) -> None:
        self.section("colcompress DML")
        N = 50_000

        rc, err = self.exec(f"""
            DROP TABLE IF EXISTS _tc;
            CREATE TABLE _tc (id int, val bigint, label text, d date) USING colcompress;
            INSERT INTO _tc
                SELECT i, i*3, 'label_'||i, '2020-01-01'::date + i
                FROM generate_series(1,{N}) i;
        """)
        self.check("CREATE TABLE + INSERT colcompress", rc == 0, err[:200] if err else "")

        cnt = self.q1("SELECT count(*) FROM _tc")
        self.check_eq(f"row count after INSERT = {N}", cnt, str(N))

        cnt2 = self.q1(f"SELECT count(*) FROM _tc WHERE id > {N // 2}")
        self.check_eq("SELECT with WHERE filter", cnt2, str(N // 2))

        rc, err = self.exec("UPDATE _tc SET label = 'updated' WHERE id = 1")
        self.check("UPDATE single row", rc == 0, err[:200] if err else "")

        upd = self.q1("SELECT label FROM _tc WHERE id = 1")
        self.check_eq("UPDATE value persisted", upd, "updated")

        rc, err = self.exec(f"DELETE FROM _tc WHERE id > {N - 10}")
        self.check("DELETE 10 rows", rc == 0, err[:200] if err else "")

        cnt3 = self.q1("SELECT count(*) FROM _tc")
        self.check_eq(f"count after DELETE = {N - 10}", cnt3, str(N - 10))

        rc, err = self.exec("TRUNCATE _tc")
        self.check("TRUNCATE", rc == 0, err[:200] if err else "")

        cnt4 = self.q1("SELECT count(*) FROM _tc")
        self.check_eq("count after TRUNCATE = 0", cnt4, "0")

        # Re-populate for later aggregate tests
        rc, err = self.exec(f"""
            INSERT INTO _tc
                SELECT i, i*3, 'label_'||i, '2020-01-01'::date + i
                FROM generate_series(1,{N}) i;
        """)
        self.check("re-INSERT after TRUNCATE", rc == 0, err[:200] if err else "")

    def test_rowcompress_dml(self) -> None:
        self.section("rowcompress DML")
        N = 10_000

        rc, err = self.exec(f"""
            DROP TABLE IF EXISTS _tr;
            CREATE TABLE _tr (id int, val bigint, label text) USING rowcompress;
            INSERT INTO _tr
                SELECT i, i * 2, 'row_'||i
                FROM generate_series(1,{N}) i;
        """)
        self.check("CREATE TABLE + INSERT rowcompress", rc == 0, err[:200] if err else "")

        cnt = self.q1("SELECT count(*) FROM _tr")
        self.check_eq(f"row count = {N}", cnt, str(N))

        cnt2 = self.q1(f"SELECT count(*) FROM _tr WHERE val > {N}")
        self.check_eq("SELECT with WHERE filter (val > N → N/2 rows)", cnt2, str(N // 2))

        rc, err = self.exec("UPDATE _tr SET label = 'changed' WHERE id = 42")
        self.check("UPDATE rowcompress", rc == 0, err[:200] if err else "")

        rc, err = self.exec("DELETE FROM _tr WHERE id > 9000")
        self.check("DELETE rowcompress", rc == 0, err[:200] if err else "")

    # ------------------------------------------------------------------ options

    def test_compression_options(self) -> None:
        self.section("Compression Options")

        # Options are set via engine.alter_colcompress_table_set(), not WITH clause.
        for codec in ("zstd", "lz4", "pglz"):
            rc, err = self.exec(f"""
                DROP TABLE IF EXISTS _tcodec;
                CREATE TABLE _tcodec (id int, payload text) USING colcompress;
                SELECT engine.alter_colcompress_table_set('_tcodec', compression => '{codec}');
                INSERT INTO _tcodec
                    SELECT i, repeat('storage_engine_', 10)
                    FROM generate_series(1, 2000) i;
            """)
            self.check(f"colcompress: alter_colcompress_table_set compression='{codec}'",
                       rc == 0, err[:200] if err else "")

            cnt = self.q1("SELECT count(*) FROM _tcodec")
            self.check_eq(f"  row count with {codec} = 2000", cnt, "2000")

            saved = self.q1(
                "SELECT compression FROM engine.col_options "
                "WHERE regclass = '_tcodec'::regclass"
            )
            self.check(f"  engine.col_options.compression = '{codec}'",
                       saved == codec, f"got {saved!r}")

    def test_sort_key(self) -> None:
        self.section("Sort Key (orderby) Option")

        # orderby is set via engine.alter_colcompress_table_set() after CREATE.
        rc, err = self.exec("""
            DROP TABLE IF EXISTS _tsort;
            CREATE TABLE _tsort (ts date, val integer) USING colcompress;
            SELECT engine.alter_colcompress_table_set('_tsort', orderby => 'ts');
            INSERT INTO _tsort
                SELECT '2020-01-01'::date + (i % 3650), i
                FROM generate_series(1, 100000) i;
        """)
        self.check("alter_colcompress_table_set orderby='ts'", rc == 0, err[:200] if err else "")

        saved = self.q1(
            "SELECT orderby FROM engine.col_options "
            "WHERE regclass = '_tsort'::regclass"
        )
        self.check("engine.col_options.orderby = 'ts'", saved == "ts", f"got {saved!r}")

        cnt = self.q1("SELECT count(*) FROM _tsort")
        self.check_eq("all rows readable after sorted INSERT", cnt, "100000")

    # ------------------------------------------------------------------ vectorized aggregates

    def test_vect_int_aggregates(self) -> None:
        self.section("Vectorized Aggregates — smallint / integer / bigint")

        self.exec("""
            DROP TABLE IF EXISTS _tagg_int;
            CREATE TABLE _tagg_int (i2 smallint, i4 integer, i8 bigint) USING colcompress;
            INSERT INTO _tagg_int
                SELECT (i % 32767)::smallint, i, i::bigint * 10
                FROM generate_series(1, 100000) i;
        """)

        for col, typ in (("i2", "smallint"), ("i4", "integer"), ("i8", "bigint")):
            self.agg_ok(f"count(*)   [{typ}]",
                        f"SELECT count(*) FROM _tagg_int")
            self.agg_ok(f"count(col) [{typ}]",
                        f"SELECT count({col}) FROM _tagg_int")
            self.agg_ok(f"min({typ})",
                        f"SELECT min({col}) FROM _tagg_int")
            self.agg_ok(f"max({typ})",
                        f"SELECT max({col}) FROM _tagg_int")
            self.agg_ok(f"sum({typ})",
                        f"SELECT sum({col}) FROM _tagg_int")
            self.agg_ok(f"avg({typ})",
                        f"SELECT round(avg({col})::numeric, 6) FROM _tagg_int")

    def test_vect_float8_aggregates(self) -> None:
        self.section("Vectorized Aggregates — float8 (double precision)")

        self.exec("""
            DROP TABLE IF EXISTS _tagg_f8;
            CREATE TABLE _tagg_f8 (val double precision) USING colcompress;
            INSERT INTO _tagg_f8
                SELECT 1.0 + (i % 9999) * 0.01
                FROM generate_series(1, 100000) i;
        """)

        self.agg_ok("count(*)   [float8]",
                    "SELECT count(*) FROM _tagg_f8")
        self.agg_ok("count(col) [float8]",
                    "SELECT count(val) FROM _tagg_f8")
        self.agg_ok("min(float8)",
                    "SELECT min(val)::text FROM _tagg_f8")
        self.agg_ok("max(float8)",
                    "SELECT max(val)::text FROM _tagg_f8")
        self.agg_ok("sum(float8)",
                    "SELECT round(sum(val)::numeric, 4) FROM _tagg_f8")
        self.agg_ok("avg(float8)",
                    "SELECT round(avg(val)::numeric, 6) FROM _tagg_f8")

    def test_vect_numeric_aggregates(self) -> None:
        self.section("Vectorized Aggregates — numeric")

        self.exec("""
            DROP TABLE IF EXISTS _tagg_num;
            CREATE TABLE _tagg_num (val numeric(14, 4)) USING colcompress;
            INSERT INTO _tagg_num
                SELECT (1000 + (i % 9000))::numeric(14,4) / 100
                FROM generate_series(1, 100000) i;
        """)

        self.agg_ok("count(*)   [numeric]",
                    "SELECT count(*) FROM _tagg_num")
        self.agg_ok("count(col) [numeric]",
                    "SELECT count(val) FROM _tagg_num")
        self.agg_ok("min(numeric)",
                    "SELECT min(val) FROM _tagg_num")
        self.agg_ok("max(numeric)",
                    "SELECT max(val) FROM _tagg_num")
        self.agg_ok("sum(numeric)",
                    "SELECT sum(val) FROM _tagg_num")
        self.agg_ok("avg(numeric)",
                    "SELECT round(avg(val), 6) FROM _tagg_num")

    def test_vect_money_aggregates(self) -> None:
        self.section("Vectorized Aggregates — money")

        self.exec("""
            DROP TABLE IF EXISTS _tagg_money;
            CREATE TABLE _tagg_money (val money) USING colcompress;
            INSERT INTO _tagg_money
                SELECT ((100 + i % 9900)::numeric / 100)::money
                FROM generate_series(1, 100000) i;
        """)

        self.agg_ok("count(*)   [money]",
                    "SELECT count(*) FROM _tagg_money")
        self.agg_ok("min(money)",
                    "SELECT min(val) FROM _tagg_money")
        self.agg_ok("max(money)",
                    "SELECT max(val) FROM _tagg_money")
        self.agg_ok("sum(money)",
                    "SELECT sum(val) FROM _tagg_money")

    def test_vect_date_aggregates(self) -> None:
        self.section("Vectorized Aggregates — date")

        self.exec("""
            DROP TABLE IF EXISTS _tagg_date;
            CREATE TABLE _tagg_date (val date) USING colcompress;
            INSERT INTO _tagg_date
                SELECT '2000-01-01'::date + (i % 10000)
                FROM generate_series(1, 100000) i;
        """)

        self.agg_ok("count(*)   [date]",
                    "SELECT count(*) FROM _tagg_date")
        self.agg_ok("count(col) [date]",
                    "SELECT count(val) FROM _tagg_date")
        self.agg_ok("min(date)",
                    "SELECT min(val) FROM _tagg_date")
        self.agg_ok("max(date)",
                    "SELECT max(val) FROM _tagg_date")

    # ------------------------------------------------------------------ multi-column

    def test_multi_column_aggregates(self) -> None:
        self.section("Multi-column / Multi-type Aggregate Query")

        self.exec("""
            DROP TABLE IF EXISTS _tmulti;
            CREATE TABLE _tmulti (
                i2  smallint,
                i4  integer,
                i8  bigint,
                f8  double precision,
                num numeric(12, 4),
                d   date,
                m   money
            ) USING colcompress;
            INSERT INTO _tmulti
            SELECT
                (i % 32767)::smallint,
                i,
                i::bigint * 2,
                i * 0.01,
                (i * 0.01)::numeric(12, 4),
                '2020-01-01'::date + (i % 3650),
                (i * 0.01)::numeric::money
            FROM generate_series(1, 100000) i;
        """)

        self.agg_ok(
            "multi-type query: count/min/max for all columns",
            "SELECT count(*), "
            "min(i2), max(i2), "
            "min(i4), max(i4), "
            "min(i8), max(i8), "
            "round(min(f8)::numeric, 4), round(max(f8)::numeric, 4), "
            "min(num), max(num), "
            "min(d), max(d), "
            "min(m), max(m) "
            "FROM _tmulti",
        )
        self.agg_ok(
            "multi-type query: sum/avg for numeric types",
            "SELECT sum(i4), round(avg(i4)::numeric, 6), "
            "sum(i8), "
            "round(sum(f8)::numeric, 2), round(avg(f8)::numeric, 6), "
            "sum(num), round(avg(num), 6), "
            "sum(m) "
            "FROM _tmulti",
        )

    # ------------------------------------------------------------------ NULL handling

    def test_null_handling(self) -> None:
        self.section("NULL Handling")

        # Note: direct q1() calls below do NOT prepend SET statements so the
        # last-line heuristic in q1() always returns the SELECT result.
        # All NULL-vs-expected correctness is verified via agg_ok() which
        # compares VEC OFF against VEC ON (both sides produce the same NULL).

        # --- empty table ---
        self.exec("""
            DROP TABLE IF EXISTS _tnull;
            CREATE TABLE _tnull (
                i2  smallint,
                i4  integer,
                f8  double precision,
                num numeric,
                d   date,
                m   money
            ) USING colcompress;
        """)

        cnt = self.q1("SELECT count(*) FROM _tnull")
        self.check_eq("empty table: count(*) = 0", cnt, "0")

        # Correctness: VEC OFF == VEC ON for NULL-returning aggregates
        for col, agg in (
            ("i2",  "min"), ("i2",  "max"), ("i2",  "sum"),
            ("i4",  "min"), ("i4",  "max"), ("i4",  "sum"),
            ("f8",  "min"), ("f8",  "max"),
            ("num", "min"), ("num", "max"),
            ("d",   "min"), ("d",   "max"),
            ("m",   "min"), ("m",   "max"),
        ):
            self.agg_ok(f"empty table: {agg}({col}) VEC==OFF",
                        f"SELECT {agg}({col}) FROM _tnull")

        # Verify the result is actually NULL (not some sentinel like INT_MIN/0)
        for col, agg in (("i2", "sum"), ("i4", "sum"), ("i4", "min"), ("f8", "max"), ("m", "sum")):
            val = self.q1(f"SELECT {agg}({col}) FROM _tnull")
            self.check(f"empty table: {agg}({col}) IS NULL (VEC default)",
                       val == "", f"got {val!r}")

        # --- all-NULL column ---
        self.exec("""
            INSERT INTO _tnull
                SELECT i, i, NULL, NULL, NULL, NULL
                FROM generate_series(1, 1000) i;
        """)

        star = self.q1("SELECT count(*) FROM _tnull")
        self.check_eq("all-NULL cols: count(*) = 1000", star, "1000")

        col_cnt = self.q1("SELECT count(f8) FROM _tnull")
        self.check_eq("all-NULL col: count(f8) = 0", col_cnt, "0")

        self.agg_ok("all-NULL col: min(f8) VEC==OFF", "SELECT min(f8) FROM _tnull")
        self.agg_ok("all-NULL col: max(num) VEC==OFF", "SELECT max(num) FROM _tnull")
        self.agg_ok("all-NULL col: sum(money) VEC==OFF", "SELECT sum(m) FROM _tnull")

        min_null = self.q1("SELECT min(f8) FROM _tnull")
        self.check("all-NULL col: min(f8) IS NULL", min_null == "", f"got {min_null!r}")

        max_null = self.q1("SELECT max(num) FROM _tnull")
        self.check("all-NULL col: max(num) IS NULL", max_null == "", f"got {max_null!r}")

        # --- mixed NULLs (every 3rd row NULL) ---
        self.exec("""
            DROP TABLE IF EXISTS _tmixed;
            CREATE TABLE _tmixed (val integer, fval double precision) USING colcompress;
            INSERT INTO _tmixed
                SELECT
                    CASE WHEN i % 3 = 0 THEN NULL ELSE i END,
                    CASE WHEN i % 3 = 0 THEN NULL ELSE i * 0.01 END
                FROM generate_series(1, 30000) i;
        """)

        self.agg_ok("mixed NULLs: count(*)",       "SELECT count(*)    FROM _tmixed")
        self.agg_ok("mixed NULLs: count(val)",     "SELECT count(val)  FROM _tmixed")
        self.agg_ok("mixed NULLs: min(integer)",   "SELECT min(val)    FROM _tmixed")
        self.agg_ok("mixed NULLs: max(integer)",   "SELECT max(val)    FROM _tmixed")
        self.agg_ok("mixed NULLs: sum(integer)",   "SELECT sum(val)    FROM _tmixed")
        self.agg_ok("mixed NULLs: avg(integer)",
                    "SELECT round(avg(val)::numeric, 6) FROM _tmixed")
        self.agg_ok("mixed NULLs: sum(float8)",
                    "SELECT round(sum(fval)::numeric, 4) FROM _tmixed")
        self.agg_ok("mixed NULLs: avg(float8)",
                    "SELECT round(avg(fval)::numeric, 6) FROM _tmixed")

        # single non-NULL row
        self.exec("""
            DROP TABLE IF EXISTS _tsingle;
            CREATE TABLE _tsingle (val integer) USING colcompress;
            INSERT INTO _tsingle VALUES (42);
        """)
        self.agg_ok("single row: min/max/sum/count",
                    "SELECT min(val), max(val), sum(val), count(*) FROM _tsingle")

    # ------------------------------------------------------------------ EXPLAIN

    def test_explain_plan(self) -> None:
        self.section("EXPLAIN Plan Verification")

        self.exec("""
            DROP TABLE IF EXISTS _texpl;
            CREATE TABLE _texpl (val integer) USING colcompress;
            INSERT INTO _texpl SELECT i FROM generate_series(1, 100000) i;
        """)

        # VEC ON → StorageEngineVectorAgg present
        plan_on = self.q(
            "SET storage_engine.enable_vectorization=on; "
            "SET max_parallel_workers_per_gather=0; "
            "EXPLAIN SELECT min(val), max(val), sum(val), count(*) FROM _texpl"
        )
        self.check("VEC ON: EXPLAIN shows StorageEngineVectorAgg",
                   "StorageEngineVectorAgg" in plan_on, plan_on[:400])
        self.check("VEC ON: EXPLAIN shows 'Engine Vectorized Aggregate: enabled'",
                   "Engine Vectorized Aggregate: enabled" in plan_on, plan_on[:400])

        # VEC OFF → no StorageEngineVectorAgg
        plan_off = self.q(
            "SET storage_engine.enable_vectorization=off; "
            "SET max_parallel_workers_per_gather=0; "
            "EXPLAIN SELECT min(val) FROM _texpl"
        )
        self.check("VEC OFF: EXPLAIN has no StorageEngineVectorAgg",
                   "StorageEngineVectorAgg" not in plan_off, plan_off[:400])

        # EXPLAIN ANALYZE should complete without error and show VectorAgg node
        out, rc, err = self._run(
            "SET storage_engine.enable_vectorization=on; "
            "SET max_parallel_workers_per_gather=0; "
            "EXPLAIN ANALYZE SELECT min(val), max(val), count(*) FROM _texpl"
        )
        self.check("EXPLAIN ANALYZE: no error (VEC ON)", rc == 0, err[:200] if err else "")
        self.check("EXPLAIN ANALYZE: shows StorageEngineVectorAgg",
                   "StorageEngineVectorAgg" in out, out[:400])

        # Plain EXPLAIN (no ANALYZE) on a filter query
        plan_filter = self.q(
            "SET storage_engine.enable_vectorization=on; "
            "EXPLAIN SELECT val FROM _texpl WHERE val > 50000"
        )
        self.check("EXPLAIN SELECT (filter): no crash, returns plan",
                   len(plan_filter) > 0, plan_filter[:200])

    # ------------------------------------------------------------------ parallel safety

    def test_parallel_safety(self) -> None:
        self.section("Parallel Safety (AGGSPLIT_SIMPLE guard)")

        self.exec("""
            DROP TABLE IF EXISTS _tpar;
            CREATE TABLE _tpar (val integer) USING colcompress;
            INSERT INTO _tpar SELECT i FROM generate_series(1, 100000) i;
        """)

        pfx_serial   = ("SET max_parallel_workers_per_gather=0; "
                        "SET storage_engine.enable_vectorization=on;")
        pfx_parallel = ("SET max_parallel_workers_per_gather=4; "
                        "SET storage_engine.enable_vectorization=on;")

        agg_sql = "SELECT min(val), max(val), sum(val), count(*) FROM _tpar"

        r_serial   = self.q(f"{pfx_serial}   {agg_sql}")
        r_parallel = self.q(f"{pfx_parallel} {agg_sql}")
        self.check("parallel=4: result equals serial",
                   r_serial == r_parallel,
                   f"serial={r_serial!r} parallel={r_parallel!r}")

        _, rc, err = self._run(f"{pfx_parallel} {agg_sql}")
        self.check("parallel=4: no crash / error", rc == 0, err[:200] if err else "")

        # Also test with float8, numeric, money under parallelism
        self.exec("""
            DROP TABLE IF EXISTS _tpar_typed;
            CREATE TABLE _tpar_typed (
                f8  double precision,
                num numeric(12,4),
                m   money
            ) USING colcompress;
            INSERT INTO _tpar_typed
                SELECT i * 0.01, (i * 0.01)::numeric(12,4), (i * 0.01)::numeric::money
                FROM generate_series(1, 100000) i;
        """)

        typed_sql = ("SELECT round(sum(f8)::numeric,2), "
                     "sum(num), sum(m) FROM _tpar_typed")

        r_s = self.q(f"{pfx_serial}   {typed_sql}")
        r_p = self.q(f"{pfx_parallel} {typed_sql}")
        self.check("parallel=4 float8+numeric+money: result equals serial",
                   r_s == r_p, f"serial={r_s!r} parallel={r_p!r}")

    # ------------------------------------------------------------------ upgrade path

    def test_upgrade_path(self) -> None:
        self.section("Upgrade Path Chain")

        # Latest version available
        ver = self.q1(
            "SELECT max(version) FROM pg_available_extension_versions "
            "WHERE name = 'storage_engine'"
        )
        self.check("latest available version = 1.2.8", ver == "1.2.8", f"got {ver!r}")

        # Complete upgrade path from 1.0 to 1.2.8 exists
        path = self.q1(
            "SELECT path FROM pg_extension_update_paths('storage_engine') "
            "WHERE source = '1.0' AND target = '1.2.8'"
        )
        self.check("upgrade path 1.0 \u2192 1.2.8 exists", path != "", f"path={path!r}")

        # Each individual upgrade step
        steps = [
            ("1.0", "1.1"),
            ("1.1", "1.2.0"),
            ("1.2.0", "1.2.1"),
            ("1.2.1", "1.2.2"),
            ("1.2.2", "1.2.3"),
            ("1.2.3", "1.2.4"),
            ("1.2.4", "1.2.5"),
            ("1.2.5", "1.2.6"),
            ("1.2.6", "1.2.7"),
            ("1.2.7", "1.2.8"),
        ]
        for src, tgt in steps:
            p = self.q1(
                f"SELECT path FROM pg_extension_update_paths('storage_engine') "
                f"WHERE source = '{src}' AND target = '{tgt}'"
            )
            self.check(f"upgrade step {src} → {tgt}", p != "", f"path={p!r}")

    # ================================================================== run all

    def run_all(self) -> bool:
        print(f"\n{'=' * 62}")
        print(f"  storage_engine test suite — {self.label}")
        print(f"{'=' * 62}")

        print(f"\n[setup] creating test database '{self.dbname}'…")
        self.setup()

        self.test_extension_lifecycle()
        self.test_colcompress_dml()
        self.test_rowcompress_dml()
        self.test_compression_options()
        self.test_sort_key()
        self.test_vect_int_aggregates()
        self.test_vect_float8_aggregates()
        self.test_vect_numeric_aggregates()
        self.test_vect_money_aggregates()
        self.test_vect_date_aggregates()
        self.test_multi_column_aggregates()
        self.test_null_handling()
        self.test_explain_plan()
        self.test_parallel_safety()
        self.test_upgrade_path()

        self.teardown()

        total = self.passed + self.failed
        print(f"\n{'=' * 62}")
        if self.failed == 0:
            print(f"  {self.label}: {GREEN}ALL {total} TESTS PASSED{RESET}")
        else:
            print(f"  {self.label}: {RED}{self.failed} FAILED{RESET} / {total} total")
            print(f"\n  Failures:")
            for f in self.failures:
                print(f"    • {f}")
        print(f"{'=' * 62}\n")
        return self.failed == 0


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="storage_engine test suite")
    parser.add_argument("--port",     type=int, default=5432,
                        help="Primary PostgreSQL port (default: 5432)")
    parser.add_argument("--pg19",     action="store_true",
                        help="Also run tests on the secondary PostgreSQL instance")
    parser.add_argument("--pg19-port", type=int, default=5433, dest="pg19_port",
                        help="Secondary PostgreSQL port (default: 5433)")
    args = parser.parse_args()

    all_ok = True

    r1 = TestRunner(args.port, label=f"PG@{args.port}")
    all_ok = r1.run_all() and all_ok

    if args.pg19:
        r2 = TestRunner(args.pg19_port, label=f"PG@{args.pg19_port}")
        all_ok = r2.run_all() and all_ok

    sys.exit(0 if all_ok else 1)


if __name__ == "__main__":
    main()
