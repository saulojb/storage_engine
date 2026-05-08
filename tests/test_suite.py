#!/usr/bin/env python3
"""
storage_engine comprehensive regression test suite — v2.1.0.

Covers:
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

  Phase 1–3 Maintenance / Merge Incremental (v2.1.0):
  - Catalog: col_maintenance_options, row_maintenance_options tables
  - Catalog: stripe.pruning_valid, stripe.dirty_rows columns
  - Catalog: row_batch.pruning_valid, row_batch.deleted_count columns
  - API: colcompress_set_maintenance / rowcompress_set_maintenance
  - API: storage_health view — dirty_units, tombstone_rows, recommended_action
  - API: storage_maintenance_recommendation function
  - API: storage_maintenance_stats function
  - Phase 2: pruning_valid marked false + dirty_rows incremented on DELETE
  - Phase 3: colcompress_merge_incremental — rewrite dirty stripes
  - Phase 3: rowcompress_merge_incremental — rewrite dirty batches
  - Merge idempotency, fully-dead stripe handling, live_rows preservation

Usage:
    python3 tests/test_suite.py                     # PG@5432 only
    python3 tests/test_suite.py --pg19              # PG@5432 + PG@5433
    python3 tests/test_suite.py --port 5433         # single custom port
    python3 tests/test_suite.py --port 5432 --pg19 --pg19-port 5433
    python3 tests/test_suite.py --ports 5432,5435   # multiple ports
"""

import argparse
import json
import re
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
            "-p", str(self.port), "-d", db, "--no-psqlrc"] + flag + [sql]
        r    = subprocess.run(cmd, capture_output=True, text=True)
        # Use rstrip('\n') not strip(): preserves the empty line that represents
        # a NULL value in tuples-only mode (strip() would destroy it).
        return r.stdout.rstrip("\n"), r.returncode, r.stderr.strip()

    def q(self, sql: str, dbname: str | None = None) -> str:
        """Return all output rows joined by newline."""
        out, _rc, _err = self._run(sql, dbname=dbname)
        return "\n".join(line for line in out.split("\n") if line != "SET")

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

    def _execution_time_ms(self, explain_output: str) -> float | None:
        """Extract execution time in ms from EXPLAIN ANALYZE output."""
        # Prefer JSON plans for locale-independent parsing.
        lines = [ln for ln in explain_output.splitlines() if ln and ln != "SET"]
        if lines:
            raw = "\n".join(lines)
            try:
                payload = json.loads(raw)
                if isinstance(payload, list) and payload and isinstance(payload[0], dict):
                    t = payload[0].get("Execution Time")
                    if isinstance(t, (int, float)):
                        return float(t)
            except Exception:
                pass

        patterns = [
            r"Execution Time:\\s*([0-9]+(?:[\\.,][0-9]+)?)\\s*ms",
            r"Execution time:\\s*([0-9]+(?:[\\.,][0-9]+)?)\\s*ms",
            r"Total runtime:\\s*([0-9]+(?:[\\.,][0-9]+)?)\\s*ms",
            r"Tempo de Execu(?:c|ç)[aã]o:\\s*([0-9]+(?:[\\.,][0-9]+)?)\\s*ms",
            r"Tempo de execu(?:c|ç)[aã]o:\\s*([0-9]+(?:[\\.,][0-9]+)?)\\s*ms",
        ]
        for pat in patterns:
            m = re.search(pat, explain_output)
            if m:
                return float(m.group(1).replace(",", "."))
        return None

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
        self.check("extension version = 2.2.0", ver == "2.2.0", f"got {ver!r}")

        # Schema
        ns = self.q1("SELECT nspname FROM pg_namespace WHERE nspname='engine'")
        self.check("schema engine exists", ns == "engine")

        # Core catalog tables
        for tbl in ("col_options", "stripe", "chunk_group", "chunk",
                    "row_options", "row_batch",
                    "col_maintenance_options", "row_maintenance_options"):
            cnt = self.q1(
                f"SELECT count(*) FROM pg_class c "
                f"JOIN pg_namespace n ON n.oid = c.relnamespace "
                f"WHERE c.relname = '{tbl}' AND n.nspname = 'engine'"
            )
            self.check(f"catalog table engine.{tbl} exists", cnt == "1")

        # Phase 2/2.5: new columns on stripe and row_batch
        for tbl, col in (
            ("stripe",    "pruning_valid"),
            ("stripe",    "dirty_rows"),
            ("row_batch", "pruning_valid"),
            ("row_batch", "deleted_count"),
        ):
            cnt = self.q1(
                f"SELECT count(*) FROM pg_attribute a "
                f"JOIN pg_class c ON c.oid = a.attrelid "
                f"JOIN pg_namespace n ON n.oid = c.relnamespace "
                f"WHERE n.nspname = 'engine' AND c.relname = '{tbl}' "
                f"  AND a.attname = '{col}' AND NOT a.attisdropped"
            )
            self.check(f"column engine.{tbl}.{col} exists", cnt == "1")

        # Phase 1: storage_health view
        cnt_view = self.q1(
            "SELECT count(*) FROM pg_class c "
            "JOIN pg_namespace n ON n.oid = c.relnamespace "
            "WHERE c.relname = 'storage_health' AND n.nspname = 'engine' AND c.relkind = 'v'"
        )
        self.check("view engine.storage_health exists", cnt_view == "1")

        # Phase 1: maintenance functions
        for fn_name, arg_types in (
            ("colcompress_set_maintenance",       "regclass, text, real, real"),
            ("rowcompress_set_maintenance",       "regclass, text, real, real"),
            ("storage_maintenance_recommendation", "regclass"),
            ("storage_maintenance_stats",          "regclass"),
        ):
            cnt = self.q1(
                f"SELECT count(*) FROM pg_proc p "
                f"JOIN pg_namespace n ON n.oid = p.pronamespace "
                f"WHERE n.nspname = 'engine' AND p.proname = '{fn_name}'"
            )
            self.check(f"function engine.{fn_name}({arg_types}) exists", cnt != "0")

        # Phase 3: merge incremental procedures
        for proc_name in ("colcompress_merge_incremental", "rowcompress_merge_incremental"):
            cnt = self.q1(
                f"SELECT count(*) FROM pg_proc p "
                f"JOIN pg_namespace n ON n.oid = p.pronamespace "
                f"WHERE n.nspname = 'engine' AND p.proname = '{proc_name}' "
                f"  AND p.prokind = 'p'"  # 'p' = procedure
            )
            self.check(f"procedure engine.{proc_name}(regclass, int) exists", cnt != "0")

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

        self.exec("""
            DROP TABLE IF EXISTS _texpl_mixed;
            CREATE TABLE _texpl_mixed (
                i4  integer,
                num numeric(12, 4),
                m   money
            ) USING colcompress;
            INSERT INTO _texpl_mixed
                SELECT
                    i,
                    (i * 0.01)::numeric(12, 4),
                    (i * 0.01)::numeric::money
                FROM generate_series(1, 100000) i;
        """)

        plan_mixed = self.q(
            "SET storage_engine.enable_vectorization=on; "
            "SET max_parallel_workers_per_gather=0; "
            "EXPLAIN SELECT sum(i4), round(avg(num), 6), sum(m) FROM _texpl_mixed"
        )
        self.check("mixed numeric+money: EXPLAIN falls back to regular Agg",
                   "StorageEngineVectorAgg" not in plan_mixed, plan_mixed[:400])

        self.agg_ok("mixed numeric+money: VEC ON falls back without changing result",
                    "SELECT sum(i4), round(avg(num), 6), sum(m) FROM _texpl_mixed")

        out, rc, err = self._run(
            "SET storage_engine.enable_vectorization=on; "
            "SET max_parallel_workers_per_gather=0; "
            "SELECT sum(i4), round(avg(num), 6), sum(m) FROM _texpl_mixed"
        )
        self.check("mixed numeric+money: VEC ON query runs without crash",
                   rc == 0, err[:200] if err else out[:200])

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

        # --------------------------------------------------------------
        # Plan-shape gates for real vectorized parallel execution.
        # --------------------------------------------------------------
        self.exec("""
            DROP TABLE IF EXISTS _tpar_group;
            CREATE TABLE _tpar_group (
                grp integer,
                v   integer,
                vn  numeric(12,2)
            ) USING colcompress;
            INSERT INTO _tpar_group
                SELECT i % 16, i, (i * 0.01)::numeric(12,2)
                FROM generate_series(1, 500000) i;
        """)

        pfx_parallel_vec = (
            "SET storage_engine.enable_vectorization=on; "
            "SET storage_engine.enable_vectorized_groupagg=on; "
            "SET max_parallel_workers_per_gather=4; "
            "SET parallel_setup_cost=0; "
            "SET parallel_tuple_cost=0; "
            "SET min_parallel_table_scan_size=0; "
            "SET min_parallel_index_scan_size=0;"
        )
        pfx_parallel_native = (
            "SET storage_engine.enable_vectorization=off; "
            "SET storage_engine.enable_vectorized_groupagg=off; "
            "SET max_parallel_workers_per_gather=4; "
            "SET parallel_setup_cost=0; "
            "SET parallel_tuple_cost=0; "
            "SET min_parallel_table_scan_size=0; "
            "SET min_parallel_index_scan_size=0;"
        )

        vec_group_sql = (
            "SELECT grp, count(*), sum(v), min(v), max(v) "
            "FROM _tpar_group GROUP BY grp ORDER BY grp"
        )

        plan_vec = self.q(f"{pfx_parallel_vec} EXPLAIN {vec_group_sql}")
        self.check(
            "parallel grouped count/sum/min/max: EXPLAIN shows StorageEngineVectorGroupAgg",
            "StorageEngineVectorGroupAgg" in plan_vec,
            plan_vec[:500],
        )
        self.check(
            "parallel grouped count/sum/min/max: EXPLAIN is parallel",
            "Parallel" in plan_vec,
            plan_vec[:500],
        )

        avg_group_sql = "SELECT grp, avg(v::float8) FROM _tpar_group GROUP BY grp"
        plan_avg = self.q(f"{pfx_parallel_vec} EXPLAIN {avg_group_sql}")
        server_version_num = self.q1("SHOW server_version_num")
        avg_groupagg_expected = (
            server_version_num and
            (int(server_version_num) < 200000)
        )
        if avg_groupagg_expected:
            self.check(
                "parallel grouped avg(v::float8): EXPLAIN shows StorageEngineVectorGroupAgg",
                "StorageEngineVectorGroupAgg" in plan_avg,
                plan_avg[:500],
            )
        else:
            self.check(
                "parallel grouped avg(v::float8): EXPLAIN falls back (version-limited)",
                "StorageEngineVectorGroupAgg" not in plan_avg,
                plan_avg[:500],
            )

        avg_group_int4_sql = "SELECT grp, avg(v) FROM _tpar_group GROUP BY grp"
        plan_avg_int4 = self.q(f"{pfx_parallel_vec} EXPLAIN {avg_group_int4_sql}")
        if avg_groupagg_expected:
            self.check(
                "parallel grouped avg(int4): EXPLAIN shows StorageEngineVectorGroupAgg",
                "StorageEngineVectorGroupAgg" in plan_avg_int4,
                plan_avg_int4[:500],
            )
        else:
            self.check(
                "parallel grouped avg(int4): EXPLAIN falls back (version-limited)",
                "StorageEngineVectorGroupAgg" not in plan_avg_int4,
                plan_avg_int4[:500],
            )

        plan_avg_int4_serial = self.q(
            "SET storage_engine.enable_vectorization=on; "
            "SET storage_engine.enable_vectorized_groupagg=on; "
            "SET max_parallel_workers_per_gather=0; "
            "EXPLAIN SELECT grp, avg(v) FROM _tpar_group GROUP BY grp"
        )
        if avg_groupagg_expected:
            self.check(
                "serial grouped avg(int4): EXPLAIN shows StorageEngineVectorGroupAgg",
                "StorageEngineVectorGroupAgg" in plan_avg_int4_serial,
                plan_avg_int4_serial[:500],
            )
        else:
            self.check(
                "serial grouped avg(int4): EXPLAIN falls back (version-limited)",
                "StorageEngineVectorGroupAgg" not in plan_avg_int4_serial,
                plan_avg_int4_serial[:500],
            )

        # PG15-only safety gate: numeric plain aggregate must fallback.
        server_version_num = self.q1("SHOW server_version_num")
        if server_version_num and int(server_version_num) < 160000:
            self.exec("""
                DROP TABLE IF EXISTS _tpar_num_plain;
                CREATE TABLE _tpar_num_plain (val numeric(14,4)) USING colcompress;
                INSERT INTO _tpar_num_plain
                    SELECT (1000 + (i % 9000))::numeric(14,4) / 100
                    FROM generate_series(1, 100000) i;
            """)
            plan_num_pg15 = self.q(
                "SET storage_engine.enable_vectorization=on; "
                "SET max_parallel_workers_per_gather=0; "
                "EXPLAIN SELECT count(val) FROM _tpar_num_plain"
            )
            self.check(
                "PG15 numeric plain aggregate: EXPLAIN falls back (no StorageEngineVectorAgg)",
                "StorageEngineVectorAgg" not in plan_num_pg15,
                plan_num_pg15[:500],
            )

        # Performance sanity gate (wide threshold to catch catastrophic regressions only).
        out_vec, rc_vec, err_vec = self._run(
            f"{pfx_parallel_vec} "
            f"EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY ON, FORMAT JSON) {vec_group_sql}"
        )
        out_nat, rc_nat, err_nat = self._run(
            f"{pfx_parallel_native} "
            f"EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY ON, FORMAT JSON) {vec_group_sql}"
        )

        self.check("parallel perf sanity: vectorized EXPLAIN ANALYZE executes", rc_vec == 0,
                   err_vec[:200] if err_vec else "")
        self.check("parallel perf sanity: native EXPLAIN ANALYZE executes", rc_nat == 0,
                   err_nat[:200] if err_nat else "")

        t_vec = self._execution_time_ms(out_vec) if rc_vec == 0 else None
        t_nat = self._execution_time_ms(out_nat) if rc_nat == 0 else None
        self.check("parallel perf sanity: execution times parsed",
                   t_vec is not None and t_nat is not None,
                   f"vec={t_vec!r} native={t_nat!r}")
        if t_vec is not None and t_nat is not None:
            self.check(
                "parallel perf sanity: vectorized <= 5x native",
                t_vec <= (t_nat * 5.0),
                f"vectorized={t_vec:.3f}ms native={t_nat:.3f}ms",
            )

    # ------------------------------------------------------------------ VecGroupAgg

    def test_vecgroupagg(self) -> None:
        self.section("VecGroupAgg — GROUP BY correctness")

        pfx_on  = ("SET storage_engine.enable_vectorization=on; "
                   "SET storage_engine.enable_vectorized_groupagg=on; "
                   "SET max_parallel_workers_per_gather=4; "
                   "SET parallel_setup_cost=0; SET parallel_tuple_cost=0; "
                   "SET min_parallel_table_scan_size=0; "
                   "SET min_parallel_index_scan_size=0;")
        pfx_off = ("SET storage_engine.enable_vectorization=off; "
                   "SET storage_engine.enable_vectorized_groupagg=off; "
                   "SET max_parallel_workers_per_gather=0;")

        # --- setup ---
        self.exec("""
            DROP TABLE IF EXISTS _tgrp;
            CREATE TABLE _tgrp (
                country char(2),
                cat     integer,
                val     float8,
                m       money
            ) USING colcompress;
            INSERT INTO _tgrp
            SELECT
                (ARRAY['BR','US','JP'])[1 + (i % 3)],
                1 + (i % 5),
                CASE WHEN i % 7  = 0 THEN NULL ELSE i * 1.5       END,
                CASE WHEN i % 11 = 0 THEN NULL ELSE (i * 0.99)::money END
            FROM generate_series(1, 1500000) i;
            ANALYZE _tgrp;

            DROP TABLE IF EXISTS _tgrp2;
            CREATE TABLE _tgrp2 (k1 integer, k2 integer, v bigint) USING colcompress;
            INSERT INTO _tgrp2
                SELECT i % 50, i % 20, i FROM generate_series(1, 2000000) i;
            ANALYZE _tgrp2;
        """)

        # 1. Single-key GROUP BY — correctness VEC ON == VEC OFF
        single_sql = ("SELECT country, count(*), count(val), "
                      "min(val), max(val), round(sum(val)::numeric, 2), "
                      "min(m), max(m), sum(m) "
                      "FROM _tgrp GROUP BY country ORDER BY country")
        r_off = self.q(f"{pfx_off} {single_sql}")
        r_on  = self.q(f"{pfx_on}  {single_sql}")
        self.check("single-key GROUP BY: VEC ON == VEC OFF", r_off == r_on,
                   f"OFF={r_off[:200]!r}  ON={r_on[:200]!r}")

        # 2. Single-key: EXPLAIN shows VecGroupAgg
        # Force HashAgg: disable sort so planner can't pick GroupAggregate
        plan = self.q(f"{pfx_on} SET enable_sort=off; EXPLAIN {single_sql}")
        self.check("single-key GROUP BY: EXPLAIN shows StorageEngineVectorGroupAgg",
                   "StorageEngineVectorGroupAgg" in plan, plan[:400])

        # 3. COUNT(col) with NULLs — val has NULLs every 7th row
        null_sql = ("SELECT country, count(*) AS cs, count(val) AS cv "
                    "FROM _tgrp GROUP BY country ORDER BY country")
        r_off2 = self.q(f"{pfx_off} {null_sql}")
        r_on2  = self.q(f"{pfx_on}  {null_sql}")
        self.check("COUNT(col) NULL-aware GROUP BY: VEC ON == VEC OFF",
                   r_off2 == r_on2, f"OFF={r_off2!r}  ON={r_on2!r}")

        # COUNT(*) > COUNT(col) because val has NULLs
        r_diff = self.q(
            f"{pfx_on} SELECT bool_and(cs > cv) FROM ({null_sql}) x"
        )
        self.check("COUNT(col) NULL-aware: count(*) > count(val) for all groups",
                   r_diff.strip() == "t", f"got {r_diff!r}")

        # 4. Composite GROUP BY (2 keys) — correctness
        composite_sql = ("SELECT country, cat, count(*), count(val), "
                         "round(sum(val)::numeric, 2), sum(m) "
                         "FROM _tgrp GROUP BY country, cat ORDER BY country, cat")
        r_off3 = self.q(f"{pfx_off} {composite_sql}")
        r_on3  = self.q(f"{pfx_on}  {composite_sql}")
        self.check("composite GROUP BY (2 keys): VEC ON == VEC OFF",
                   r_off3 == r_on3, f"OFF={r_off3[:200]!r}  ON={r_on3[:200]!r}")

        # 5. Composite GROUP BY: EXPLAIN shows VecGroupAgg
        # Disable parallel to get a plain HashAggregate (not Finalize/Partial split)
        # which the hook can intercept on all PG versions
        plan2 = self.q(f"{pfx_on} SET max_parallel_workers_per_gather=0; EXPLAIN "
                       "SELECT k1, k2, count(*), sum(v) FROM _tgrp2 GROUP BY k1, k2")
        self.check("composite GROUP BY (2 int keys): EXPLAIN shows StorageEngineVectorGroupAgg",
                   "StorageEngineVectorGroupAgg" in plan2, plan2[:400])

        # 6. HAVING — must still produce correct results (applied by Finalize node)
        having_sql = ("SELECT country, count(*) FROM _tgrp "
                      "GROUP BY country HAVING count(*) > 50000 ORDER BY country")
        r_off4 = self.q(f"{pfx_off} {having_sql}")
        r_on4  = self.q(f"{pfx_on}  {having_sql}")
        self.check("HAVING: VEC ON == VEC OFF (correct results)",
                   r_off4 == r_on4, f"OFF={r_off4!r}  ON={r_on4!r}")

        # HAVING must not be empty (all 3 groups have 100k rows > 50k)
        rows = [ln for ln in r_on4.strip().split("\n") if ln]
        self.check("HAVING: returns 3 groups (count > 50000)", len(rows) == 3,
                   f"rows={rows!r}")

        # 7. money (CASHOID) GROUP BY — correctness
        money_sql = ("SELECT country, count(m), min(m), max(m), sum(m) "
                     "FROM _tgrp GROUP BY country ORDER BY country")
        r_off5 = self.q(f"{pfx_off} {money_sql}")
        r_on5  = self.q(f"{pfx_on}  {money_sql}")
        self.check("money (CASHOID) GROUP BY: VEC ON == VEC OFF",
                   r_off5 == r_on5, f"OFF={r_off5[:200]!r}  ON={r_on5[:200]!r}")

        # 8. Composite GROUP BY (3 keys)
        three_key_sql = ("SELECT country, cat, val IS NULL AS vn, count(*) "
                         "FROM _tgrp GROUP BY country, cat, val IS NULL "
                         "ORDER BY country, cat, vn")
        r_off6 = self.q(f"{pfx_off} {three_key_sql}")
        r_on6  = self.q(f"{pfx_on}  {three_key_sql}")
        self.check("composite GROUP BY (3 keys): VEC ON == VEC OFF",
                   r_off6 == r_on6, f"OFF={r_off6[:200]!r}  ON={r_on6[:200]!r}")

        # 9. CASE WHEN filter — SUM(CASE WHEN cat=1 THEN val END)
        self.exec("""
            DROP TABLE IF EXISTS _tgrp3;
            CREATE TABLE _tgrp3 (
                grp   text,
                cat   integer,
                val   float8
            ) USING colcompress;
            INSERT INTO _tgrp3
            SELECT
                (ARRAY['A','B','C'])[1 + (i % 3)],
                1 + (i % 4),
                i * 1.0
            FROM generate_series(1, 300000) i;
            ANALYZE _tgrp3;
        """)
        case_sql = ("SELECT grp, "
                    "SUM(CASE WHEN cat=1 THEN val END) AS s1, "
                    "SUM(CASE WHEN cat=2 THEN val END) AS s2 "
                    "FROM _tgrp3 GROUP BY grp ORDER BY grp")
        r_off7 = self.q(f"{pfx_off} {case_sql}")
        r_on7  = self.q(f"{pfx_on} SET max_parallel_workers_per_gather=0; {case_sql}")
        self.check("CASE WHEN filter SUM: VEC ON == VEC OFF",
                   r_off7 == r_on7, f"OFF={r_off7!r}  ON={r_on7!r}")

        # 10. COUNT(DISTINCT col) — integer key
        self.exec("""
            DROP TABLE IF EXISTS _tgrp4;
            CREATE TABLE _tgrp4 (
                grp   integer,
                uid   integer,
                tag   text
            ) USING colcompress;
            INSERT INTO _tgrp4
            SELECT
                i % 5,
                (i % 200),
                (ARRAY['x','y','z'])[1 + (i % 3)]
            FROM generate_series(1, 300000) i;
            ANALYZE _tgrp4;
        """)
        cdist_int_sql = ("SELECT grp, COUNT(DISTINCT uid) AS dc "
                         "FROM _tgrp4 GROUP BY grp ORDER BY grp")
        r_off8 = self.q(f"{pfx_off} {cdist_int_sql}")
        r_on8  = self.q(f"{pfx_on} SET max_parallel_workers_per_gather=0; {cdist_int_sql}")
        self.check("COUNT(DISTINCT int): VEC ON == VEC OFF",
                   r_off8 == r_on8, f"OFF={r_off8!r}  ON={r_on8!r}")

        # 11. COUNT(DISTINCT col) — text key
        cdist_txt_sql = ("SELECT grp, COUNT(DISTINCT tag) AS dt "
                         "FROM _tgrp4 GROUP BY grp ORDER BY grp")
        r_off9 = self.q(f"{pfx_off} {cdist_txt_sql}")
        r_on9  = self.q(f"{pfx_on} SET max_parallel_workers_per_gather=0; {cdist_txt_sql}")
        self.check("COUNT(DISTINCT text): VEC ON == VEC OFF",
                   r_off9 == r_on9, f"OFF={r_off9!r}  ON={r_on9!r}")

        # 12. SUM(arithmetic expression) — SUM(price * qty)
        self.exec("""
            DROP TABLE IF EXISTS _tgrp5;
            CREATE TABLE _tgrp5 (
                grp   integer,
                price float8,
                qty   integer
            ) USING colcompress;
            INSERT INTO _tgrp5
            SELECT
                i % 5,
                (random() * 100)::float8,
                (random() * 10)::integer
            FROM generate_series(1, 100000) i;
            ANALYZE _tgrp5;
        """)
        expr_sql = ("SELECT grp, SUM(price * qty) AS revenue "
                    "FROM _tgrp5 GROUP BY grp ORDER BY grp")
        r_off10 = self.q(f"{pfx_off} {expr_sql}")
        r_on10  = self.q(f"{pfx_on} SET max_parallel_workers_per_gather=0; {expr_sql}")
        self.check("SUM(price*qty): VEC ON == VEC OFF",
                   r_off10 == r_on10, f"OFF={r_off10!r}  ON={r_on10!r}")

        # 13. SUM(arithmetic expression) — SUM(a + b) integer columns
        self.exec("""
            DROP TABLE IF EXISTS _tgrp6;
            CREATE TABLE _tgrp6 (
                grp  integer,
                a    integer,
                b    integer
            ) USING colcompress;
            INSERT INTO _tgrp6
            SELECT
                i % 4,
                (random() * 1000)::integer,
                (random() * 500)::integer
            FROM generate_series(1, 100000) i;
            ANALYZE _tgrp6;
        """)
        expr2_sql = ("SELECT grp, SUM(a + b) AS total "
                     "FROM _tgrp6 GROUP BY grp ORDER BY grp")
        r_off11 = self.q(f"{pfx_off} {expr2_sql}")
        r_on11  = self.q(f"{pfx_on} SET max_parallel_workers_per_gather=0; {expr2_sql}")
        self.check("SUM(a+b): VEC ON == VEC OFF",
                   r_off11 == r_on11, f"OFF={r_off11!r}  ON={r_on11!r}")

    # ------------------------------------------------------------------ maintenance API (Phase 1)

    def test_maintenance_api(self) -> None:
        self.section("Maintenance API — colcompress_set_maintenance / rowcompress_set_maintenance")

        self.exec("""
            DROP TABLE IF EXISTS _tmaint_col;
            DROP TABLE IF EXISTS _tmaint_row;
            CREATE TABLE _tmaint_col (id int, val text) USING colcompress;
            CREATE TABLE _tmaint_row (id int, val text) USING rowcompress;
            INSERT INTO _tmaint_col SELECT i, 'x' FROM generate_series(1,1000) i;
            INSERT INTO _tmaint_row SELECT i, 'x' FROM generate_series(1,1000) i;
        """)

        # --- colcompress_set_maintenance ---
        rc, err = self.exec(
            "SELECT engine.colcompress_set_maintenance('_tmaint_col', 'lazy', 0.80, 0.10)"
        )
        self.check("colcompress_set_maintenance: no error", rc == 0, err[:200] if err else "")

        mode = self.q1(
            "SELECT maintenance_mode FROM engine.col_maintenance_options "
            "WHERE regclass = '_tmaint_col'::regclass"
        )
        self.check_eq("colcompress_set_maintenance: mode='lazy' persisted", mode, "lazy")

        target = self.q1(
            "SELECT maintenance_target_pruning_ratio FROM engine.col_maintenance_options "
            "WHERE regclass = '_tmaint_col'::regclass"
        )
        self.check_eq("colcompress_set_maintenance: target_pruning=0.8 persisted",
                      target, "0.8")

        merge = self.q1(
            "SELECT maintenance_merge_trigger_ratio FROM engine.col_maintenance_options "
            "WHERE regclass = '_tmaint_col'::regclass"
        )
        self.check_eq("colcompress_set_maintenance: merge_trigger=0.1 persisted",
                      merge, "0.1")

        # UPSERT: re-call updates existing row
        self.exec(
            "SELECT engine.colcompress_set_maintenance('_tmaint_col', 'eager', 0.70, 0.20)"
        )
        mode2 = self.q1(
            "SELECT maintenance_mode FROM engine.col_maintenance_options "
            "WHERE regclass = '_tmaint_col'::regclass"
        )
        self.check_eq("colcompress_set_maintenance: UPSERT updates existing row", mode2, "eager")

        cnt_rows = self.q1(
            "SELECT count(*) FROM engine.col_maintenance_options "
            "WHERE regclass = '_tmaint_col'::regclass"
        )
        self.check_eq("col_maintenance_options: exactly one row per table", cnt_rows, "1")

        # wrong AM: colcompress_set_maintenance on rowcompress table should fail
        rc_bad, err_bad = self.exec(
            "SELECT engine.colcompress_set_maintenance('_tmaint_row', 'eager')"
        )
        self.check("colcompress_set_maintenance on rowcompress table: raises error",
                   rc_bad != 0, f"expected error, got rc={rc_bad}")

        # validation: invalid mode
        rc_inv, err_inv = self.exec(
            "SELECT engine.colcompress_set_maintenance('_tmaint_col', 'turbo')"
        )
        self.check("colcompress_set_maintenance: invalid mode raises error",
                   rc_inv != 0, f"expected error, got rc={rc_inv}")

        # validation: ratio > 1.0
        rc_r, err_r = self.exec(
            "SELECT engine.colcompress_set_maintenance('_tmaint_col', 'eager', 1.5)"
        )
        self.check("colcompress_set_maintenance: target_pruning > 1.0 raises error",
                   rc_r != 0, f"expected error, got rc={rc_r}")

        # validation: merge_trigger > target_pruning
        rc_t, err_t = self.exec(
            "SELECT engine.colcompress_set_maintenance('_tmaint_col', 'eager', 0.50, 0.80)"
        )
        self.check("colcompress_set_maintenance: merge_trigger > target raises error",
                   rc_t != 0, f"expected error, got rc={rc_t}")

        # --- rowcompress_set_maintenance ---
        rc2, err2 = self.exec(
            "SELECT engine.rowcompress_set_maintenance('_tmaint_row', 'lazy', 0.60, 0.15)"
        )
        self.check("rowcompress_set_maintenance: no error", rc2 == 0, err2[:200] if err2 else "")

        mode3 = self.q1(
            "SELECT maintenance_mode FROM engine.row_maintenance_options "
            "WHERE regclass = '_tmaint_row'::regclass"
        )
        self.check_eq("rowcompress_set_maintenance: mode='lazy' persisted", mode3, "lazy")

        # wrong AM: rowcompress_set_maintenance on colcompress table should fail
        rc_bad2, _ = self.exec(
            "SELECT engine.rowcompress_set_maintenance('_tmaint_col', 'eager')"
        )
        self.check("rowcompress_set_maintenance on colcompress table: raises error",
                   rc_bad2 != 0, f"expected error, got rc={rc_bad2}")

        # storage_health reflects configured maintenance_mode
        sh_mode = self.q1(
            "SELECT maintenance_mode FROM engine.storage_health "
            "WHERE table_name = '_tmaint_col'"
        )
        self.check_eq("storage_health reflects colcompress maintenance_mode=eager",
                      sh_mode, "eager")

    # ------------------------------------------------------------------ storage_health (Phase 2/2.5)

    def test_storage_health(self) -> None:
        self.section("storage_health View — dirty_units, tombstone_rows, recommended_action")

        # --- colcompress ---
        self.exec("""
            DROP TABLE IF EXISTS _thealth_col;
            CREATE TABLE _thealth_col (id int, val text) USING colcompress;
            INSERT INTO _thealth_col SELECT i, repeat('x', 100) FROM generate_series(1,10000) i;
        """)

        # fresh table: all clean
        dirty_u = self.q1(
            "SELECT dirty_units FROM engine.storage_health "
            "WHERE table_name = '_thealth_col'"
        )
        self.check_eq("colcompress fresh: dirty_units = 0", dirty_u, "0")

        tomb0 = self.q1(
            "SELECT tombstone_rows FROM engine.storage_health "
            "WHERE table_name = '_thealth_col'"
        )
        self.check_eq("colcompress fresh: tombstone_rows = 0", tomb0, "0")

        act_ok = self.q1(
            "SELECT recommended_action FROM engine.storage_health "
            "WHERE table_name = '_thealth_col'"
        )
        self.check_eq("colcompress fresh: recommended_action = ok", act_ok, "ok")

        # delete some rows → stripe becomes dirty
        self.exec("DELETE FROM _thealth_col WHERE id <= 100")

        dirty_after = self.q1(
            "SELECT dirty_units FROM engine.storage_health "
            "WHERE table_name = '_thealth_col'"
        )
        self.check("colcompress after DELETE: dirty_units > 0",
                   dirty_after.isdigit() and int(dirty_after) > 0,
                   f"got {dirty_after!r}")

        tomb_after = self.q1(
            "SELECT tombstone_rows FROM engine.storage_health "
            "WHERE table_name = '_thealth_col'"
        )
        self.check("colcompress after DELETE: tombstone_rows > 0",
                   tomb_after.isdigit() and int(tomb_after) > 0,
                   f"got {tomb_after!r}")

        live_after = self.q1(
            "SELECT live_rows FROM engine.storage_health "
            "WHERE table_name = '_thealth_col'"
        )
        self.check_eq("colcompress after DELETE: live_rows = 9900", live_after, "9900")

        total_u = self.q1(
            "SELECT total_units FROM engine.storage_health "
            "WHERE table_name = '_thealth_col'"
        )
        self.check("colcompress after DELETE: total_units >= dirty_units",
                   total_u.isdigit() and dirty_after.isdigit()
                   and int(total_u) >= int(dirty_after),
                   f"total={total_u!r} dirty={dirty_after!r}")

        # --- rowcompress ---
        self.exec("""
            DROP TABLE IF EXISTS _thealth_row;
            CREATE TABLE _thealth_row (id int, val text) USING rowcompress;
            INSERT INTO _thealth_row SELECT i, repeat('y', 100) FROM generate_series(1,10000) i;
        """)

        # fresh
        dirty_r0 = self.q1(
            "SELECT dirty_units FROM engine.storage_health "
            "WHERE table_name = '_thealth_row'"
        )
        self.check_eq("rowcompress fresh: dirty_units = 0", dirty_r0, "0")

        tomb_r0 = self.q1(
            "SELECT tombstone_rows FROM engine.storage_health "
            "WHERE table_name = '_thealth_row'"
        )
        self.check_eq("rowcompress fresh: tombstone_rows = 0", tomb_r0, "0")

        act_r_ok = self.q1(
            "SELECT recommended_action FROM engine.storage_health "
            "WHERE table_name = '_thealth_row'"
        )
        self.check_eq("rowcompress fresh: recommended_action = ok", act_r_ok, "ok")

        # delete some rows
        self.exec("DELETE FROM _thealth_row WHERE id <= 200")

        dirty_r1 = self.q1(
            "SELECT dirty_units FROM engine.storage_health "
            "WHERE table_name = '_thealth_row'"
        )
        self.check("rowcompress after DELETE: dirty_units > 0",
                   dirty_r1.isdigit() and int(dirty_r1) > 0,
                   f"got {dirty_r1!r}")

        tomb_r1 = self.q1(
            "SELECT tombstone_rows FROM engine.storage_health "
            "WHERE table_name = '_thealth_row'"
        )
        self.check("rowcompress after DELETE: tombstone_rows > 0",
                   tomb_r1.isdigit() and int(tomb_r1) > 0,
                   f"got {tomb_r1!r}")

        live_r1 = self.q1(
            "SELECT live_rows FROM engine.storage_health "
            "WHERE table_name = '_thealth_row'"
        )
        self.check_eq("rowcompress after DELETE: live_rows = 9800", live_r1, "9800")

        # recommended_action escalation: set thresholds so merge is triggered
        # dirty_units > 0 with merge_trigger=0.0 (every dirty unit triggers)
        self.exec(
            "SELECT engine.rowcompress_set_maintenance('_thealth_row', 'eager', 0.70, 0.00)"
        )
        act_merge = self.q1(
            "SELECT recommended_action FROM engine.storage_health "
            "WHERE table_name = '_thealth_row'"
        )
        self.check("rowcompress: recommended_action=run_incremental_merge when trigger_ratio=0",
                   act_merge in ("run_incremental_merge", "run_full_repack"),
                   f"got {act_merge!r}")

        # restore default
        self.exec(
            "SELECT engine.rowcompress_set_maintenance('_thealth_row', 'eager', 0.70, 0.20)"
        )

        # storage_maintenance_recommendation returns a row
        rec = self.q(
            "SELECT status FROM engine.storage_maintenance_recommendation('_thealth_col'::regclass)"
        )
        self.check("storage_maintenance_recommendation returns a row for colcompress",
                   len(rec) > 0, f"got {rec!r}")

        rec_row = self.q(
            "SELECT status FROM engine.storage_maintenance_recommendation('_thealth_row'::regclass)"
        )
        self.check("storage_maintenance_recommendation returns a row for rowcompress",
                   len(rec_row) > 0, f"got {rec_row!r}")

        # storage_maintenance_stats returns am_name column
        stats = self.q1(
            "SELECT am_name FROM engine.storage_maintenance_stats('_thealth_col'::regclass)"
        )
        self.check_eq("storage_maintenance_stats: am_name=colcompress", stats, "colcompress")

        stats_row = self.q1(
            "SELECT am_name FROM engine.storage_maintenance_stats('_thealth_row'::regclass)"
        )
        self.check_eq("storage_maintenance_stats: am_name=rowcompress", stats_row, "rowcompress")

        # stripe.pruning_valid directly: at least one false after delete
        pv_false = self.q1(
            "SELECT count(*) FROM engine.stripe s "
            "JOIN engine.col_options co ON co.regclass = '_thealth_col'::regclass "
            "WHERE s.storage_id = engine.colcompress_relation_storageid(co.regclass) "
            "  AND NOT s.pruning_valid"
        )
        self.check("stripe.pruning_valid=false after DELETE",
                   pv_false.isdigit() and int(pv_false) > 0,
                   f"got {pv_false!r}")

        # row_batch.pruning_valid directly: at least one false after delete
        pv_row_false = self.q1(
            "SELECT count(*) FROM engine.row_batch rb "
            "JOIN engine.row_options ro ON ro.regclass = '_thealth_row'::regclass "
            "WHERE rb.storage_id = engine.rowcompress_relation_storageid(ro.regclass) "
            "  AND NOT rb.pruning_valid"
        )
        self.check("row_batch.pruning_valid=false after DELETE",
                   pv_row_false.isdigit() and int(pv_row_false) > 0,
                   f"got {pv_row_false!r}")

    # ------------------------------------------------------------------ colcompress_merge_incremental (Phase 3)

    def test_colcompress_merge_incremental(self) -> None:
        self.section("Phase 3 — colcompress_merge_incremental")

        self.exec("""
            DROP TABLE IF EXISTS _tmerge_col;
            CREATE TABLE _tmerge_col (id int, val text) USING colcompress;
            INSERT INTO _tmerge_col SELECT i, repeat('col', 50) FROM generate_series(1,50000) i;
        """)

        # total before
        cnt_before = self.q1("SELECT count(*) FROM _tmerge_col")
        self.check_eq("colcompress merge: 50000 rows before any delete", cnt_before, "50000")

        # clean table: merge is a no-op (idempotent)
        rc_noop, err_noop = self.exec("CALL engine.colcompress_merge_incremental('_tmerge_col')")
        self.check("colcompress_merge_incremental on clean table: no error",
                   rc_noop == 0, err_noop[:200] if err_noop else "")

        dirty_clean = self.q1(
            "SELECT dirty_units FROM engine.storage_health "
            "WHERE table_name = '_tmerge_col'"
        )
        self.check_eq("colcompress merge on clean: dirty_units still 0", dirty_clean, "0")

        # delete rows to dirty stripes
        self.exec("DELETE FROM _tmerge_col WHERE id BETWEEN 1 AND 500")

        dirty_pre = self.q1(
            "SELECT dirty_units FROM engine.storage_health "
            "WHERE table_name = '_tmerge_col'"
        )
        self.check("colcompress after DELETE: has dirty_units before merge",
                   dirty_pre.isdigit() and int(dirty_pre) > 0,
                   f"got {dirty_pre!r}")

        # run merge
        rc_merge, err_merge = self.exec(
            "CALL engine.colcompress_merge_incremental('_tmerge_col')"
        )
        self.check("colcompress_merge_incremental: no error", rc_merge == 0,
                   err_merge[:200] if err_merge else "")

        # dirty_units must be 0 after merge
        dirty_post = self.q1(
            "SELECT dirty_units FROM engine.storage_health "
            "WHERE table_name = '_tmerge_col'"
        )
        self.check_eq("colcompress after merge: dirty_units = 0", dirty_post, "0")

        # tombstone_rows must be 0 after merge
        tomb_post = self.q1(
            "SELECT tombstone_rows FROM engine.storage_health "
            "WHERE table_name = '_tmerge_col'"
        )
        self.check_eq("colcompress after merge: tombstone_rows = 0", tomb_post, "0")

        # live_rows preserved (500 deleted, 49500 remain)
        cnt_after = self.q1("SELECT count(*) FROM _tmerge_col")
        self.check_eq("colcompress after merge: live_rows = 49500", cnt_after, "49500")

        # data correctness: specific row survives
        spot = self.q1("SELECT val FROM _tmerge_col WHERE id = 50000")
        self.check_eq("colcompress after merge: data correct (id=50000 readable)",
                      spot, "col" * 50)

        # deleted rows are gone
        cnt_deleted = self.q1("SELECT count(*) FROM _tmerge_col WHERE id BETWEEN 1 AND 500")
        self.check_eq("colcompress after merge: deleted rows are gone", cnt_deleted, "0")

        # idempotent: second call is a no-op
        rc_idem, _ = self.exec("CALL engine.colcompress_merge_incremental('_tmerge_col')")
        self.check("colcompress_merge_incremental: idempotent (no error on re-call)",
                   rc_idem == 0)

        dirty_idem = self.q1(
            "SELECT dirty_units FROM engine.storage_health "
            "WHERE table_name = '_tmerge_col'"
        )
        self.check_eq("colcompress_merge_incremental: dirty_units=0 after idempotent call",
                      dirty_idem, "0")

        # fully-dead stripe: delete all rows in a stripe, merge should clean metadata only
        self.exec("""
            DROP TABLE IF EXISTS _tmerge_col_dead;
            CREATE TABLE _tmerge_col_dead (id int, val text) USING colcompress;
            INSERT INTO _tmerge_col_dead SELECT i, 'dead' FROM generate_series(1,10000) i;
        """)
        # delete everything (all stripes become fully dead)
        self.exec("DELETE FROM _tmerge_col_dead")

        total_dead = self.q1(
            "SELECT total_units FROM engine.storage_health "
            "WHERE table_name = '_tmerge_col_dead'"
        )
        self.check("colcompress fully-dead: total_units > 0 before merge",
                   total_dead.isdigit() and int(total_dead) > 0,
                   f"got {total_dead!r}")

        rc_dead, err_dead = self.exec(
            "CALL engine.colcompress_merge_incremental('_tmerge_col_dead')"
        )
        self.check("colcompress fully-dead stripe: merge succeeds", rc_dead == 0,
                   err_dead[:200] if err_dead else "")

        dirty_dead_post = self.q1(
            "SELECT dirty_units FROM engine.storage_health "
            "WHERE table_name = '_tmerge_col_dead'"
        )
        self.check_eq("colcompress fully-dead: dirty_units = 0 after merge",
                      dirty_dead_post, "0")

        cnt_dead_post = self.q1("SELECT count(*) FROM _tmerge_col_dead")
        self.check_eq("colcompress fully-dead: table is empty after merge",
                      cnt_dead_post, "0")

        # error: non-colcompress table
        self.exec("""
            DROP TABLE IF EXISTS _tmerge_col_not;
            CREATE TABLE _tmerge_col_not (id int) USING rowcompress;
        """)
        rc_err, _ = self.exec(
            "CALL engine.colcompress_merge_incremental('_tmerge_col_not')"
        )
        self.check("colcompress_merge_incremental: error on non-colcompress table",
                   rc_err != 0)

        # error: max_stripes = 0
        rc_err2, _ = self.exec(
            "CALL engine.colcompress_merge_incremental('_tmerge_col', max_stripes => 0)"
        )
        self.check("colcompress_merge_incremental: error on max_stripes=0", rc_err2 != 0)

    # ------------------------------------------------------------------ rowcompress_merge_incremental (Phase 3)

    def test_rowcompress_merge_incremental(self) -> None:
        self.section("Phase 3 — rowcompress_merge_incremental")

        self.exec("""
            DROP TABLE IF EXISTS _tmerge_row;
            CREATE TABLE _tmerge_row (id int, val text) USING rowcompress;
            INSERT INTO _tmerge_row SELECT i, repeat('row', 50) FROM generate_series(1,50000) i;
        """)

        cnt_before = self.q1("SELECT count(*) FROM _tmerge_row")
        self.check_eq("rowcompress merge: 50000 rows before any delete", cnt_before, "50000")

        # clean table: merge is a no-op
        rc_noop, err_noop = self.exec("CALL engine.rowcompress_merge_incremental('_tmerge_row')")
        self.check("rowcompress_merge_incremental on clean table: no error",
                   rc_noop == 0, err_noop[:200] if err_noop else "")

        dirty_clean = self.q1(
            "SELECT dirty_units FROM engine.storage_health "
            "WHERE table_name = '_tmerge_row'"
        )
        self.check_eq("rowcompress merge on clean: dirty_units = 0", dirty_clean, "0")

        # delete rows to dirty batches
        self.exec("DELETE FROM _tmerge_row WHERE id BETWEEN 1 AND 300")

        dirty_pre = self.q1(
            "SELECT dirty_units FROM engine.storage_health "
            "WHERE table_name = '_tmerge_row'"
        )
        self.check("rowcompress after DELETE: has dirty_units before merge",
                   dirty_pre.isdigit() and int(dirty_pre) > 0,
                   f"got {dirty_pre!r}")

        tomb_pre = self.q1(
            "SELECT tombstone_rows FROM engine.storage_health "
            "WHERE table_name = '_tmerge_row'"
        )
        self.check("rowcompress after DELETE: tombstone_rows > 0 before merge",
                   tomb_pre.isdigit() and int(tomb_pre) > 0,
                   f"got {tomb_pre!r}")

        # run merge
        rc_merge, err_merge = self.exec(
            "CALL engine.rowcompress_merge_incremental('_tmerge_row')"
        )
        self.check("rowcompress_merge_incremental: no error", rc_merge == 0,
                   err_merge[:200] if err_merge else "")

        # dirty_units = 0 after merge
        dirty_post = self.q1(
            "SELECT dirty_units FROM engine.storage_health "
            "WHERE table_name = '_tmerge_row'"
        )
        self.check_eq("rowcompress after merge: dirty_units = 0", dirty_post, "0")

        # live_rows preserved (300 deleted, 49700 remain)
        cnt_after = self.q1("SELECT count(*) FROM _tmerge_row")
        self.check_eq("rowcompress after merge: live_rows = 49700", cnt_after, "49700")

        # data correctness: specific row readable
        spot = self.q1("SELECT val FROM _tmerge_row WHERE id = 50000")
        self.check_eq("rowcompress after merge: data correct (id=50000 readable)",
                      spot, "row" * 50)

        # deleted rows are gone
        cnt_deleted = self.q1("SELECT count(*) FROM _tmerge_row WHERE id BETWEEN 1 AND 300")
        self.check_eq("rowcompress after merge: deleted rows are gone", cnt_deleted, "0")

        # idempotent
        rc_idem, _ = self.exec("CALL engine.rowcompress_merge_incremental('_tmerge_row')")
        self.check("rowcompress_merge_incremental: idempotent (no error on re-call)",
                   rc_idem == 0)

        dirty_idem = self.q1(
            "SELECT dirty_units FROM engine.storage_health "
            "WHERE table_name = '_tmerge_row'"
        )
        self.check_eq("rowcompress_merge_incremental: dirty_units=0 after idempotent call",
                      dirty_idem, "0")

        # MVCC correctness: merged batch should still return only live rows
        cnt_live = self.q1(
            "SELECT count(*) FROM _tmerge_row WHERE id BETWEEN 301 AND 50000"
        )
        self.check_eq("rowcompress after merge: MVCC — 49700 live rows accessible",
                      cnt_live, "49700")

        # error: non-rowcompress table
        self.exec("""
            DROP TABLE IF EXISTS _tmerge_row_not;
            CREATE TABLE _tmerge_row_not (id int) USING colcompress;
        """)
        rc_err, _ = self.exec(
            "CALL engine.rowcompress_merge_incremental('_tmerge_row_not')"
        )
        self.check("rowcompress_merge_incremental: error on non-rowcompress table",
                   rc_err != 0)

        # error: max_batches = 0
        rc_err2, _ = self.exec(
            "CALL engine.rowcompress_merge_incremental('_tmerge_row', max_batches => 0)"
        )
        self.check("rowcompress_merge_incremental: error on max_batches=0", rc_err2 != 0)

    # ------------------------------------------------------------------ rowcompress scan stats

    def test_rowcompress_scan_stats(self) -> None:
        self.section("rowcompress_scan_stats — session-local scan statistics")

        # Create and populate two rowcompress tables.
        self.exec("""
            DROP TABLE IF EXISTS _tscanstat;
            DROP TABLE IF EXISTS _tscanstat2;
            CREATE TABLE _tscanstat  (id int, val text) USING rowcompress;
            CREATE TABLE _tscanstat2 (id int)          USING rowcompress;
            INSERT INTO _tscanstat  SELECT i, 'row' || i FROM generate_series(1, 5000) i;
            INSERT INTO _tscanstat2 SELECT i            FROM generate_series(1, 1000) i;
        """)

        # All assertions below must run in a SINGLE psql session because
        # rowcompress_scan_stats() is session-local.
        #
        # The query returns one result row per assertion (in order).
        # With psql -A -t each row lands on its own output line.
        # self.q() strips SET lines and returns newline-joined remaining lines.
        out = self.q("""
            -- start clean
            SELECT engine.rowcompress_reset_scan_stats();

            -- one scan of _tscanstat
            SELECT count(*) FROM _tscanstat;

            -- assertion 1: total_scans = 1
            SELECT total_scans
              FROM engine.rowcompress_scan_stats()
             WHERE table_name = '_tscanstat';

            -- assertion 2: batches_scanned = batches_total - batches_pruned
            SELECT bool_and(batches_scanned = batches_total - batches_pruned)
              FROM engine.rowcompress_scan_stats();

            -- assertion 3: pruning_ratio in [0,1]
            SELECT bool_and(pruning_ratio >= 0.0 AND pruning_ratio <= 1.0)
              FROM engine.rowcompress_scan_stats();

            -- second scan
            SELECT count(*) FROM _tscanstat;

            -- assertion 4: total_scans = 2
            SELECT total_scans
              FROM engine.rowcompress_scan_stats()
             WHERE table_name = '_tscanstat';

            -- scan _tscanstat2, then reset _tscanstat only
            SELECT count(*) FROM _tscanstat2;
            SELECT engine.rowcompress_reset_scan_stats('_tscanstat'::regclass);

            -- assertion 5: _tscanstat is gone (0 rows)
            SELECT count(*)
              FROM engine.rowcompress_scan_stats()
             WHERE table_name = '_tscanstat';

            -- assertion 6: _tscanstat2 is still there (1 row)
            SELECT count(*)
              FROM engine.rowcompress_scan_stats()
             WHERE table_name = '_tscanstat2';

            -- global reset
            SELECT engine.rowcompress_reset_scan_stats();

            -- assertion 7: everything cleared
            SELECT count(*) FROM engine.rowcompress_scan_stats()
        """)

        # Strip psql procedural output (blank rows from void-returning functions,
        # count(*) rows we only ran to produce stats, etc.) and collect non-empty
        # non-SET lines into an ordered list.
        lines = [ln for ln in out.split("\n") if ln and ln != "SET"]

        # Expected ordering of meaningful output lines:
        #  0: 5000          (count _tscanstat scan 1)
        #  1: 1             (total_scans assertion 1)
        #  2: t             (batches_ok assertion 2)
        #  3: t             (ratio_ok assertion 3)
        #  4: 5000          (count _tscanstat scan 2)
        #  5: 2             (total_scans assertion 4)
        #  6: 1000          (count _tscanstat2)
        #  7: 0             (assertion 5: _tscanstat gone)
        #  8: 1             (assertion 6: _tscanstat2 still there)
        #  9: 0             (assertion 7: all gone)

        def _line(idx: int) -> str:
            return lines[idx] if idx < len(lines) else ""

        self.check_eq("scan_stats: total_scans = 1 after one scan",          _line(1), "1")
        self.check("scan_stats: batches_scanned = batches_total - batches_pruned",
                   _line(2) == "t", f"got {_line(2)!r}")
        self.check("scan_stats: pruning_ratio in [0,1]",
                   _line(3) == "t", f"got {_line(3)!r}")
        self.check_eq("scan_stats: total_scans = 2 after second scan",       _line(5), "2")
        self.check_eq("scan_stats: table-specific reset removes that entry",  _line(7), "0")
        self.check("scan_stats: table-specific reset leaves other entries",
                   _line(8).isdigit() and int(_line(8)) > 0, f"got {_line(8)!r}")
        self.check_eq("scan_stats: global reset clears all entries",          _line(9), "0")

    # ------------------------------------------------------------------ upgrade path

    def test_upgrade_path(self) -> None:
        self.section("Upgrade Path Chain")

        # Latest version available
        ver = self.q1(
            "SELECT max(version) FROM pg_available_extension_versions "
            "WHERE name = 'storage_engine'"
        )
        self.check("latest available version = 2.2.0", ver == "2.2.0", f"got {ver!r}")

        # Complete upgrade path from 1.0 to 2.2.0 exists
        path = self.q1(
            "SELECT path FROM pg_extension_update_paths('storage_engine') "
            "WHERE source = '1.0' AND target = '2.2.0'"
        )
        self.check("upgrade path 1.0 → 2.2.0 exists", path != "", f"path={path!r}")

        # Each individual upgrade step
        steps = [
            ("1.0",   "1.1"),
            ("1.1",   "1.2.0"),
            ("1.2.0", "1.2.1"),
            ("1.2.1", "1.2.2"),
            ("1.2.2", "1.2.3"),
            ("1.2.3", "1.2.4"),
            ("1.2.4", "1.2.5"),
            ("1.2.5", "1.2.6"),
            ("1.2.6", "1.2.7"),
            ("1.2.7", "1.2.8"),
            ("1.2.8", "1.2.9"),
            ("1.2.9", "1.3.0"),
            ("1.3.0", "1.3.1"),
            ("1.3.1", "1.3.2"),
            ("1.3.2", "1.3.3"),
            ("1.3.3", "1.3.4"),
            ("1.3.4", "2.0.0"),
            ("2.0.0", "2.0.1"),
            ("2.0.1", "2.1.0"),
            ("2.1.0", "2.2.0"),
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
        self.test_vecgroupagg()
        self.test_maintenance_api()
        self.test_storage_health()
        self.test_colcompress_merge_incremental()
        self.test_rowcompress_merge_incremental()
        self.test_rowcompress_scan_stats()
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
    parser.add_argument("--ports",    type=str, default=None,
                        help="Comma-separated list of PostgreSQL ports to test "
                             "(overrides --port / --pg19, e.g. 5436,5434,5435,5432,5433)")
    args = parser.parse_args()

    all_ok = True

    if args.ports:
        ports = [int(p.strip()) for p in args.ports.split(",") if p.strip()]
        for port in ports:
            r = TestRunner(port, label=f"PG@{port}")
            all_ok = r.run_all() and all_ok
    else:
        r1 = TestRunner(args.port, label=f"PG@{args.port}")
        all_ok = r1.run_all() and all_ok

        if args.pg19:
            r2 = TestRunner(args.pg19_port, label=f"PG@{args.pg19_port}")
            all_ok = r2.run_all() and all_ok

    sys.exit(0 if all_ok else 1)


if __name__ == "__main__":
    main()
