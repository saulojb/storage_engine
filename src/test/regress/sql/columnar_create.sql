--
-- Test the CREATE statements related to columnar.
--

-- Create uncompressed table
CREATE TABLE contestant (handle TEXT, birthdate DATE, rating INT,
	percentile FLOAT, country CHAR(3), achievements TEXT[])
	USING columnar;
SELECT columnar.alter_engine_table_set('contestant', compression => 'none');

CREATE INDEX contestant_idx on contestant(handle);

-- Create zstd compressed table
CREATE TABLE contestant_compressed (handle TEXT, birthdate DATE, rating INT,
	percentile FLOAT, country CHAR(3), achievements TEXT[])
	USING columnar;

-- Test that querying an empty table works
ANALYZE contestant;
SELECT count(*) FROM contestant;

-- Should fail: unlogged tables not supported
CREATE UNLOGGED TABLE engine_unlogged(i int) USING columnar;

CREATE TABLE engine_table_1 (a int) USING columnar;
INSERT INTO engine_table_1 VALUES (1);

CREATE MATERIALIZED VIEW engine_table_1_mv USING columnar
AS SELECT * FROM engine_table_1;

SELECT engine_test_helpers.engine_relation_storageid(oid) AS engine_table_1_mv_storage_id
FROM pg_class WHERE relname='engine_table_1_mv' \gset

-- test engine_relation_set_new_filenode
REFRESH MATERIALIZED VIEW engine_table_1_mv;
SELECT engine_test_helpers.engine_metadata_has_storage_id(:engine_table_1_mv_storage_id);

SELECT engine_test_helpers.engine_relation_storageid(oid) AS engine_table_1_storage_id
FROM pg_class WHERE relname='engine_table_1' \gset

BEGIN;
  -- test engine_relation_nontransactional_truncate
  TRUNCATE engine_table_1;
  SELECT engine_test_helpers.engine_metadata_has_storage_id(:engine_table_1_storage_id);
ROLLBACK;

-- since we rollback'ed above xact, should return true
SELECT engine_test_helpers.engine_metadata_has_storage_id(:engine_table_1_storage_id);

-- test dropping columnar table
DROP TABLE engine_table_1 CASCADE;
SELECT engine_test_helpers.engine_metadata_has_storage_id(:engine_table_1_storage_id);

-- test temporary columnar tables

-- Should work: temporary tables are supported
CREATE TEMPORARY TABLE engine_temp(i int) USING columnar;

-- reserve some chunks and a stripe
INSERT INTO engine_temp SELECT i FROM generate_series(1,5) i;

SELECT engine_test_helpers.engine_relation_storageid(oid) AS engine_temp_storage_id
FROM pg_class WHERE relname='engine_temp' \gset

SELECT pg_backend_pid() AS val INTO old_backend_pid;

\c - - - -

-- wait until old backend to expire to make sure that temp table cleanup is complete
SELECT engine_test_helpers.pg_waitpid(val) FROM old_backend_pid;

DROP TABLE old_backend_pid;

-- show that temporary table itself and it's metadata is removed
SELECT COUNT(*)=0 FROM pg_class WHERE relname='engine_temp';
SELECT engine_test_helpers.engine_metadata_has_storage_id(:engine_temp_storage_id);

-- connect to another session and create a temp table with same name
CREATE TEMPORARY TABLE engine_temp(i int) USING columnar;

-- reserve some chunks and a stripe
INSERT INTO engine_temp SELECT i FROM generate_series(1,5) i;

-- test basic select
SELECT COUNT(*) FROM engine_temp WHERE i < 5;

SELECT engine_test_helpers.engine_relation_storageid(oid) AS engine_temp_storage_id
FROM pg_class WHERE relname='engine_temp' \gset

BEGIN;
  DROP TABLE engine_temp;
  -- show that we drop stripes properly
  SELECT engine_test_helpers.engine_metadata_has_storage_id(:engine_temp_storage_id);
ROLLBACK;

-- make sure that table is not dropped yet since we rollbacked above xact
SELECT COUNT(*)=1 FROM pg_class WHERE relname='engine_temp';
-- show that we preserve the stripe of the temp columanar table after rollback
SELECT engine_test_helpers.engine_metadata_has_storage_id(:engine_temp_storage_id);

-- drop it for next tests
DROP TABLE engine_temp;

BEGIN;
  CREATE TEMPORARY TABLE engine_temp(i int) USING columnar ON COMMIT DROP;
  -- force flushing stripe
  INSERT INTO engine_temp SELECT i FROM generate_series(1,150000) i;

  SELECT engine_test_helpers.engine_relation_storageid(oid) AS engine_temp_storage_id
  FROM pg_class WHERE relname='engine_temp' \gset
COMMIT;

-- make sure that table & it's stripe is dropped after commiting above xact
SELECT COUNT(*)=0 FROM pg_class WHERE relname='engine_temp';
SELECT engine_test_helpers.engine_metadata_has_storage_id(:engine_temp_storage_id);

BEGIN;
  CREATE TEMPORARY TABLE engine_temp(i int) USING columnar ON COMMIT DELETE ROWS;
  -- force flushing stripe
  INSERT INTO engine_temp SELECT i FROM generate_series(1,150000) i;

  SELECT engine_test_helpers.engine_relation_storageid(oid) AS engine_temp_storage_id
  FROM pg_class WHERE relname='engine_temp' \gset
COMMIT;

-- make sure that table is not dropped but it's rows's are deleted after commiting above xact
SELECT COUNT(*)=1 FROM pg_class WHERE relname='engine_temp';
SELECT COUNT(*)=0 FROM engine_temp;
-- since we deleted all the rows, we shouldn't have any stripes for table
SELECT engine_test_helpers.engine_metadata_has_storage_id(:engine_temp_storage_id);

-- make sure we can create a table from a table
CREATE TABLE sampletable (x numeric) using columnar;
INSERT INTO sampletable SELECT generate_series(1, 1000000, 1);
CREATE TABLE sampletable_columnar USING columnar AS SELECT * FROM sampletable ORDER BY 1 ASC;
DROP TABLE sampletable;
DROP TABLE sampletable_columnar;
