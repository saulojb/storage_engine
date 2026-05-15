ch: tpch-ch.sql
	clickhouse client --queries-file $<

pg: tpch-pg.sql
	createdb tpch
	psql --quiet --no-psqlrc --set ON_ERROR_STOP=1 -d tpch -f $<

run: run.sh
	./$<

clean:
	rm -rf result

links:
	@for i in {1..22}; do printf '  [Query %s] https://github.com/ClickHouse/pg_clickhouse/blob/main/dev/tpch/queries/%s.sql\n' "$$i" "$$i"; done
