# storage_engine toplevel Makefile
# Build:        sudo make -j$(nproc) install
# Custom PG:    sudo make -j$(nproc) install PG_CONFIG=/usr/lib/postgresql/15/bin/pg_config
# Note: do NOT use  PG_CONFIG=... sudo make  — sudo discards env vars by default.

PG_CONFIG ?= pg_config

ifeq (,$(shell $(PG_CONFIG) --version 2>/dev/null))
  $(error pg_config not found in PATH. Install postgresql-server-dev-XX or set PG_CONFIG=/path/to/pg_config)
endif

all install clean:
	$(MAKE) -C src/backend/engine $@ PG_CONFIG='$(PG_CONFIG)'

# Run the comprehensive test suite against the locally installed extension.
# Usage:
#   sudo make installcheck
#   sudo make installcheck PG_CONFIG=/usr/lib/postgresql/18/bin/pg_config
#   sudo make installcheck PG19=1   (also tests the secondary port 5433)
PG_PORT    ?= $(shell $(PG_CONFIG) --pgport 2>/dev/null || echo 5432)
PYTHON3    ?= python3
SUITE      := tests/test_suite.py
PG19_FLAG  := $(if $(PG19),--pg19,)

installcheck:
	$(PYTHON3) $(SUITE) --port $(PG_PORT) $(PG19_FLAG)

# Run the test suite against all installed PostgreSQL versions (15–19).
# Installs the extension for each version before running the tests.
installcheck-all:
	for ver in 15 16 17 18 19; do \
		if pg_lsclusters -h | awk '{print $$1}' | grep -qx "$$ver"; then \
			echo "=== Installing for PG$$ver ==="; \
			$(MAKE) -C src/backend/engine clean >/dev/null; \
			sudo $(MAKE) -C src/backend/engine install \
				PG_CONFIG=/usr/lib/postgresql/$$ver/bin/pg_config TMPDIR=$(TMPDIR); \
		fi; \
	done
	$(PYTHON3) $(SUITE) --ports 5436,5434,5435,5432,5433

.PHONY: all install clean installcheck installcheck-all
