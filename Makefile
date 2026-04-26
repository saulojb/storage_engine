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

.PHONY: all install clean installcheck
