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

.PHONY: all install clean
