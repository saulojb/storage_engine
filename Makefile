# storage_engine toplevel Makefile
# Build:   sudo make -j$(nproc) install
# Custom:  PG_CONFIG=/path/to/pg_config sudo make install

PG_CONFIG ?= pg_config

ifeq (,$(shell $(PG_CONFIG) --version 2>/dev/null))
  $(error pg_config not found in PATH. Install postgresql-server-dev-XX or set PG_CONFIG=/path/to/pg_config)
endif

all install clean:
	$(MAKE) -C src/backend/engine $@ PG_CONFIG='$(PG_CONFIG)'

.PHONY: all install clean
