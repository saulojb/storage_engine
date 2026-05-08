---
layout: default
title: Installation
nav_order: 2
---

# Installation
{: .no_toc }

<details open markdown="block">
<summary>Table of contents</summary>
{: .text-delta }
1. TOC
{:toc}
</details>

---

## Requirements

- **PostgreSQL 16, 17, or 18** (server headers required)
- **C compiler** (`gcc` or `clang`)
- Optional compression libraries (strongly recommended):

| Library | Package (Debian/Ubuntu) | Benefit |
|---|---|---|
| **LZ4** | `liblz4-dev` | Very fast compression/decompression (~500 MB/s). Ideal for write-heavy workloads. |
| **ZSTD** ★ | `libzstd-dev` | Best ratio + good speed. **Strongly recommended** — saves 40–60% disk vs LZ4 with comparable read performance. |
| **libdeflate** | `libdeflate-dev` | zlib-compatible codec. Good middle ground between LZ4 and ZSTD. |
| **ZXC** | [build from source](https://github.com/hellobertrand/zxc) | Asymmetric codec: extremely fast decompression via SIMD (NEON/ARM64, AVX2-AVX512/x86_64). Excellent on ARM Graviton/Neoverse. |

Default compression precedence (first available wins): `ZSTD > ZXC > LZ4 > Deflate > pglz`

---

## Build from Source

### 1. Install build dependencies

**Ubuntu / Debian:**
```bash
sudo apt update
sudo apt install -y build-essential postgresql-server-dev-18 \
    liblz4-dev libzstd-dev

# Optional
# sudo apt install libdeflate-dev
```

Replace `postgresql-server-dev-18` with your PostgreSQL version (16, 17, or 18).

**RPM-based (dnf):**
```bash
sudo dnf install -y gcc make postgresql18-devel \
    lz4-devel libzstd-devel

# Optional
# sudo dnf install libdeflate-devel
```

### 2. Clone and build

```bash
git clone https://github.com/saulojb/storage_engine.git
cd storage_engine
sudo make -j$(nproc) install
```

If multiple PostgreSQL versions are installed, specify which one:

```bash
# Pass PG_CONFIG as a make argument (not an env var before sudo)
sudo make -j$(nproc) install PG_CONFIG=/usr/lib/postgresql/17/bin/pg_config
```

### 3. Configure PostgreSQL

Add to `postgresql.conf`:

```
shared_preload_libraries = 'storage_engine'
```

{: .important }
> `storage_engine` must appear **after** `citus` and `pg_cron` if they are also loaded:
> ```
> shared_preload_libraries = 'pg_cron,citus,storage_engine'
> ```
> `citus` must be the outermost planner hook; reversing the order prevents PostgreSQL from starting.

### 4. Restart and create extension

```bash
sudo systemctl restart postgresql
```

```sql
CREATE EXTENSION storage_engine;
```

---

## Upgrade

To upgrade from a previous version:

```bash
# Build and install new version
sudo make -j$(nproc) install

# Restart PostgreSQL, then run in each database that uses the extension:
ALTER EXTENSION storage_engine UPDATE;
```

Upgrade scripts (`storage_engine--X.Y.Z--A.B.C.sql`) are provided for all version pairs.

---

## Verify Installation

```sql
-- Check loaded version
SELECT extversion FROM pg_extension WHERE extname = 'storage_engine';

-- List available AMs
SELECT amname FROM pg_am WHERE amname IN ('colcompress', 'rowcompress');

-- Quick sanity check
CREATE TABLE _test (id int, val text) USING colcompress;
INSERT INTO _test VALUES (1, 'hello'), (2, 'world');
SELECT * FROM _test;
DROP TABLE _test;
```

---

## Configuration

Key GUCs after installation. All can be set globally in `postgresql.conf` or per-session.

```sql
-- Enable vectorized GROUP BY aggregation (default: on)
SET storage_engine.enable_vectorized_groupagg = on;

-- Enable parallel scan (default: on)
SET storage_engine.enable_parallel_execution = on;

-- Default compression codec (default: zstd)
SET storage_engine.compression = 'zstd';

-- Default compression level for zstd 1–19 (default: 3)
SET storage_engine.compression_level = 3;
```

See [Reference → GUCs](reference#configuration-gucs) for the full parameter list.
