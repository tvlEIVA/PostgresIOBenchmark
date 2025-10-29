# PostgreSQL IO Benchmarks (.NET 8 / C# 12)

This solution benchmarks different PostgreSQL data access/write patterns using Npgsql:
1. Simple inserts with varying transaction batch sizes.
2. Simple reads using a server-side cursor with varying FETCH sizes.
3. Multi-format inserts (10 DOUBLE PRECISION columns vs single packed BYTEA).
4. Point/attribute ingestion (three modes: per-row, multi-row statement, binary blob packing).

Generated results are written to CSV for later analysis.

## Projects

| Project | Scenario | Connection target |
|---------|----------|-------------------|
| PostgresIOBenchmark | Benchmarks against the Postgres container defined in docker-compose (internal) |
| PostgresBenchmarkLinuxExternal | Benchmarks from a Linux container to a Postgres instance on the host (external) |
| PostgresIOBenchmarkWindowsNative | Benchmarks from Windows directly to a locally installed Postgres (external) |
| PostgresBenchmarkCore | Shared benchmark implementations |

## Benchmarks & Output Columns

| Benchmark | CSV Columns | Description |
|-----------|-------------|-------------|
| PostgresSimpleInsertBenchmark | BatchSize,Seconds,RowsPerSec | INSERT into benchmarkdata with different transaction sizes |
| PostgresSimpleReadBenchmark | FetchSize,Seconds,RowsPerSec | Server-side cursor FETCH sizes vs throughput |
| PostgresMultiFormatInsertBenchmark | Mode,BatchSize,Seconds,RowsPerSec | 10 separate doubles vs packed blob row insertion |
| PostgresPointInsertBenchmark | Mode,Size,Seconds,PointsPerSec,AvgBytesPerPoint | Point ingestion: PointByPoint, BatchedRows, BinaryBlob |

File names may include the OS version (e.g. benchmark_simpleInsert_internalDatabase_{OS}.csv).

## Prerequisites

- .NET 8 SDK
- Docker Desktop (for container-based runs)
- PostgreSQL 15+ (external runs) with: user=postgres, password=test (or adjust), database=benchmarkdb
- Enable __Configuration: Release__ for more stable timing.

## Getting Started (Internal Container)

1. Start services:

docker compose up --build

- Exposes Postgres on host port 5437 (mapped to container 5432).
   - Internal benchmark uses Host=db (service name) with credentials: postgres/postgres.

2. Run from CLI (example):

dotnet run --project PostgresIOBenchmark

3. CSV files appear in the working directory.

## External (Host Postgres)

Ensure a Postgres instance listening on port 5432 with:

CREATE DATABASE benchmarkdb; ALTER USER postgres WITH PASSWORD 'test';

dotnet run --project PostgresIOBenchmarkWindowsNative

For Linux container to host:
- Start docker compose (only need the benchmark container if you already have host Postgres).
- Connection string uses Host=host.docker.internal.

docker compose run --rm postgresiobenchmarklinux

## Adjusting Workload Size

Each Program.cs exposes constants:
- totalRows (default 5000) for simple/multi-format insert/read
- totalPoints (default varies, e.g. 10_000 or 200_000 in Point benchmark)
- attributeCount (number of point attributes)
- Batch arrays (rowBatchSizes / blobGroupSizes). Modify to explore scaling behavior.

Increase totals for more reliable averages; keep runtime practical. Very small totals magnify startup jitter.

## Interpreting Results

- Simple Insert: Optimal batch balances round trips vs transaction overhead.
- Simple Read: FETCH size trades off memory vs network/chatiness.
- Multi-format Insert: Compare columnar vs packed binary per batch size.
- Point Insert: Evaluate ingestion strategies and bytes per point when packing.

## Performance Tips

- Use __Build > Configuration: Release__.
- Run multiple times; discard first (JIT warm-up).
- Pin CPU frequency / disable power saving when comparing machines.
- Avoid other heavy processes during measurement.
- Optionally increase totalRows/totalPoints for steadier throughput numbers.

## Extending

Add additional patterns (COPY, prepared statements, pipelines):
- Create new class in Postgres

Internal container benchmark
docker compose up --build dotnet run --project PostgresIOBenchmark
Windows native external
dotnet run --project PostgresIOBenchmarkWindowsNative
Linux container to host Postgres
docker compose run --rm postgresiobenchmarklinux
