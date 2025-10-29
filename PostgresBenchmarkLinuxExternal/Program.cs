using PostgresBenchmarkCore;

var connString = "Host=host.docker.internal;Port=5432;Username=postgres;Password=test;Database=benchmarkdb"; // Postgres on host machine from Linux container
var osVersion = Environment.OSVersion;

// Adjust totalRows if your system is very fast/slow
const int totalRows = 5000;

//await PostgresSimpleInsertBenchmark.Run(connString, $"benchmark_simpleInsert_externalDatabase_{osVersion}.csv", totalRows: totalRows);
//await PostgresSimpleReadBenchmark.Run(connString, $"benchmark_simpleRead_externalDatabase_{osVersion}.csv", totalRows: totalRows, populateFirst: true);
//await PostgresMultiFormatInsertBenchmark.Run(connString, $"benchmark_insert_formats_{osVersion}.csv", totalRows: totalRows);
await PostgresPointInsertBenchmark.Run(connString, "benchmark_points.csv",
    totalPoints: 10_000,
    attributeCount: 16,
    rowBatchSizes: new[] { 1, 25, 50, 100, 250, 500, 1000, 2000, 5000, 10000, 20000, 50000 },
    blobGroupSizes: new[] { 1, 25, 50, 100, 250, 500, 1000, 2000, 5000, 10000, 20000, 50000 });
