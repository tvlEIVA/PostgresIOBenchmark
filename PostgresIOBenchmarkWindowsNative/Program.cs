using PostgresBenchmarkCore;

string connString = "Host=localhost;Port=5432;Username=postgres;Password=test;Database=benchmarkdb"; // local

// Postgres on
// Windows
var osVersion = Environment.OSVersion;

// Adjust totalRows if your system is very fast/slow
const int totalRows = 10_000;

await PostgresSimpleInsertBenchmark.Run(
    connString,
    $"benchmark_simpleInsert_externalDatabase_{osVersion}.csv",
    totalRows: totalRows
);
await PostgresSimpleReadBenchmark.Run(
    connString,
    $"benchmark_simpleRead_externalDatabase_{osVersion}.csv",
    totalRows: totalRows,
    populateFirst: true
);
await PostgresMultiFormatInsertBenchmark.Run(
    connString,
    $"benchmark_insert_formats_{osVersion}.csv",
    totalRows: totalRows
);
await PostgresPointInsertBenchmark.Run(
    connString,
    $"benchmark_insert_points_{osVersion}.csv",
    totalPoints: totalRows,
    attributeCount: 16,
    rowBatchSizes: new[] { 1, 10, 50, 100, 500, 1000, 2000, 5000, 10000, 20000, 50000 },
    blobGroupSizes: new[] { 1, 10, 50, 100, 500, 1000, 2000, 5000, 10000, 20000, 50000 }
);
