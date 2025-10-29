using Npgsql;
using System.Diagnostics;

namespace PostgresBenchmarkCore
{
    public class PostgresSimpleReadBenchmark
    {
        private readonly NpgsqlConnection _conn;

        public PostgresSimpleReadBenchmark(string connString)
        {
            _conn = new NpgsqlConnection(connString);
            _conn.Open();
        }

        private async Task EnsureDataAsync(int totalRows, bool populateFirst, int populateBatchSize = 1000)
        {
            await using var countCmd = new NpgsqlCommand("SELECT COUNT(*) FROM benchmarkdata", _conn);
            var existing = (long)(await countCmd.ExecuteScalarAsync() ?? 0L);

            if (existing >= totalRows) return;

            if (!populateFirst)
                throw new InvalidOperationException($"Table has {existing} rows but {totalRows} required. Populate first or set populateFirst=true.");

            var inserter = new PostgresSimpleInsertBenchmark(_conn.ConnectionString);
            await inserter.CleanDatabase();
            // Use populateBatchSize for insertion efficiency
            await inserter.RunBenchmark(totalRows, populateBatchSize);
        }

        // Core read benchmark: fetch rows via server-side cursor in chunks of fetchSize
        public async Task<(double Seconds, double RowsPerSec)> RunReadBenchmark(int totalRows, int fetchSize)
        {
            var sw = Stopwatch.StartNew();
            int rowsRead = 0;

            await using (var tx = await _conn.BeginTransactionAsync())
            {
                // Declare cursor
                await using (var declare = new NpgsqlCommand("DECLARE benchmark_cursor NO SCROLL CURSOR FOR SELECT id,value1,value2,textvalue FROM benchmarkdata ORDER BY id", _conn, tx))
                    await declare.ExecuteNonQueryAsync();

                while (rowsRead < totalRows)
                {
                    int remaining = totalRows - rowsRead;
                    int thisFetch = remaining < fetchSize ? remaining : fetchSize;

                    await using var fetchCmd = new NpgsqlCommand($"FETCH FORWARD {thisFetch} FROM benchmark_cursor", _conn, tx);
                    await using var reader = await fetchCmd.ExecuteReaderAsync();

                    // Materialize columns to simulate realistic access cost
                    while (await reader.ReadAsync())
                    {
                        // Access fields (avoid GC pressure by not allocating strings unnecessarily)
                        var intVal = reader.GetInt32(0);
                        var bigIntVal = reader.GetInt64(1);
                        var doubleVal = reader.GetDouble(2);
                        // For text value we can skip or touch
                        if (!reader.IsDBNull(3))
                        {
                            string stringVal = reader.GetString(3);
                        }
                            
                        rowsRead++;
                    }
                }

                // Close cursor explicitly (optional, COMMIT will also clean it up)
                await using (var closeCmd = new NpgsqlCommand("CLOSE benchmark_cursor", _conn, tx))
                    await closeCmd.ExecuteNonQueryAsync();

                await tx.CommitAsync();
            }

            sw.Stop();
            double seconds = sw.Elapsed.TotalSeconds;
            double rate = rowsRead / seconds;
            return (seconds, rate);
        }

        public static async Task Run(string connString, string outputFile, int totalRows = 5000, bool populateFirst = false)
        {
            var readBenchmark = new PostgresSimpleReadBenchmark(connString);

            // Make sure data exists (optional auto-populate)
            await readBenchmark.EnsureDataAsync(totalRows, populateFirst);

            int[] fetchSizes = { 1, 10, 50, 100, 500, 1000, 2000, 5000, 10000 };

            Console.WriteLine("Starting PostgreSQL read benchmark...");
            Console.WriteLine($"Total rows to read per test: {totalRows}");
            Console.WriteLine($"Testing fetch sizes (cursor FETCH): {string.Join(", ", fetchSizes)}");
            Console.WriteLine();

            var results = new List<(int FetchSize, double Seconds, double RowsPerSec)>();

            foreach (var fetchSize in fetchSizes)
            {
                Console.WriteLine($"=== Fetch size: {fetchSize} ===");
                var (seconds, rate) = await readBenchmark.RunReadBenchmark(totalRows, fetchSize);
                results.Add((fetchSize, seconds, rate));
                Console.WriteLine($"→ {totalRows} rows in {seconds:F2}s ({rate:F0} rows/s)\n");
            }

            Console.WriteLine("\n=== Summary ===");
            foreach (var r in results)
                Console.WriteLine($"Fetch {r.FetchSize,6}: {r.Seconds,6:F2}s  {r.RowsPerSec,10:F0} rows/s");

            var best = results.OrderByDescending(r => r.RowsPerSec).First();
            Console.WriteLine($"\n🏆 Optimal fetch size: {best.FetchSize} rows per FETCH ({best.RowsPerSec:F0} rows/s)");

            File.WriteAllLines(outputFile, new[] { "FetchSize,Seconds,RowsPerSec" }
                .Concat(results.Select(r => $"{r.FetchSize},{r.Seconds:F3},{r.RowsPerSec:F0}")));

            Console.WriteLine($"\nResults saved to {outputFile}");
        }
    }
}