using Npgsql;
using System.Diagnostics;

namespace PostgresBenchmarkCore
{
    public class PostgresSimpleInsertBenchmark
    {
        private NpgsqlConnection conn;
        public PostgresSimpleInsertBenchmark(string connString)
        {
            conn = new NpgsqlConnection(connString);
            conn.Open();
        }
        public async Task<(double Seconds, double RowsPerSec)> RunBenchmark(int totalRows, int batchSize)
        {
            var stopwatch = Stopwatch.StartNew();
            int rowsInserted = 0;

            while (rowsInserted < totalRows)
            {
                await using var tx = await conn.BeginTransactionAsync();
                await using var cmd = new NpgsqlCommand();
                cmd.Connection = conn;
                cmd.Transaction = tx;

                for (int i = 0; i < batchSize && rowsInserted < totalRows; i++)
                {
                    cmd.CommandText = "INSERT INTO benchmarkdata (value1, value2, textvalue) VALUES (@v1, @v2, @v3)";
                    cmd.Parameters.Clear();
                    cmd.Parameters.AddWithValue("v1", rowsInserted);
                    cmd.Parameters.AddWithValue("v2", Math.Sin(rowsInserted));
                    cmd.Parameters.AddWithValue("v3", $"row_{rowsInserted}");
                    await cmd.ExecuteNonQueryAsync();
                    rowsInserted++;
                }

                await tx.CommitAsync();
            }

            stopwatch.Stop();
            double seconds = stopwatch.Elapsed.TotalSeconds;
            double rowsPerSec = totalRows / seconds;
            return (seconds, rowsPerSec);
        }

        public async Task CleanDatabase()
        {
            await using var cmd = new NpgsqlCommand("TRUNCATE benchmarkdata RESTART IDENTITY", conn);
            await cmd.ExecuteNonQueryAsync();
        }

        public async Task RecreateBenchmarkTableAsync()
        {
            const string sql = @"
                DROP TABLE IF EXISTS benchmarkdata CASCADE;

                CREATE TABLE benchmarkdata (
                    id SERIAL PRIMARY KEY,
                    value1 BIGINT,
                    value2 DOUBLE PRECISION,
                    textvalue TEXT
                );
            ";

            try
            {

                using var cmd = new NpgsqlCommand(sql, conn);
                await cmd.ExecuteNonQueryAsync();

                Console.WriteLine("✅ Table 'benchmarkdata' recreated successfully.");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"❌ Failed to recreate table: {ex.Message}");
            }
        }

        public static async Task Run(string connString, string outputFile, int totalRows = 5000)
        {
            PostgresSimpleInsertBenchmark simpleInsertBenchmark = new PostgresSimpleInsertBenchmark(connString);
            await simpleInsertBenchmark.RecreateBenchmarkTableAsync();
            int[] batchSizes = { 1, 10, 50, 100, 500, 1000, 2000, 5000, 10000 };

            Console.WriteLine("Starting PostgreSQL insertion benchmark...");
            Console.WriteLine($"Total rows to insert per test: {totalRows}");
            Console.WriteLine($"Testing batch sizes: {string.Join(", ", batchSizes)}");
            Console.WriteLine();

            var results = new List<(int BatchSize, double Seconds, double RowsPerSec)>();

            await simpleInsertBenchmark.CleanDatabase();

            foreach (var batchSize in batchSizes)
            {
                Console.WriteLine($"=== Batch size: {batchSize} ===");

                var (seconds, rate) = await simpleInsertBenchmark.RunBenchmark(totalRows, batchSize);
                results.Add((batchSize, seconds, rate));

                Console.WriteLine($"→ {totalRows} rows in {seconds:F2}s ({rate:F0} rows/s)\n");
            }

            // Display summary
            Console.WriteLine("\n=== Summary ===");
            foreach (var r in results)
                Console.WriteLine($"Batch {r.BatchSize,6}: {r.Seconds,6:F2}s  {r.RowsPerSec,10:F0} rows/s");

            var best = results.OrderByDescending(r => r.RowsPerSec).First();
            Console.WriteLine($"\n🏆 Optimal batch size: {best.BatchSize} rows per transaction ({best.RowsPerSec:F0} rows/s)");

            // Optional: write results to CSV
            File.WriteAllLines(outputFile, new[] { "BatchSize,Seconds,RowsPerSec" }
                .Concat(results.Select(r => $"{r.BatchSize},{r.Seconds:F3},{r.RowsPerSec:F0}")));

            Console.WriteLine($"\nResults saved to {outputFile}");
        }
    }    
}
