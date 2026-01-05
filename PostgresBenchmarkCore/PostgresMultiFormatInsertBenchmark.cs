using System.Diagnostics;
using Npgsql;
using NpgsqlTypes;

namespace PostgresBenchmarkCore
{
    /// <summary>
    /// Compares insertion speed among:
    /// 1. Row with 10 separate DOUBLE PRECISION columns.
    /// 2. Row with a single BYTEA blob containing 10 packed doubles.
    /// 3. Existing baseline table (benchmarkdata) from PostgresSimpleInsertBenchmark.
    /// </summary>
    public class PostgresMultiFormatInsertBenchmark
    {
        private readonly NpgsqlConnection _conn;

        public PostgresMultiFormatInsertBenchmark(string connString)
        {
            _conn = new NpgsqlConnection(connString);
            _conn.Open();
        }

        public async Task RecreateTablesAsync()
        {
            const string sql =
                @"
                DROP TABLE IF EXISTS benchmarkfloats CASCADE;
                DROP TABLE IF EXISTS benchmarkfloats_blob CASCADE;

                CREATE TABLE benchmarkfloats (
                    id SERIAL PRIMARY KEY,
                    f1 DOUBLE PRECISION,
                    f2 DOUBLE PRECISION,
                    f3 DOUBLE PRECISION,
                    f4 DOUBLE PRECISION,
                    f5 DOUBLE PRECISION,
                    f6 DOUBLE PRECISION,
                    f7 DOUBLE PRECISION,
                    f8 DOUBLE PRECISION,
                    f9 DOUBLE PRECISION,
                    f10 DOUBLE PRECISION
                );

                CREATE TABLE benchmarkfloats_blob (
                    id SERIAL PRIMARY KEY,
                    payload BYTEA
                );
                ";
            await using var cmd = new NpgsqlCommand(sql, _conn);
            await cmd.ExecuteNonQueryAsync();
        }

        private static void FillTenDoubles(int index, double[] buffer)
        {
            // Example deterministic data pattern (cheap math); can be replaced with random if desired.
            buffer[0] = index;
            buffer[1] = Math.Sin(index);
            buffer[2] = Math.Cos(index);
            buffer[3] = Math.Sqrt(index + 1);
            buffer[4] = index * 0.5;
            buffer[5] = index % 97;
            buffer[6] = Math.Tanh(index * 0.001);
            buffer[7] = Math.Log(index + 2);
            buffer[8] = Math.Exp((index % 5) * 0.001);
            buffer[9] = (index & 1) == 0 ? 1.0 : -1.0;
        }

        private static byte[] PackTenDoubles(double[] src)
        {
            // 10 doubles * 8 bytes
            var bytes = new byte[80];
            for (int i = 0; i < 10; i++)
                BitConverter.GetBytes(src[i]).CopyTo(bytes, i * 8);
            return bytes;
        }

        public async Task<(double Seconds, double RowsPerSec)> RunInsertFloatsAsync(
            int totalRows,
            int batchSize
        )
        {
            var stopwatch = Stopwatch.StartNew();
            int inserted = 0;
            var values = new double[10];

            while (inserted < totalRows)
            {
                await using var tx = await _conn.BeginTransactionAsync();

                // Prepare command once per transaction; reuse parameters.
                await using var cmd = new NpgsqlCommand(
                    @"
                    INSERT INTO benchmarkfloats (f1,f2,f3,f4,f5,f6,f7,f8,f9,f10)
                    VALUES (@f1,@f2,@f3,@f4,@f5,@f6,@f7,@f8,@f9,@f10);",
                    _conn,
                    tx
                );

                // Define parameters
                cmd.Parameters.Add(new NpgsqlParameter("f1", NpgsqlDbType.Double));
                cmd.Parameters.Add(new NpgsqlParameter("f2", NpgsqlDbType.Double));
                cmd.Parameters.Add(new NpgsqlParameter("f3", NpgsqlDbType.Double));
                cmd.Parameters.Add(new NpgsqlParameter("f4", NpgsqlDbType.Double));
                cmd.Parameters.Add(new NpgsqlParameter("f5", NpgsqlDbType.Double));
                cmd.Parameters.Add(new NpgsqlParameter("f6", NpgsqlDbType.Double));
                cmd.Parameters.Add(new NpgsqlParameter("f7", NpgsqlDbType.Double));
                cmd.Parameters.Add(new NpgsqlParameter("f8", NpgsqlDbType.Double));
                cmd.Parameters.Add(new NpgsqlParameter("f9", NpgsqlDbType.Double));
                cmd.Parameters.Add(new NpgsqlParameter("f10", NpgsqlDbType.Double));

                for (int i = 0; i < batchSize && inserted < totalRows; i++)
                {
                    FillTenDoubles(inserted, values);

                    for (int p = 0; p < 10; p++)
                        cmd.Parameters[p].Value = values[p];

                    await cmd.ExecuteNonQueryAsync();
                    inserted++;
                }

                await tx.CommitAsync();
            }

            stopwatch.Stop();
            double seconds = stopwatch.Elapsed.TotalSeconds;
            return (seconds, inserted / seconds);
        }

        public async Task<(double Seconds, double RowsPerSec)> RunInsertBlobAsync(
            int totalRows,
            int batchSize
        )
        {
            var stopwatch = Stopwatch.StartNew();
            int inserted = 0;
            var values = new double[10];

            while (inserted < totalRows)
            {
                await using var tx = await _conn.BeginTransactionAsync();

                await using var cmd = new NpgsqlCommand(
                    @"
                    INSERT INTO benchmarkfloats_blob (payload) VALUES (@p);",
                    _conn,
                    tx
                );

                var p = new NpgsqlParameter<byte[]>("p", NpgsqlTypes.NpgsqlDbType.Bytea);
                cmd.Parameters.Add(p);

                for (int i = 0; i < batchSize && inserted < totalRows; i++)
                {
                    FillTenDoubles(inserted, values);
                    p.Value = PackTenDoubles(values);
                    await cmd.ExecuteNonQueryAsync();
                    inserted++;
                }

                await tx.CommitAsync();
            }

            stopwatch.Stop();
            double seconds = stopwatch.Elapsed.TotalSeconds;
            return (seconds, inserted / seconds);
        }

        public static async Task Run(string connString, string outputFile, int totalRows = 5000)
        {
            int[] batchSizes = { 1, 10, 50, 100, 500, 1000, 2000, 5000, 10000 };

            var bench = new PostgresMultiFormatInsertBenchmark(connString);
            await bench.RecreateTablesAsync();

            Console.WriteLine("PostgreSQL multi-format insertion benchmark");
            Console.WriteLine($"Total rows per test: {totalRows}");
            Console.WriteLine($"Batch sizes: {string.Join(", ", batchSizes)}");
            Console.WriteLine();

            var results =
                new List<(string Mode, int BatchSize, double Seconds, double RowsPerSec)>();

            foreach (var batch in batchSizes)
            {
                Console.WriteLine($"=== Batch {batch} (10 floats columns) ===");
                var (s1, r1) = await bench.RunInsertFloatsAsync(totalRows, batch);
                Console.WriteLine($"→ {totalRows} rows in {s1:F2}s ({r1:F0} rows/s)\n");
                results.Add(("10cols", batch, s1, r1));

                Console.WriteLine($"=== Batch {batch} (binary blob) ===");
                var (s2, r2) = await bench.RunInsertBlobAsync(totalRows, batch);
                Console.WriteLine($"→ {totalRows} rows in {s2:F2}s ({r2:F0} rows/s)\n");
                results.Add(("blob", batch, s2, r2));
            }

            Console.WriteLine("\n=== Summary ===");
            foreach (var r in results)
                Console.WriteLine(
                    $"{r.Mode, -8} batch {r.BatchSize, 5}: {r.Seconds, 6:F2}s  {r.RowsPerSec, 10:F0} rows/s"
                );

            var best = results.OrderByDescending(r => r.RowsPerSec).First();
            Console.WriteLine(
                $"\n🏆 Fastest overall: {best.Mode} batch {best.BatchSize} ({best.RowsPerSec:F0} rows/s)"
            );

            File.WriteAllLines(
                outputFile,
                new[] { "Mode,BatchSize,Seconds,RowsPerSec" }.Concat(
                    results.Select(r => $"{r.Mode},{r.BatchSize},{r.Seconds:F3},{r.RowsPerSec:F0}")
                )
            );

            Console.WriteLine($"\nResults saved to {outputFile}");
        }
    }
}
