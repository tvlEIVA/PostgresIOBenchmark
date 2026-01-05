using System.Diagnostics;
using Npgsql;
using NpgsqlTypes;

namespace PostgresBenchmarkCore
{
    public class PostgresPointInsertBenchmark
    {
        private readonly NpgsqlConnection _conn;

        public PostgresPointInsertBenchmark(string connString)
        {
            _conn = new NpgsqlConnection(connString);
            _conn.Open();
        }

        public async Task RecreateTablesAsync()
        {
            const string sql =
                @"
                DROP TABLE IF EXISTS benchmark_points CASCADE;
                DROP TABLE IF EXISTS benchmark_points_blob CASCADE;

                CREATE TABLE benchmark_points (
                    id SERIAL PRIMARY KEY,
                    x DOUBLE PRECISION,
                    y DOUBLE PRECISION,
                    z DOUBLE PRECISION,
                    attrs DOUBLE PRECISION[]
                );

                CREATE TABLE benchmark_points_blob (
                    id SERIAL PRIMARY KEY,
                    group_size INT,
                    attr_count INT,
                    payload BYTEA  -- Packed sequence of points: (x,y,z,attr1..attrN) * group_size as IEEE 754 doubles
                );
                ";
            await using var cmd = new NpgsqlCommand(sql, _conn);
            await cmd.ExecuteNonQueryAsync();
        }

        private static void FillPoint(
            int index,
            int attrCount,
            double[] attrBuffer,
            out double x,
            out double y,
            out double z
        )
        {
            // Deterministic but varied pattern
            x = index * 0.001;
            y = Math.Sin(index * 0.01) * 10.0;
            z = Math.Cos(index * 0.01) * 10.0;
            for (int i = 0; i < attrCount; i++)
                attrBuffer[i] = (index % (i + 7)) * 0.1 + i;
        }

        // Mode 1: one INSERT per point (inside batched transaction of batchSize points for fairness)
        public async Task<(double Seconds, double PointsPerSec)> RunPointByPointAsync(
            int totalPoints,
            int attributeCount,
            int batchSize
        )
        {
            var sw = Stopwatch.StartNew();
            int inserted = 0;
            var attrBuffer = new double[attributeCount];

            while (inserted < totalPoints)
            {
                await using var tx = await _conn.BeginTransactionAsync();
                await using var cmd = new NpgsqlCommand(
                    "INSERT INTO benchmark_points (x,y,z,attrs) VALUES (@x,@y,@z,@attrs);",
                    _conn,
                    tx
                );

                var px = cmd.Parameters.Add(new NpgsqlParameter("x", NpgsqlDbType.Double));
                var py = cmd.Parameters.Add(new NpgsqlParameter("y", NpgsqlDbType.Double));
                var pz = cmd.Parameters.Add(new NpgsqlParameter("z", NpgsqlDbType.Double));
                var pattrs = cmd.Parameters.Add(
                    new NpgsqlParameter("attrs", NpgsqlDbType.Array | NpgsqlDbType.Double)
                );

                for (int i = 0; i < batchSize && inserted < totalPoints; i++)
                {
                    FillPoint(
                        inserted,
                        attributeCount,
                        attrBuffer,
                        out var x,
                        out var y,
                        out var z
                    );
                    px.Value = x;
                    py.Value = y;
                    pz.Value = z;
                    var arr = new double[attributeCount];
                    Array.Copy(attrBuffer, arr, attributeCount);
                    pattrs.Value = arr;

                    await cmd.ExecuteNonQueryAsync();
                    inserted++;
                }

                await tx.CommitAsync();
            }

            sw.Stop();
            double seconds = sw.Elapsed.TotalSeconds;
            return (seconds, inserted / seconds);
        }

        // Mode 2: multi-row INSERT with batchSize points per statement
        public async Task<(double Seconds, double PointsPerSec)> RunBatchedRowsAsync(
            int totalPoints,
            int attributeCount,
            int batchSize
        )
        {
            var sw = Stopwatch.StartNew();
            int inserted = 0;
            var attrBuffer = new double[attributeCount];

            while (inserted < totalPoints)
            {
                int thisBatch = Math.Min(batchSize, totalPoints - inserted);

                var sb = new System.Text.StringBuilder();
                sb.Append("INSERT INTO benchmark_points (x,y,z,attrs) VALUES ");
                var cmd = new NpgsqlCommand();
                cmd.Connection = _conn;

                for (int i = 0; i < thisBatch; i++)
                {
                    if (i > 0)
                        sb.Append(',');
                    string suffix = i.ToString();
                    sb.Append($"(@x{suffix},@y{suffix},@z{suffix},@a{suffix})");

                    FillPoint(
                        inserted + i,
                        attributeCount,
                        attrBuffer,
                        out var x,
                        out var y,
                        out var z
                    );

                    cmd.Parameters.Add(
                        new NpgsqlParameter($"x{suffix}", NpgsqlDbType.Double) { Value = x }
                    );
                    cmd.Parameters.Add(
                        new NpgsqlParameter($"y{suffix}", NpgsqlDbType.Double) { Value = y }
                    );
                    cmd.Parameters.Add(
                        new NpgsqlParameter($"z{suffix}", NpgsqlDbType.Double) { Value = z }
                    );

                    var arr = new double[attributeCount];
                    Array.Copy(attrBuffer, arr, attributeCount);
                    cmd.Parameters.Add(
                        new NpgsqlParameter($"a{suffix}", NpgsqlDbType.Array | NpgsqlDbType.Double)
                        {
                            Value = arr,
                        }
                    );
                }

                cmd.CommandText = sb.ToString();

                await using var tx = await _conn.BeginTransactionAsync();
                cmd.Transaction = tx;
                await cmd.ExecuteNonQueryAsync();
                await tx.CommitAsync();

                inserted += thisBatch;
            }

            sw.Stop();
            double seconds = sw.Elapsed.TotalSeconds;
            return (seconds, inserted / seconds);
        }

        // Mode 3: Binary COPY streaming with optional chunking & transaction control.
        // copyChunkSize: if >0, splits the load into multiple COPY statements of at most that many
        // rows.
        public async Task<(double Seconds, double PointsPerSec)> RunCopyBinaryAsync(
            int totalPoints,
            int attributeCount,
            int copyChunkSize = 0
        )
        {
            var sw = Stopwatch.StartNew();
            var attrBuffer = new double[attributeCount];

            // Determine chunking
            int remaining = totalPoints;
            int offset = 0;
            bool chunked = copyChunkSize > 0 && copyChunkSize < totalPoints;

            while (remaining > 0)
            {
                int thisChunk = chunked ? Math.Min(copyChunkSize, remaining) : remaining;

                await using var importer = await _conn.BeginBinaryImportAsync(
                    "COPY benchmark_points (x,y,z,attrs) FROM STDIN (FORMAT BINARY)"
                );

                for (int i = 0; i < thisChunk; i++)
                {
                    int rowIndex = offset + i;
                    FillPoint(
                        rowIndex,
                        attributeCount,
                        attrBuffer,
                        out var x,
                        out var y,
                        out var z
                    );
                    await importer.StartRowAsync();
                    await importer.WriteAsync(x, NpgsqlDbType.Double);
                    await importer.WriteAsync(y, NpgsqlDbType.Double);
                    await importer.WriteAsync(z, NpgsqlDbType.Double);
                    await importer.WriteAsync(attrBuffer, NpgsqlDbType.Array | NpgsqlDbType.Double);
                }

                await importer.CompleteAsync();

                offset += thisChunk;
                remaining -= thisChunk;
            }

            sw.Stop();
            double seconds = sw.Elapsed.TotalSeconds;
            return (seconds, totalPoints / seconds);
        }

        // Mode 4: Packs groupSize points into one blob row; payload layout is contiguous doubles.
        public async Task<(double Seconds, double PointsPerSec, int BlobBytes)> RunBlobAsync(
            int totalPoints,
            int attributeCount,
            int groupSize
        )
        {
            var sw = Stopwatch.StartNew();
            int pointsInserted = 0;
            int blobBytes = 0;

            int doublesPerPoint = 3 + attributeCount;
            int bytesPerPoint = doublesPerPoint * 8;

            while (pointsInserted < totalPoints)
            {
                int thisGroup = Math.Min(groupSize, totalPoints - pointsInserted);
                int totalBytes = thisGroup * bytesPerPoint;
                var payload = new byte[totalBytes];

                int offset = 0;
                var attrBuffer = new double[attributeCount];

                for (int i = 0; i < thisGroup; i++)
                {
                    FillPoint(
                        pointsInserted + i,
                        attributeCount,
                        attrBuffer,
                        out var x,
                        out var y,
                        out var z
                    );

                    BitConverter.GetBytes(x).CopyTo(payload, offset);
                    offset += 8;
                    BitConverter.GetBytes(y).CopyTo(payload, offset);
                    offset += 8;
                    BitConverter.GetBytes(z).CopyTo(payload, offset);
                    offset += 8;
                    for (int a = 0; a < attributeCount; a++)
                        BitConverter.GetBytes(attrBuffer[a]).CopyTo(payload, offset + a * 8);
                    offset += attributeCount * 8;
                }

                await using var tx = await _conn.BeginTransactionAsync();
                await using var cmd = new NpgsqlCommand(
                    "INSERT INTO benchmark_points_blob (group_size, attr_count, payload) VALUES (@g,@c,@p)",
                    _conn,
                    tx
                );

                cmd.Parameters.AddWithValue("g", NpgsqlDbType.Integer, thisGroup);
                cmd.Parameters.AddWithValue("c", NpgsqlDbType.Integer, attributeCount);
                cmd.Parameters.AddWithValue("p", NpgsqlDbType.Bytea, payload);

                await cmd.ExecuteNonQueryAsync();
                await tx.CommitAsync();

                pointsInserted += thisGroup;
                blobBytes += totalBytes;
            }

            sw.Stop();
            double seconds = sw.Elapsed.TotalSeconds;
            return (seconds, pointsInserted / seconds, blobBytes);
        }

        public static async Task Run(
            string connString,
            string outputFile,
            int totalPoints = 200_000,
            int attributeCount = 10,
            int[]? rowBatchSizes = null,
            int[]? blobGroupSizes = null
        )
        {
            rowBatchSizes ??= new[] { 1, 10, 50, 100, 250, 500, 1000 };
            blobGroupSizes ??= new[] { 1, 10, 50, 100, 250, 500, 1000 };

            var bench = new PostgresPointInsertBenchmark(connString);
            await bench.RecreateTablesAsync();

            Console.WriteLine("Point insertion benchmark (x,y,z + attributes)");
            Console.WriteLine($"Total points: {totalPoints}");
            Console.WriteLine($"Attribute count per point: {attributeCount}");
            Console.WriteLine($"Row batch sizes: {string.Join(", ", rowBatchSizes)}");
            Console.WriteLine($"Blob group sizes: {string.Join(", ", blobGroupSizes)}");
            Console.WriteLine();

            var results =
                new List<(
                    string Mode,
                    int Size,
                    double Seconds,
                    double PointsPerSec,
                    double Extra
                )>(); // Extra: bytes per point avg for blob

            // Point-by-point
            foreach (var batch in rowBatchSizes)
            {
                Console.WriteLine($"=== PointByPoint batchSize={batch} ===");
                var (sec, rate) = await bench.RunPointByPointAsync(
                    totalPoints,
                    attributeCount,
                    batch
                );
                Console.WriteLine($"→ {totalPoints} pts in {sec:F2}s ({rate:F0} pts/s)\n");
                results.Add(("PointByPoint", batch, sec, rate, 0));
            }

            // Batched multi-row
            foreach (var batch in rowBatchSizes)
            {
                Console.WriteLine($"=== BatchedRows batchSize={batch} ===");
                var (sec, rate) = await bench.RunBatchedRowsAsync(
                    totalPoints,
                    attributeCount,
                    batch
                );
                Console.WriteLine($"→ {totalPoints} pts in {sec:F2}s ({rate:F0} pts/s)\n");
                results.Add(("BatchedRows", batch, sec, rate, 0));
            }

            // Binary COPY (single streaming operation)
            foreach (var batch in rowBatchSizes)
            {
                Console.WriteLine($"=== BinaryCopy (COPY ... FORMAT BINARY) batchSize={batch} ===");
                var (sec, rate) = await bench.RunCopyBinaryAsync(
                    totalPoints,
                    attributeCount,
                    batch
                );
                Console.WriteLine($"→ {totalPoints} pts in {sec:F2}s ({rate:F0} pts/s)\n");
                results.Add(("BinaryCopy", batch, sec, rate, 0));
            }

            // Blob packing
            foreach (var group in blobGroupSizes)
            {
                Console.WriteLine($"=== BinaryBlob groupSize={group} ===");
                var (sec, rate, bytes) = await bench.RunBlobAsync(
                    totalPoints,
                    attributeCount,
                    group
                );
                double avgBytesPerPoint = (double)bytes / totalPoints;
                Console.WriteLine(
                    $"→ {totalPoints} pts in {sec:F2}s ({rate:F0} pts/s) avg {avgBytesPerPoint:F1} B/pt\n"
                );
                results.Add(("BinaryBlob", group, sec, rate, avgBytesPerPoint));
            }

            Console.WriteLine("\n=== Summary ===");
            foreach (var r in results)
            {
                string extra = r.Mode == "BinaryBlob" ? $"{r.Extra:F1}B/pt" : "";
                Console.WriteLine(
                    $"{r.Mode, -12} size {r.Size, 4}: {r.Seconds, 7:F2}s  {r.PointsPerSec, 9:F0} pts/s {extra}"
                );
            }

            var best = results.OrderByDescending(r => r.PointsPerSec).First();
            Console.WriteLine(
                $"\n🏆 Fastest: {best.Mode} size={best.Size} ({best.PointsPerSec:F0} pts/s)"
            );

            System.IO.File.WriteAllLines(
                outputFile,
                new[] { "Mode,Size,Seconds,PointsPerSec,AvgBytesPerPoint" }.Concat(
                    results.Select(r =>
                        $"{r.Mode},{r.Size},{r.Seconds:F3},{r.PointsPerSec:F0},{(r.Mode == "BinaryBlob" ? r.Extra.ToString("F1") : "")}"
                    )
                )
            );

            Console.WriteLine($"\nResults saved to {outputFile}");
        }
    }
}
