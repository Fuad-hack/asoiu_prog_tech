using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace AnomalyDetectionKaggle
{
    /// <summary>
    /// Paralel anomaliya aşkarlama pipeline-ı
    /// l_ipn (local IP network) üzrə 4 işçiyə bölür
    /// </summary>
    public class PartitionedPipeline
    {
        private readonly string _csvPath;
        private readonly string _metric;   // həmişə "flow"

        private const int WorkerCount  = 4;
        private const int InputBuffer  = 2000;
        private const int WorkerBuffer = 500;

        private double _threshold  = 3.0;
        private int    _windowSize = 50;

        // Ümumi statistika
        private int _totalProcessed;
        private int _totalAnomalies;

        public PartitionedPipeline(string csvPath, string metric = "flow")
        {
            _csvPath = csvPath;
            _metric  = metric;
        }

        public async Task RunAsync()
        {
            Console.WriteLine($"\n[🚀 Pipeline] Başlayır — metric: flow, threshold: {_threshold}, window: {_windowSize}");

            // ── Kanallar ────────────────────────────────────────────
            var inputChannel = Channel.CreateBounded<DataPoint>(InputBuffer);

            var workerChannels = new Channel<DataPoint>[WorkerCount];
            for (int i = 0; i < WorkerCount; i++)
                workerChannels[i] = Channel.CreateBounded<DataPoint>(WorkerBuffer);

            // ── Dataset məlumatı ────────────────────────────────────
            var loader = new KaggleCSVLoader(_csvPath);
            var (total, uniqueIPs, uniqueASNs, avgFlow, maxFlow) = await loader.GetStatisticsAsync();

            Console.WriteLine("\n[📊 Dataset Məlumatı]");
            Console.WriteLine($"   • Cəmi record:      {total:N0}");
            Console.WriteLine($"   • Unikal l_ipn:     {uniqueIPs}");
            Console.WriteLine($"   • Unikal ASN:       {uniqueASNs}");
            Console.WriteLine($"   • Orta flow (f):    {avgFlow:F2}");
            Console.WriteLine($"   • Maks flow (f):    {maxFlow:F2}");

            // ── Task-lar ────────────────────────────────────────────
            var produceTask = ProduceAsync(inputChannel.Writer);
            var routeTask   = RouteAsync(inputChannel.Reader, workerChannels);
            var configTask  = SimulateConfigChangesAsync();

            var workerTasks = new Task[WorkerCount];
            for (int i = 0; i < WorkerCount; i++)
            {
                int id = i;
                workerTasks[id] = WorkerAsync(id, workerChannels[id].Reader);
            }

            await produceTask;
            await routeTask;
            await Task.WhenAll(workerTasks);
            // configTask fire-and-forget işləyir, dispose etmə

            PrintFinalStats();
        }

        // ── Producer ────────────────────────────────────────────────
        private async Task ProduceAsync(ChannelWriter<DataPoint> writer)
        {
            Console.WriteLine("\n[📥 Producer] Kaggle dataseti yüklənir...");

            var loader = new KaggleCSVLoader(_csvPath);

            await foreach (var dp in loader.LoadFlowsAsync())
            {
                await writer.WriteAsync(dp);
            }

            writer.Complete();
            Console.WriteLine("[📥 Producer] ✓ Bütün məlumatlar göndərildi.");
        }

        // ── Router — l_ipn hash mod 4 ────────────────────────────────
        private async Task RouteAsync(
            ChannelReader<DataPoint> reader,
            Channel<DataPoint>[] workers)
        {
            int routed = 0;

            await foreach (var dp in reader.ReadAllAsync())
            {
                int workerIdx = Math.Abs(dp.DeviceId.GetHashCode()) % WorkerCount;
                await workers[workerIdx].Writer.WriteAsync(dp);
                routed++;
            }

            foreach (var w in workers)
                w.Writer.Complete();

            Console.WriteLine($"[🔀 Router] ✓ {routed:N0} data point {WorkerCount} işçiyə paylandı.");
        }

        // ── Worker ───────────────────────────────────────────────────
        private async Task WorkerAsync(int workerId, ChannelReader<DataPoint> reader)
        {
            var stats = new Dictionary<string, DeviceStatistics>();
            var alerts = new List<DetectionResult>();

            int processed = 0;
            int anomalies = 0;

            await foreach (var dp in reader.ReadAllAsync())
            {
                processed++;

                if (!stats.ContainsKey(dp.DeviceId))
                    stats[dp.DeviceId] = new DeviceStatistics(_windowSize);

                var (z, isAnomaly, shouldAlert, mean, stdDev) =
                    stats[dp.DeviceId].Add(dp.Value, _threshold);

                var result = new DetectionResult
                {
                    Data        = dp,
                    ZScore      = z,
                    IsAnomaly   = isAnomaly,
                    ShouldAlert = shouldAlert,
                    Mean        = mean,
                    StdDev      = stdDev,
                    DetectedAt  = DateTime.UtcNow
                };

                if (isAnomaly)
                {
                    anomalies++;
                    dp.OriginalLabel = "anomaly";
                }

                if (shouldAlert)
                {
                    alerts.Add(result);
                    PrintAlert(workerId, result);
                }
                else if (processed % 200 == 0)
                {
                    // Normal trafik nümunəsi
                    Console.ForegroundColor = ConsoleColor.DarkGray;
                    Console.WriteLine(
                        $"  W{workerId} l_ipn={dp.DeviceId,-4} asn={dp.RemoteASN,-6} " +
                        $"flow={dp.Value,8:F0}  z={z,6:F2}  ✓ normal");
                    Console.ResetColor();
                }
            }

            // Alert faylına yaz
            await WriteAlertsAsync(workerId, alerts);

            Interlocked.Add(ref _totalProcessed, processed);
            Interlocked.Add(ref _totalAnomalies, anomalies);

            double rate = processed > 0 ? (double)anomalies / processed * 100 : 0;
            Console.ForegroundColor = ConsoleColor.Cyan;
            Console.WriteLine(
                $"\n[Worker {workerId}] ✓ Tamamlandı. " +
                $"Emal={processed:N0}, Anomaliya={anomalies}, Dərəcə={rate:F2}%");
            Console.ResetColor();
        }

        // ── Alert çap et ─────────────────────────────────────────────
        private void PrintAlert(int workerId, DetectionResult r)
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine(
                $"🚨 W{workerId} l_ipn={r.Data.DeviceId,-4} asn={r.Data.RemoteASN,-6} " +
                $"flow={r.Data.Value,8:F0}  z={r.ZScore,6:F2}  " +
                $"(threshold={_threshold})  [{r.Data.Timestamp:yyyy-MM-dd}]");
            Console.ResetColor();
        }

        // ── Alert CSV-ə yaz ──────────────────────────────────────────
        private async Task WriteAlertsAsync(int workerId, List<DetectionResult> alerts)
        {
            string fileName = $"alerts_worker_{workerId}.csv";

            using var writer = new StreamWriter(fileName, false);
            await writer.WriteLineAsync(
                "DetectedAt,Date,LocalIPN,RemoteASN,FlowCount,ZScore,Mean,StdDev,Threshold");

            foreach (var r in alerts)
            {
                await writer.WriteLineAsync(
                    $"{r.DetectedAt:O}," +
                    $"{r.Data.Timestamp:yyyy-MM-dd}," +
                    $"{r.Data.DeviceId}," +
                    $"{r.Data.RemoteASN}," +
                    $"{r.Data.Value:F0}," +
                    $"{r.ZScore:F4}," +
                    $"{r.Mean:F2}," +
                    $"{r.StdDev:F2}," +
                    $"{_threshold}");
            }

            Console.WriteLine($"[💾 Worker {workerId}] → {fileName} ({alerts.Count} alert)");
        }

        // ── Canlı konfiqurasiya dəyişikliyi (simulyasiya) ───────────
        private async Task SimulateConfigChangesAsync()
        {
            await Task.Delay(5000);
            _threshold = 2.5;
            Console.ForegroundColor = ConsoleColor.Yellow;
            Console.WriteLine("\n⚙️  [Config] Threshold dəyişdi: 3.0 → 2.5 (daha həssas)");
            Console.ResetColor();

            await Task.Delay(5000);
            _threshold = 3.5;
            Console.ForegroundColor = ConsoleColor.Yellow;
            Console.WriteLine("⚙️  [Config] Threshold dəyişdi: 2.5 → 3.5 (daha ciddi)\n");
            Console.ResetColor();
        }

        // ── Yekun statistika ─────────────────────────────────────────
        private void PrintFinalStats()
        {
            double rate = _totalProcessed > 0
                ? (double)_totalAnomalies / _totalProcessed * 100 : 0;

            Console.WriteLine("\n╔════════════════════════════════════════════════════════════╗");
            Console.WriteLine("║                    YEKUn STATİSTİKA                       ║");
            Console.WriteLine($"║  Cəmi Data Point:     {_totalProcessed,10:N0}                        ║");
            Console.WriteLine($"║  Anomaliya:           {_totalAnomalies,10:N0}                        ║");
            Console.WriteLine($"║  Anomaliya dərəcəsi:  {rate,9:F2}%                        ║");
            Console.WriteLine("║  Alert faylları:                                           ║");
            for (int i = 0; i < WorkerCount; i++)
                Console.WriteLine($"║   • alerts_worker_{i}.csv                                   ║");
            Console.WriteLine("╚════════════════════════════════════════════════════════════╝");
        }
    }
}