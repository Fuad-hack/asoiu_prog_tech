using System;
using System.IO;
using System.Linq;
using System.Threading.Channels;
using System.Threading.Tasks;
using System.Collections.Generic;

public class PartitionedPipeline
{
    private const int WORKERS = 4;

    private readonly Channel<DataPoint> _input =
        Channel.CreateBounded<DataPoint>(1000);

    private readonly Channel<DataPoint>[] _workerChannels;

    private readonly Dictionary<string, DeviceStatistics>[] _state;

    private readonly Channel<ConfigUpdate> _configChannel =
        Channel.CreateUnbounded<ConfigUpdate>();

    private double _threshold = 3.0;
    private int _windowSize = 20;

    private int _totalProcessed = 0;
    private int _totalAnomalies = 0;
    private bool _isRunning = true;

    public PartitionedPipeline()
    {
        _workerChannels = new Channel<DataPoint>[WORKERS];
        _state = new Dictionary<string, DeviceStatistics>[WORKERS];

        for (int i = 0; i < WORKERS; i++)
        {
            _workerChannels[i] = Channel.CreateBounded<DataPoint>(500);
            _state[i] = new Dictionary<string, DeviceStatistics>();
        }
    }

    public async Task RunAsync()
    {
        PrintBanner();

        var producer = ProduceAsync();
        var router = RouteAsync();

        var workers = Enumerable.Range(0, WORKERS)
            .Select(i => WorkerAsync(i));

        var configTask = SimulateConfigChanges();
        var statsTask = PrintStatsAsync();

        await Task.WhenAll(new[] { producer, router, configTask, statsTask }.Concat(workers));

        _isRunning = false;
        PrintFinalStats();
    }

    // ============================================================
    // 🎯 HYBRID DATA INGESTION - CSV or Synthetic
    // ============================================================
    private async Task ProduceAsync()
    {
        string csvPath = "data/network_traffic.csv";

        // TRY CSV FIRST
        if (File.Exists(csvPath))
        {
            Console.WriteLine($"✅ Found CSV at: {csvPath}\n");
            await LoadFromCsvAsync(csvPath);
        }
        else
        {
            // FALLBACK TO SYNTHETIC
            Console.WriteLine($"⚠️  CSV not found at: {csvPath}");
            Console.WriteLine("   Using synthetic data instead (simulates network traffic)\n");
            await GenerateSyntheticDataAsync();
        }

        _input.Writer.Complete();
    }

    // ============================================================
    // 📂 LOAD FROM REAL CSV FILE
    // ============================================================
    private async Task LoadFromCsvAsync(string csvPath)
    {
        try
        {
            var lines = File.ReadLines(csvPath).Skip(1); // Skip header
            int count = 0;
            int skipped = 0;

            Console.WriteLine("[📥 CSV Loader] Parsing CSV file...");

            foreach (var line in lines)
            {
                var fields = line.Split(new[] { ',', '\t' }, StringSplitOptions.RemoveEmptyEntries);

                if (fields.Length < 3)
                {
                    skipped++;
                    continue;
                }

                if (!double.TryParse(fields[1], out double value))
                {
                    skipped++;
                    continue;
                }

                if (!DateTime.TryParse(fields[2], out DateTime timestamp))
                    timestamp = DateTime.Now.AddSeconds(count);

                var dp = new DataPoint
                {
                    DeviceId = fields[0].Trim(),
                    Value = value,
                    Timestamp = timestamp
                };

                await _input.Writer.WriteAsync(dp);
                await Task.Delay(10);
                count++;
            }

            Console.WriteLine($"[📥 CSV Loader] ✓ Loaded {count} records (skipped {skipped})\n");
        }
        catch (Exception ex)
        {
            Console.ForegroundColor = ConsoleColor.Yellow;
            Console.WriteLine($"[⚠️  CSV Error] {ex.Message}");
            Console.WriteLine("   Falling back to synthetic data...\n");
            Console.ResetColor();

            await GenerateSyntheticDataAsync();
        }
    }

    // ============================================================
    // 🎲 GENERATE SYNTHETIC DATA
    // ============================================================
    private async Task GenerateSyntheticDataAsync()
    {
        string[] devices = { "IP_192.168.1.1", "IP_192.168.1.2", "IP_192.168.1.3", "IP_192.168.1.4" };
        var random = new Random(42);

        Console.WriteLine("[🎲 Synthetic Generator] Creating simulated network traffic...");
        Console.WriteLine("   • 4 devices (endpoints)");
        Console.WriteLine("   • 200 packets per device = 800 total");
        Console.WriteLine("   • Normal baseline: 50-100 Mbps");
        Console.WriteLine("   • Anomalies: Every 50th packet (spike to 500-700 Mbps)\n");

        int count = 0;
        int anomalies = 0;

        foreach (var device in devices)
        {
            for (int i = 0; i < 200; i++)
            {
                // NORMAL: 50-100 Mbps
                double baseValue = 50 + random.NextDouble() * 50;

                // ANOMALY: Every 50th packet = DDoS/attack (500-700 Mbps)
                if (i % 50 == 0)
                {
                    baseValue = 500 + random.NextDouble() * 200;
                    anomalies++;
                }

                var dp = new DataPoint
                {
                    DeviceId = device,
                    Value = baseValue,
                    Timestamp = DateTime.Now.AddSeconds(count++)
                };

                await _input.Writer.WriteAsync(dp);
                await Task.Delay(5);
            }
        }

        Console.WriteLine($"[🎲 Synthetic Generator] ✓ Generated {count} packets ({anomalies} anomalies)\n");
    }

    // ============================================================
    // 🔀 ROUTER - Partition by Device Key
    // ============================================================
    private async Task RouteAsync()
    {
        int count = 0;

        await foreach (var item in _input.Reader.ReadAllAsync())
        {
            int idx = Math.Abs(item.DeviceId.GetHashCode()) % WORKERS;
            await _workerChannels[idx].Writer.WriteAsync(item);
            count++;
        }

        foreach (var ch in _workerChannels)
            ch.Writer.Complete();

        Console.WriteLine($"[🔀 Router] ✓ Routed {count} items to {WORKERS} workers\n");
    }

    // ============================================================
    // ⚡ WORKER - Process Partition and Detect Anomalies
    // ============================================================
    private async Task WorkerAsync(int id)
    {
        string alertPath = $"alerts_worker_{id}.csv";
        if (File.Exists(alertPath)) File.Delete(alertPath);

        using var writer = new StreamWriter(alertPath, true);
        await writer.WriteLineAsync("Timestamp,DeviceId,Value,ZScore,Threshold");

        int processed = 0;
        int anomalies = 0;

        await foreach (var dp in _workerChannels[id].Reader.ReadAllAsync())
        {
            // Check for config updates
            while (_configChannel.Reader.TryRead(out var cfg))
            {
                if (cfg.Threshold.HasValue)
                {
                    _threshold = cfg.Threshold.Value;
                    Console.WriteLine($"\n⚙️  [Worker {id}] Threshold → {_threshold}\n");
                }
                if (cfg.WindowSize.HasValue)
                {
                    _windowSize = cfg.WindowSize.Value;
                    Console.WriteLine($"⚙️  [Worker {id}] Window → {_windowSize}\n");
                }
            }

            if (!_state[id].ContainsKey(dp.DeviceId))
                _state[id][dp.DeviceId] = new DeviceStatistics(_windowSize);

            var stats = _state[id][dp.DeviceId];
            var (z, isAnomaly, shouldAlert) = stats.Add(dp.Value, _threshold);

            processed++;
            _totalProcessed++;

            if (!isAnomaly)
                Console.WriteLine($"  W{id} {dp.DeviceId,-17} ✓ val={dp.Value:F1:F2} z={z:F2}");

            if (shouldAlert)
            {
                anomalies++;
                _totalAnomalies++;

                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine($"🚨 W{id} {dp.DeviceId,-17} ANOMALY! val={dp.Value:F1:F2} z={z:F2} (t={_threshold})");
                Console.ResetColor();

                await writer.WriteLineAsync(
                    $"{dp.Timestamp:O},{dp.DeviceId},{dp.Value:F2},{z:F2},{_threshold}");
                await writer.FlushAsync();
            }
        }

        Console.WriteLine($"\n[Worker {id}] ✓ Done. Processed={processed}, Anomalies={anomalies}\n");
    }

    // ============================================================
    // 🔧 SIMULATE LIVE CONFIG CHANGES
    // ============================================================
    private async Task SimulateConfigChanges()
    {
        await Task.Delay(3000);
        await _configChannel.Writer.WriteAsync(new ConfigUpdate { Threshold = 2.5 });

        await Task.Delay(5000);
        await _configChannel.Writer.WriteAsync(new ConfigUpdate { Threshold = 3.5 });
    }

    // ============================================================
    // 📊 PRINT LIVE METRICS
    // ============================================================
    private async Task PrintStatsAsync()
    {
        while (_isRunning)
        {
            await Task.Delay(15000);
            if (_totalProcessed > 0)
            {
                double rate = (_totalAnomalies * 100.0 / _totalProcessed);
                Console.WriteLine($"\n📊 Threshold={_threshold:F1} | Processed={_totalProcessed} | " +
                    $"Anomalies={_totalAnomalies} | Rate={rate:F2}%\n");
            }
        }
    }

    private void PrintBanner()
    {
        Console.WriteLine("\n╔════════════════════════════════════════════════════════════╗");
        Console.WriteLine("║   Real-Time Anomaly Detection with Concurrent Pipeline      ║");
        Console.WriteLine("║   Hybrid Mode: CSV → Synthetic Fallback                     ║");
        Console.WriteLine("╚════════════════════════════════════════════════════════════╝\n");
    }

    private void PrintFinalStats()
    {
        Console.WriteLine("\n╔════════════════════════════════════════════════════════════╗");
        Console.WriteLine($"║  Total Processed: {_totalProcessed,-45}║");
        Console.WriteLine($"║  Anomalies: {_totalAnomalies,-52}║");
        Console.WriteLine($"║  Rate: {(_totalAnomalies * 100.0 / _totalProcessed):F2}%{new string(' ', 52)}║");
        Console.WriteLine("╚════════════════════════════════════════════════════════════╝\n");
    }
}