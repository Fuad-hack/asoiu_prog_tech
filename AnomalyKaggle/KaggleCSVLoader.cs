using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using CsvHelper;
using CsvHelper.Configuration;
using System.Globalization;

namespace AnomalyDetectionKaggle
{
    public class KaggleCSVLoader
    {
        private readonly string _csvPath;
        private readonly int _sampleRate;

        public KaggleCSVLoader(string csvPath, int sampleRate = 1)
        {
            _csvPath = csvPath;
            _sampleRate = Math.Max(sampleRate, 1);
        }

        public async IAsyncEnumerable<DataPoint> LoadFlowsAsync()
        {
            if (!File.Exists(_csvPath))
            {
                Console.WriteLine($"❌ CSV tapılmadı: {_csvPath}");
                yield break;
            }

            var config = new CsvConfiguration(CultureInfo.InvariantCulture)
            {
                MissingFieldFound = null,
                HeaderValidated = null,
                BadDataFound = null,
            };

            int recordCount = 0;
            List<NetworkTrafficRecord> allRecords;

            try
            {
                using var reader = new StreamReader(_csvPath);
                using var csv = new CsvReader(reader, config);
                csv.Context.RegisterClassMap<NetworkTrafficRecordMap>();
                allRecords = csv.GetRecords<NetworkTrafficRecord>().ToList();
            }
            catch (Exception ex)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine($"[❌ CSV Xəta] {ex.Message}");
                Console.ResetColor();
                yield break;
            }

            foreach (var record in allRecords)
            {
                recordCount++;

                if (recordCount % _sampleRate != 0)
                    continue;

                var dp = new DataPoint
                {
                    DeviceId      = record.LocalIPN.ToString(),
                    Value         = record.FlowCount,
                    Timestamp     = ParseDate(record.Date),
                    MetricType    = "flow",
                    RemoteASN     = record.RemoteASN,
                    OriginalLabel = "unknown"
                };

                yield return dp;

                if (recordCount % 500 == 0)
                    Console.WriteLine($"[📥 Loader] {recordCount} record emal edildi...");

                if (recordCount % 1000 == 0)
                    await Task.Yield();
            }

            Console.WriteLine($"[📥 Loader] ✓ Cəmi {recordCount} record emal edildi.");
        }

        private DateTime ParseDate(string? date)
        {
            if (string.IsNullOrEmpty(date)) return DateTime.Now;
            if (DateTime.TryParseExact(date, "yyyy-MM-dd",
                CultureInfo.InvariantCulture, DateTimeStyles.None, out var result))
                return result;
            return DateTime.Now;
        }

        public async Task<(int totalRecords, int uniqueIPs, int uniqueASNs, double avgFlow, double maxFlow)>
            GetStatisticsAsync()
        {
            if (!File.Exists(_csvPath)) return (0, 0, 0, 0, 0);

            var config = new CsvConfiguration(CultureInfo.InvariantCulture)
            {
                MissingFieldFound = null,
                HeaderValidated = null,
                BadDataFound = null,
            };

            try
            {
                using var reader = new StreamReader(_csvPath);
                using var csv = new CsvReader(reader, config);
                csv.Context.RegisterClassMap<NetworkTrafficRecordMap>();

                var list = csv.GetRecords<NetworkTrafficRecord>().ToList();
                int total     = list.Count;
                int uniqueIPs = list.Select(r => r.LocalIPN).Distinct().Count();
                int uniqueASN = list.Select(r => r.RemoteASN).Distinct().Count();
                double avg    = list.Any() ? list.Average(r => r.FlowCount) : 0;
                double max    = list.Any() ? list.Max(r => r.FlowCount) : 0;

                await Task.CompletedTask;
                return (total, uniqueIPs, uniqueASN, avg, max);
            }
            catch { return (0, 0, 0, 0, 0); }
        }
    }

    public class NetworkTrafficRecordMap : ClassMap<NetworkTrafficRecord>
    {
        public NetworkTrafficRecordMap()
        {
            Map(m => m.Date).Name("date");
            Map(m => m.LocalIPN).Name("l_ipn");
            Map(m => m.RemoteASN).Name("r_asn");
            Map(m => m.FlowCount).Name("f");
        }
    }
}