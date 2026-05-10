using System;
using CsvHelper.Configuration.Attributes;

namespace AnomalyDetectionKaggle
{
    /// <summary>
    /// Kaggle BGP/ASN traffic dataset record
    /// Columns: date, l_ipn, r_asn, f
    /// </summary>
    public class NetworkTrafficRecord
    {
        [Name("date")]
        public string? Date { get; set; }

        [Name("l_ipn")]
        public int LocalIPN { get; set; }       // Local IP network number

        [Name("r_asn")]
        public int RemoteASN { get; set; }      // Remote Autonomous System Number

        [Name("f")]
        public double FlowCount { get; set; }   // Flow count (traffic volume)
    }

    /// <summary>
    /// Processed data point for anomaly detection
    /// </summary>
    public class DataPoint
    {
        public string DeviceId { get; set; } = string.Empty;   // l_ipn as string
        public double Value { get; set; }                       // flow count (f)
        public DateTime Timestamp { get; set; }
        public string MetricType { get; set; } = "flow";
        public int RemoteASN { get; set; }
        public string OriginalLabel { get; set; } = "unknown"; // anomaly sonra təyin edilir
    }

    /// <summary>
    /// Configuration update for live threshold changes
    /// </summary>
    public class ConfigUpdate
    {
        public double? Threshold { get; set; }
        public int? WindowSize { get; set; }
        public string? Description { get; set; }
    }

    /// <summary>
    /// Detection result for a single data point
    /// </summary>
    public class DetectionResult
    {
        public DataPoint Data { get; set; } = new();
        public double ZScore { get; set; }
        public bool IsAnomaly { get; set; }
        public bool ShouldAlert { get; set; }
        public double Mean { get; set; }
        public double StdDev { get; set; }
        public DateTime DetectedAt { get; set; }
    }

    /// <summary>
    /// Statistics summary per device
    /// </summary>
    public class DeviceMetrics
    {
        public string DeviceId { get; set; } = string.Empty;
        public int TotalProcessed { get; set; }
        public int AnomaliesDetected { get; set; }
        public double AnomalyRate { get; set; }
        public double AvgValue { get; set; }
        public double MaxValue { get; set; }
        public double MinValue { get; set; }
        public DateTime LastUpdated { get; set; }
    }
}