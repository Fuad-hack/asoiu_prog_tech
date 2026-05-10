using System;

public class DataPoint
{
    public string DeviceId { get; set; }
    public double Value { get; set; }
    public DateTime Timestamp { get; set; }
}

public class ConfigUpdate
{
    public double? Threshold { get; set; }
    public int? WindowSize { get; set; }
}