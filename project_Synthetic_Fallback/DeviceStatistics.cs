using System;
using System.Collections.Generic;

public class DeviceStatistics
{
    private readonly int _windowSize;
    private readonly Queue<double> _window = new();
    private double _sum;
    private double _sumSq;

    private DateTime _lastAlertTime = DateTime.MinValue;
    private readonly int _cooldownSeconds = 5;

    public DeviceStatistics(int windowSize)
    {
        _windowSize = windowSize;
    }

    public (double z, bool isAnomaly, bool shouldAlert) Add(double value, double threshold)
    {
        // Maintain sliding window
        if (_window.Count == _windowSize)
        {
            var old = _window.Dequeue();
            _sum -= old;
            _sumSq -= old * old;
        }

        _window.Enqueue(value);
        _sum += value;
        _sumSq += value * value;

        // Need at least 2 points for variance
        if (_window.Count < 2)
            return (0, false, false);

        // Calculate mean and standard deviation
        double mean = _sum / _window.Count;
        double variance = (_sumSq / _window.Count) - (mean * mean);
        double std = Math.Sqrt(Math.Max(variance, 0));

        // Prevent division by zero
        if (std < 1e-6)
            return (0, false, false);

        // Z-score: how many standard deviations away from mean?
        double z = (value - mean) / std;
        bool isAnomaly = Math.Abs(z) > threshold;

        // Cool-off window: prevent alert spam
        bool shouldAlert = isAnomaly &&
            (DateTime.Now - _lastAlertTime).TotalSeconds > _cooldownSeconds;

        if (shouldAlert)
            _lastAlertTime = DateTime.Now;

        return (z, isAnomaly, shouldAlert);
    }
}