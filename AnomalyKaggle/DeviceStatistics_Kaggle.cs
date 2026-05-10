using System;
using System.Collections.Generic;

namespace AnomalyDetectionKaggle
{
    /// <summary>
    /// Maintains rolling window statistics for per-device anomaly detection
    /// Thread-safe for per-worker isolation
    /// </summary>
    public class DeviceStatistics
    {
        private readonly int _windowSize;
        private readonly Queue<double> _window = new();
        private double _sum;
        private double _sumSq;
        private double _maxValue = double.MinValue;
        private double _minValue = double.MaxValue;

        private DateTime _lastAlertTime = DateTime.MinValue;
        private readonly int _cooldownSeconds = 5;

        public DeviceStatistics(int windowSize)
        {
            _windowSize = Math.Max(windowSize, 2); // At least 2 for variance
        }

        /// <summary>
        /// Add a new value and check if it's an anomaly
        /// Returns: (Z-score, IsAnomaly, ShouldAlert, Mean, StdDev)
        /// </summary>
        public (double z, bool isAnomaly, bool shouldAlert, double mean, double stdDev) Add(
            double value, double threshold)
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

            // Track min/max
            _maxValue = Math.Max(_maxValue, value);
            _minValue = Math.Min(_minValue, value);

            // Need at least 2 points for variance
            if (_window.Count < 2)
                return (0, false, false, _sum, 0);

            // Calculate mean and standard deviation
            double mean = _sum / _window.Count;
            double variance = (_sumSq / _window.Count) - (mean * mean);
            double stdDev = Math.Sqrt(Math.Max(variance, 0));

            // Prevent division by zero
            if (stdDev < 1e-6)
                return (0, false, false, mean, stdDev);

            // Z-score: how many standard deviations away from mean?
            double z = (value - mean) / stdDev;
            bool isAnomaly = Math.Abs(z) > threshold;

            // Cool-off window: prevent alert spam (one alert per device per N seconds)
            bool shouldAlert = isAnomaly &&
                (DateTime.Now - _lastAlertTime).TotalSeconds > _cooldownSeconds;

            if (shouldAlert)
                _lastAlertTime = DateTime.Now;

            return (z, isAnomaly, shouldAlert, mean, stdDev);
        }

        /// <summary>
        /// Get current statistics without adding new value
        /// </summary>
        public (double mean, double stdDev, int count) GetStatistics()
        {
            if (_window.Count == 0)
                return (0, 0, 0);

            double mean = _sum / _window.Count;
            double variance = (_sumSq / _window.Count) - (mean * mean);
            double stdDev = Math.Sqrt(Math.Max(variance, 0));

            return (mean, stdDev, _window.Count);
        }

        /// <summary>
        /// Get min/max values seen in window
        /// </summary>
        public (double min, double max) GetMinMax() => (_minValue, _maxValue);

        /// <summary>
        /// Clear all statistics
        /// </summary>
        public void Reset()
        {
            _window.Clear();
            _sum = 0;
            _sumSq = 0;
            _maxValue = double.MinValue;
            _minValue = double.MaxValue;
        }
    }
}