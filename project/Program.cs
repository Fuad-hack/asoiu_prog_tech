using System;
using System.Threading.Tasks;

class Program
{
    static async Task Main()
    {
        Console.WriteLine("=== Real-Time Anomaly Detection Pipeline ===\n");

        var pipeline = new PartitionedPipeline();
        await pipeline.RunAsync();

        Console.WriteLine("\n=== Pipeline Completed ===");
    }
}