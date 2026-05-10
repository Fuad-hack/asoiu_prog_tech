using System;
using System.IO;
using System.Threading.Tasks;
using AnomalyDetectionKaggle;

class Program
{
    static async Task Main(string[] args)
    {
        Console.OutputEncoding = System.Text.Encoding.UTF8;

        Console.WriteLine("╔════════════════════════════════════════════════════════════╗");
        Console.WriteLine("║   Kaggle BGP/ASN Traffic - Anomaliya Aşkarlama            ║");
        Console.WriteLine("╚════════════════════════════════════════════════════════════╝\n");

        // CSV faylını tap
        string? csvPath = DetermineCsvPath();

        if (string.IsNullOrEmpty(csvPath) || !File.Exists(csvPath))
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine("❌ CSV fayl tapılmadı!");
            Console.WriteLine("   Gözlənilən yerlər:");
            Console.WriteLine("   • ./data/network_traffic.csv");
            Console.WriteLine("   • ./network_traffic.csv");
            Console.ResetColor();
            return;
        }

        // Pipeline-ı işlət
        var pipeline = new PartitionedPipeline(csvPath, "flow");
        await pipeline.RunAsync();

        Console.WriteLine("\n✅ Pipeline uğurla tamamlandı!");
        Console.WriteLine("📁 Alert fayllarına bax: alerts_worker_0.csv ... alerts_worker_3.csv");
    }

    static string? DetermineCsvPath()
    {
        // Mümkün yerlər
        string[] candidates = new[]
        {
            Path.Combine("data", "network_traffic.csv"),
            "network_traffic.csv",
            Path.Combine("..", "data", "network_traffic.csv"),
            Path.Combine(Directory.GetCurrentDirectory(), "data", "network_traffic.csv"),
        };

        foreach (var p in candidates)
        {
            if (File.Exists(p))
            {
                Console.WriteLine($"✅ CSV tapıldı: {Path.GetFullPath(p)}");
                return p;
            }
        }

        // İstifadəçidən sor
        Console.Write("\n📁 CSV faylının yolunu daxil et: ");
        var userPath = Console.ReadLine()?.Trim();

        return (!string.IsNullOrEmpty(userPath) && File.Exists(userPath)) ? userPath : null;
    }
}