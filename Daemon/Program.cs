using System;
using System.Threading.Tasks;
using Jobs;

namespace Daemon
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Starting WebGallery background jobs...");
            Console.WriteLine($"Elasticsearch endpoint: {args[0]}");
            Console.WriteLine($"Job name: {args[1]}");

            var esService = new ElasticsearchService(args[0]);
            var job = new ReIndexAllJob(esService);
            
            Task t = job.Run();
            t.Wait();

            Console.WriteLine($"Job completed. Press key to exit.");
            Console.ReadKey();
        }

    }
}
