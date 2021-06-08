using System;
using System.Threading.Tasks;
using Jobs;

namespace Daemon
{
    class Program
    {
        static void Main(string[] args)
        {
            if (args.Length == 0)
            {
                Console.WriteLine("Usage: WebGallery.Daemon [Elasticsearch endpoint] [Username] [Jobname]");
                Console.ReadKey();
                Environment.Exit(1);
            }

            Console.WriteLine("Starting WebGallery background jobs...");
            Console.WriteLine($"Elasticsearch endpoint: {args[0]}");
            Console.WriteLine($"User name: {args[1]}");
            Console.WriteLine($"Job name: {args[2]}");

            var esService = new ElasticsearchService(args[0], args[1]);
            
            if (args[2].ToLower() == nameof(ReIndexAllJob).ToLower())
            {
                var job = new ReIndexAllJob(esService);
                Task t = job.Run();
                t.Wait();
            }
            else if (args[2].ToLower() == nameof(AddAppPathToTagsJob).ToLower())
            {
                var job = new AddAppPathToTagsJob(esService);
                Task t = job.Run();
                t.Wait();
            }
            else
            {
                throw new Exception($"Unknown job '{args[2]}'");
            }

            Console.WriteLine($"Job completed. Press key to exit.");
            Console.ReadKey();
        }
    }
}
