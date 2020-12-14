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

            var esService = new ElasticsearchService("http://localhost:9200");
            var job = new ReIndexAllJob(esService);
            
            Task t = job.Run();
            t.Wait();
        }

    }
}
