using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Jobs
{
    public class ReIndexAllJob
    {
        private readonly ElasticsearchService _esService;
        private int _globalIndexCounter;

        public ReIndexAllJob(ElasticsearchService esService)
        {
            _esService = esService;
            _globalIndexCounter = 1;
        }

        public async Task Run()
        {
            Console.WriteLine("Getting all albums...");
            var allAlbums = await _esService.GetAllAlbums();

            foreach (var albumId in allAlbums)
                await IndexAlbum(albumId);

            Console.WriteLine($"Indexed {allAlbums.Count()} albums - {_globalIndexCounter-1} pictures (max global index).");
        }

        async Task IndexAlbum(string albumId)
        {
            Console.WriteLine("Indexing album " + albumId);
            int from = 0, size = 100, albumIndexCounter = 1;

            while (true)
            {
                Console.WriteLine($"Indexing album pictures from {from} to {from + size} ...");
                var pictures = await _esService.GetAlbumPictures(albumId, from, size);
                if (pictures.Count() == 0)
                    break;
                    
                foreach (var picture in pictures)
                {
                    picture.GlobalSortOrder = _globalIndexCounter++;
                    picture.FolderSortOrder = albumIndexCounter++;

                    await _esService.UpdatePicture(picture);
                }

                from += size;
            }
        }
    }
}
