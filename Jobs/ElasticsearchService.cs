using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Nest;

namespace Jobs
{
    public class ElasticsearchService
    {
        private readonly IElasticClient _client;
        private readonly string _userName;
        
        public ElasticsearchService(string elasticsearchEndpoint, string userName)
        {
            var connectionSettings = new ConnectionSettings(
                new Uri(elasticsearchEndpoint)
            );

            _client = new ElasticClient(connectionSettings);
            _userName = userName;
        }

        public async Task<IEnumerable<string>> GetAllAlbums()
        {
            try
            {
                var result = await _client.SearchAsync<AlbumDTO>(s => s
                    .Aggregations(a => a
                        .Terms("my_agg", st => st
                            .Field(f => f.FolderId.Suffix("keyword"))
                            .Size(800)
                        )
                    )
                    .Index($"{_userName}_picture")
                );

                var list = new List<string>();
                foreach (var bucket in result.Aggregations.Terms("my_agg").Buckets)
                    list.Add(bucket.Key);

                return list;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public async Task<IEnumerable<PictureDTO>> GetAlbumPictures(string albumId, int from, int batchSize)
        {
            var searchResponse = await _client.SearchAsync<PictureDTO>(s => s
                    .Query(q => q
                        .Match(m => m
                            .Field(f => f.FolderId.Suffix("keyword"))
                            .Query(albumId)
                        )
                    )
                    .Sort(srt => srt
                        .Ascending(p => p.Name.Suffix("keyword"))
                    )
                    .From(from)
                    .Size(batchSize)
                    .Index($"{_userName}_picture")
                );

            if (!searchResponse.IsValid)
                throw new Exception(searchResponse.DebugInformation);

            return searchResponse.Documents;
        }

        public async Task UpdatePicture(PictureDTO picture)
        {
            var updateResponse = await _client.UpdateAsync<PictureDTO>(picture.Id, u => u
                .Doc(picture)
                .Index($"{_userName}_picture")
            );

            if (!updateResponse.IsValid)
            {
                throw new Exception(updateResponse.DebugInformation);
            }
        }

        public async Task<IEnumerable<TagDTO>> GetAllTagsAndUpdateAppPathFromPicture()
        {
            Console.WriteLine("Initiating scroll search on tags index ...");
            var searchResponse = _client.Search<TagDTO>(s => s
                .Scroll("10s")
                .Index($"{_userName}_tag")
            );

            if (!searchResponse.IsValid)
                throw new Exception(searchResponse.DebugInformation);

            while (searchResponse.Documents.Any())
            {
                await ProcessResponse();
                Console.WriteLine("Continue scrolling ...");
                searchResponse = _client.Scroll<TagDTO>("10s", searchResponse.ScrollId);
            }

            return searchResponse.Documents;

            async Task ProcessResponse()
            {
                // Map the internal _id field to the DTO 'Id' field
                var tags = searchResponse.Hits.Select(h =>
                {
                    h.Source.Id = h.Id;
                    return h.Source;
                }).ToList();

                Console.WriteLine($"Processing a batch of {tags.Count} tags ...");
                foreach (var tag in tags)
                {
                    if (string.IsNullOrWhiteSpace(tag.PictureAppPath))
                    {
                        var pic = GetPicture(tag.PictureId);
                        tag.PictureAppPath = pic.AppPath;
                        
                        await UpdateTag(tag);
                    }
                }
            }
        }

        PictureDTO GetPicture(string id)
        {
            var searchResponse = _client.Search<PictureDTO>(s => s
                    .Query(q => q
                        .Match(m => m
                            .Field(f => f.Id.Suffix("keyword"))
                            .Query(id)
                        )
                    )
                    .Index($"{_userName}_picture")
                );

            if (!searchResponse.IsValid)
                throw new Exception(searchResponse.DebugInformation);

            return searchResponse.Documents.FirstOrDefault();
        }

        async Task UpdateTag(TagDTO tag)
        {
            var updateResponse = await _client.UpdateAsync<TagDTO>(tag.Id, u => u
                .Doc(tag)
                .Index($"{_userName}_tag")
            );

            if (!updateResponse.IsValid)
            {
                throw new Exception(updateResponse.DebugInformation);
            }
        }
    }

    public class AlbumDTO
    {
        public string Id { get; set; }
        public string FolderId { get; set; }
    }

    public class PictureDTO
    {
        public string Id { get; set; }
        public int GlobalSortOrder { get; set; }
        public string Name { get; set; }
        public string FolderId { get; set; }
        public int FolderSortOrder { get; set; }
        public string AppPath { get; set; }
    }

    public class TagDTO
    {
        public string Id { get; set; }
        public string TagName { get; set; }
        public string PictureId { get; set; }
        public string PictureAppPath { get; set; }
        public DateTime Added { get; set; }
    }
}