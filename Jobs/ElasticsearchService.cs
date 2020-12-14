using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Nest;

namespace Jobs
{
    public class ElasticsearchService
    {
        private readonly IElasticClient _client;
        
        public ElasticsearchService(string elasticsearchEndpoint)
        {
            var connectionSettings = new ConnectionSettings(
                new Uri(elasticsearchEndpoint)
            );

            _client = new ElasticClient(connectionSettings);
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
                    .Index("picture")
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
                    .Index("picture")
                );

            if (!searchResponse.IsValid)
                throw new Exception(searchResponse.DebugInformation);

            return searchResponse.Documents;
        }

        public async Task UpdatePicture(PictureDTO picture)
        {
            var updateResponse = await _client.UpdateAsync<PictureDTO>(picture.Id, u => u
                .Doc(picture)
                .Index("picture")
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
    }
}