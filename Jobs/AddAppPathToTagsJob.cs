using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Jobs
{
    public class AddAppPathToTagsJob
    {
        private readonly ElasticsearchService _esService;

        public AddAppPathToTagsJob(ElasticsearchService esService)
        {
            _esService = esService;
        }

        public async Task Run()
        {
            await _esService.GetAllTagsAndUpdateAppPathFromPicture();
        }
    }
}
