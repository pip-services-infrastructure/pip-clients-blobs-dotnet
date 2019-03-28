using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using PipServices3.Commons.Data;

namespace PipServices.Blobs.Client.Version1
{
    public class BlobsNullClientV1 : IBlobsClientV1
    {
        public async Task<DataPage<BlobInfoV1>> GetBlobsByFilterAsync(string correlationId,
            FilterParams filter, PagingParams paging)
        {
            return await Task.FromResult(new DataPage<BlobInfoV1>());
        }

        public async Task<List<BlobInfoV1>> GetBlobsByIdsAsync(string correlationId, string[] ids)
        {
            return await Task.FromResult(new List<BlobInfoV1>());
        }

        public async Task<BlobInfoV1> GetBlobByIdAsync(string correlationId, string id)
        {
            return await Task.FromResult<BlobInfoV1>(null);
        }

        public async Task<BlobInfoV1> CreateBlobFromUriAsync(string correlationId, BlobInfoV1 blob, string uri)
        {
            return await Task.FromResult(blob);
        }

        public async Task<string> GetBlobUriByIdAsync(string correlationId, string id)
        {
            return await Task.FromResult<string>(null);
        }

        public async Task<BlobInfoV1> CreateBlobFromDataAsync(string correlationId, BlobInfoV1 blob, byte[] buffer)
        {
            return await Task.FromResult(blob);
        }

        public async Task<byte[]> GetBlobDataByIdAsync(string correlationId, string id)
        {
            return await Task.FromResult<byte[]>(null);
        }

        public async Task<BlobInfoV1> CreateBlobFromStreamAsync(string correlationId, BlobInfoV1 blob, Stream stream)
        {
            return await Task.FromResult(blob);
        }

        public async Task ReadBlobStreamByIdAsync(string correlationId, string id, Stream stream)
        {
            await Task.Delay(0);
        }

        public async Task MarkBlobsCompletedAsync(string correlationId, string[] ids)
        {
            await Task.Delay(0);
        }

        public async Task<BlobInfoV1> UpdateBlobInfoAsync(string correlationId, BlobInfoV1 blob)
        {
            return await Task.FromResult(blob);
        }

        public async Task DeleteBlobByIdAsync(string correlationId, string id)
        {
            await Task.Delay(0);
        }

        public async Task DeleteBlobsByIdsAsync(string correlationId, string[] ids)
        {
            await Task.Delay(0);
        }
    }
}
