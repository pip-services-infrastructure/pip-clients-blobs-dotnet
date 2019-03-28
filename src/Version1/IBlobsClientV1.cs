using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using PipServices3.Commons.Data;

namespace PipServices.Blobs.Client.Version1
{
    public interface IBlobsClientV1
    {
        Task<DataPage<BlobInfoV1>> GetBlobsByFilterAsync(string correlationId,
           FilterParams filter, PagingParams paging);
        Task<List<BlobInfoV1>> GetBlobsByIdsAsync(string correlationId, string[] ids);
        Task<BlobInfoV1> GetBlobByIdAsync(string correlationId, string id);

        Task<BlobInfoV1> CreateBlobFromUriAsync(string correlationId, BlobInfoV1 blob, string uri);
        Task<string> GetBlobUriByIdAsync(string correlationId, string id);

        Task<BlobInfoV1> CreateBlobFromDataAsync(string correlationId, BlobInfoV1 blob, byte[] buffer);
        Task<byte[]> GetBlobDataByIdAsync(string correlationId, string id);

        Task<BlobInfoV1> CreateBlobFromStreamAsync(string correlationId, BlobInfoV1 blob, Stream stream);
        Task ReadBlobStreamByIdAsync(string correlationId, string id, Stream stream);

        Task<BlobInfoV1> UpdateBlobInfoAsync(string correlationId, BlobInfoV1 blob);
        Task MarkBlobsCompletedAsync(string correlationId, string[] ids);
        Task DeleteBlobByIdAsync(string correlationId, string id);
        Task DeleteBlobsByIdsAsync(string correlationId, string[] ids);
    }
}
