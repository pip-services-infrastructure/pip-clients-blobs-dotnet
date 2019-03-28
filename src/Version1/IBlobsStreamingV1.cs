using System.Threading.Tasks;

namespace PipServices.Blobs.Client.Version1
{
    public interface IBlobsStreamingV1
    {
        Task<string> BeginBlobWriteAsync(string correlationId, BlobInfoV1 item);
        Task<string> WriteBlobChunkAsync(string correlationId, string token, byte[] buffer);
        Task<BlobInfoV1> EndBlobWriteAsync(string correlationId, string token, byte[] buffer);
        Task AbortBlobWriteAsync(string correlationId, string token);

        Task<BlobInfoV1> BeginBlobReadAsync(string correlationId, string id);
        Task<byte[]> ReadBlobChunkAsync(string correlationId, string id, long skip, int take);
        Task EndBlobReadAsync(string correlationId, string id);
    }
}
