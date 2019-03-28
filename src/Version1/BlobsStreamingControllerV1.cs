using System;
using System.IO;
using System.Net;
using System.Threading.Tasks;
using PipServices3.Commons.Config;
using PipServices3.Commons.Data;

namespace PipServices.Blobs.Client.Version1
{
    public class BlobsStreamingControllerV1: IConfigurable
    {
        private int _chunkSize = 4 * 1024 * 1024;
        private IBlobsStreamingV1 _client;

        public BlobsStreamingControllerV1(IBlobsStreamingV1 client)
        {
            _client = client;
        }

        public void Configure(ConfigParams config)
        {
            _chunkSize = config.GetAsIntegerWithDefault("options.chunk_size", _chunkSize);
        }

        public async Task<BlobInfoV1> CreateBlobFromUriAsync(string correlationId, BlobInfoV1 blob, string uri)
        {
            var request = WebRequest.Create(uri);
            var response = request.GetResponse();
            using (var stream = response.GetResponseStream())
            {
                blob.ContentType = response.ContentType;
                blob.Size = response.ContentLength;
                return await CreateBlobFromStreamAsync(correlationId, blob, stream);
            }
        }

        public async Task<BlobInfoV1> CreateBlobFromDataAsync(string correlationId, BlobInfoV1 blob, byte[] buffer)
        {
            using (var stream = new MemoryStream(buffer))
            {
                return await CreateBlobFromStreamAsync(correlationId, blob, stream);
            }
        }

        public async Task<byte[]> GetBlobDataByIdAsync(string correlationId, string id)
        {
            var stream = new MemoryStream();
            await ReadBlobStreamByIdAsync(correlationId, id, stream);
            return stream.ToArray();
        }

        public async Task<BlobInfoV1> CreateBlobFromStreamAsync(string correlationId, BlobInfoV1 blob, Stream stream)
        {
            // Generate file id
            blob.Id = blob.Id ?? IdGenerator.NextLong();
            blob.CreateTime = DateTime.UtcNow;

            // Start writing
            var token = await _client.BeginBlobWriteAsync(correlationId, blob);

            // Write in chunks
            var buffer = new byte[_chunkSize];
            while (true)
            {
                var size = await stream.ReadAsync(buffer, 0, buffer.Length);

                if (size == 0)
                    break;

                var chunk = buffer;

                if (size != buffer.Length)
                {
                    chunk = new byte[size];
                    Array.Copy(buffer, 0, chunk, 0, size);
                    //                    Array.Resize(ref buffer, size);
                }
                
                token = await _client.WriteBlobChunkAsync(correlationId, token, chunk);
            }

            // Finish writing and return blobId
            blob = await _client.EndBlobWriteAsync(correlationId, token, new byte[] { });

            return blob;
        }

        public async Task ReadBlobStreamByIdAsync(string correlationId, string id, Stream stream)
        {
            // Start reading
            var blob = await _client.BeginBlobReadAsync(correlationId, id);

            // Read in chunks
            var start = 0;
            while (true)
            {
                var buffer = await _client.ReadBlobChunkAsync(correlationId, id, start, _chunkSize);

                // Protection against infinite loop
                if (buffer != null && buffer.Length > 0)
                {
                    await stream.WriteAsync(buffer, 0, buffer.Length);
                    start += buffer.Length;

                    if (buffer.Length < _chunkSize)
                        break;
                }
                else
                {
                    break;
                }
            }

            // Finish reading
            await _client.EndBlobReadAsync(correlationId, id);
        }
    }
}
