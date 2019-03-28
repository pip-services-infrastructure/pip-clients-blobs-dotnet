using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using PipServices3.Commons.Config;
using PipServices3.Commons.Data;
using PipServices3.Rpc.Clients;

namespace PipServices.Blobs.Client.Version1
{
    public class BlobsHttpClientV1: CommandableHttpClient, IBlobsClientV1, IBlobsStreamingV1
    {
        private BlobsStreamingControllerV1 _streamingController;

        public BlobsHttpClientV1()
            : base("v1/blobs") 
        {
            _streamingController = new BlobsStreamingControllerV1(this);
        }

        public override void Configure(ConfigParams config)
        {
            base.Configure(config);

            _streamingController.Configure(config);
        }

        public async Task<DataPage<BlobInfoV1>> GetBlobsByFilterAsync(string correlationId,
            FilterParams filter, PagingParams paging)
        {
            return await CallCommandAsync<DataPage<BlobInfoV1>>(
                "get_blobs_by_filter",
                correlationId,
                new { filter = filter, paging = paging }
            );
        }

        public async Task<List<BlobInfoV1>> GetBlobsByIdsAsync(string correlationId, string[] ids)
        {
            return await CallCommandAsync<List<BlobInfoV1>>(
                "get_blobs_by_ids",
                correlationId,
                new { blob_ids = ids }
            );
        }

        public async Task<BlobInfoV1> GetBlobByIdAsync(string correlationId, string id)
        {
            return await CallCommandAsync<BlobInfoV1>(
                "get_blob_by_id",
                correlationId,
                new { blob_id = id }
            );
        }

        public async Task<BlobInfoV1> CreateBlobFromUriAsync(string correlationId, BlobInfoV1 blob, string uri)
        {
            return await _streamingController.CreateBlobFromUriAsync(correlationId, blob, uri);
        }

        public async Task<string> GetBlobUriByIdAsync(string correlationId, string id)
        {
            return await CallCommandAsync<string>(
                "get_blob_uri_by_id",
                correlationId,
                new { blob_id = id }
            );
        }

        public async Task<BlobInfoV1> CreateBlobFromDataAsync(string correlationId, BlobInfoV1 blob, byte[] buffer)
        {
            return await _streamingController.CreateBlobFromDataAsync(correlationId, blob, buffer);
        }

        public async Task<byte[]> GetBlobDataByIdAsync(string correlationId, string id)
        {
            return await _streamingController.GetBlobDataByIdAsync(correlationId, id);
        }

        public async Task<BlobInfoV1> CreateBlobFromStreamAsync(string correlationId, BlobInfoV1 blob, Stream stream)
        {
            return await _streamingController.CreateBlobFromStreamAsync(correlationId, blob, stream);
        }

        public async Task ReadBlobStreamByIdAsync(string correlationId, string id, Stream stream)
        {
            await _streamingController.ReadBlobStreamByIdAsync(correlationId, id, stream);
        }

        public async Task MarkBlobsCompletedAsync(string correlationId, string[] ids)
        {
            await CallCommandAsync<object>(
                "mark_blobs_completed",
                correlationId,
                new { blob_ids = ids }
            );
        }

        public async Task<BlobInfoV1> UpdateBlobInfoAsync(string correlationId, BlobInfoV1 blob)
        {
            return await CallCommandAsync<BlobInfoV1>(
                "update_blob_info",
                correlationId,
                new { blob = blob }
            );
        }

        public async Task DeleteBlobByIdAsync(string correlationId, string id)
        {
            await CallCommandAsync<object>(
                "delete_blob_by_id",
                correlationId,
                new { blob_id = id }
            );
        }

        public async Task DeleteBlobsByIdsAsync(string correlationId, string[] ids)
        {
            await CallCommandAsync<object>(
                "delete_blobs_by_ids",
                correlationId,
                new { blob_ids = ids }
            );
        }

        ////////// Blobs streaming API //////////

        public async Task<string> BeginBlobWriteAsync(string correlationId, BlobInfoV1 item)
        {
            return await CallCommandAsync<string>(
                "begin_blob_write",
                correlationId,
                new { blob = item }
            );
        }

        public async Task<string> WriteBlobChunkAsync(string correlationId, string token, byte[] buffer)
        {
            var chunk = buffer != null ? Convert.ToBase64String(buffer) : null;

            return await CallCommandAsync<string>(
                "write_blob_chunk",
                correlationId,
                new { token = token, chunk = chunk }
            );
        }

        public async Task<BlobInfoV1> EndBlobWriteAsync(string correlationId, string token, byte[] buffer)
        {
            var chunk = buffer != null ? Convert.ToBase64String(buffer) : null;

            return await CallCommandAsync<BlobInfoV1>(
                "end_blob_write",
                correlationId,
                new { token = token, chunk = chunk }
            );
        }

        public async Task AbortBlobWriteAsync(string correlationId, string token)
        {
            await CallCommandAsync<object>(
                "abort_blob_write",
                correlationId,
                new { token = token }
            );
        }

        public async Task<BlobInfoV1> BeginBlobReadAsync(string correlationId, string id)
        {
            return await CallCommandAsync<BlobInfoV1>(
                "begin_blob_read",
                correlationId,
                new { blob_id = id }
            );
        }

        public async Task<byte[]> ReadBlobChunkAsync(string correlationId, string id, long skip, int take)
        {
            var chunk = await CallCommandAsync<string>(
                "read_blob_chunk",
                correlationId,
                new { blob_id = id, skip = skip, take = take }
            );

            var buffer = !string.IsNullOrEmpty(chunk) ? Convert.FromBase64String(chunk) : null;
            return buffer;
        }

        public async Task EndBlobReadAsync(string correlationId, string id)
        {
            await CallCommandAsync<object>(
                "end_blob_read",
                correlationId,
                new { blob_id = id }
            );
        }

    }
}
