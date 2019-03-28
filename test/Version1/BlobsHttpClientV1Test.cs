using System;
using System.Threading;
using System.Threading.Tasks;
using PipServices.Blobs.Client.Version1;
using PipServices3.Commons.Config;
using Xunit;

namespace PipServices.Blobs.Client.Test.Version1
{
    public class BlobsHttpClientV1Test: IDisposable
    {
        private static readonly ConfigParams HttpConfig = ConfigParams.FromTuples(
            "options.chunk_size", 3,
            "connection.protocol", "http",
            "connection.host", "localhost",
            "connection.port", 8080
        );

        private BlobsHttpClientV1 _client;
        private BlobsClientV1Fixture _fixture;

        public BlobsHttpClientV1Test()
        {
            _client = new BlobsHttpClientV1();

            _client.Configure(HttpConfig);

            _fixture = new BlobsClientV1Fixture(_client);

            _client.OpenAsync(null).Wait();

            Thread.Sleep(1000); // Just let service a sec to be initialized

            _client.OpenAsync(null).Wait();
        }

        public void Dispose()
        {
            _client.CloseAsync(null).Wait();
        }

        [Fact]
        public async Task TestBlobOperationsAsync()
        {
            await _fixture.TestBlobOperationsAsync();
        }

        [Fact]
        public async Task TestReadingAndWritingDataAsync()
        {
            await _fixture.TestReadingAndWritingDataAsync();
        }

        [Fact]
        public async Task TestWritingBlobUriAsync()
        {
            await _fixture.TestWritingBlobUriAsync();
        }

    }
}
