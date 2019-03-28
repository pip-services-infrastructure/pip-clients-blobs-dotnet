using System;
using System.Threading.Tasks;
using PipServices.Blobs.Client.Version1;
using PipServices3.Commons.Config;
using PipServices3.Commons.Convert;
using Xunit;

namespace PipServices.Blobs.Client.Test.Version1
{
    public class BlobsAwsClientV1Test
    {
        private bool _enabled = false;
        private BlobsAwsClientV1 _client;
        private BlobsClientV1Fixture _fixture;

        public BlobsAwsClientV1Test()
        {
            //var AWS_ENABLED = Environment.GetEnvironmentVariable("AWS_ENABLED") ?? "true";
            //var AWS_REGION = Environment.GetEnvironmentVariable("AWS_REGION") ?? "us-east-1";
            //var AWS_ACCOUNT = Environment.GetEnvironmentVariable("AWS_ACCOUNT");
            //var AWS_ACCESS_ID = Environment.GetEnvironmentVariable("AWS_ACCESS_ID") ?? "";
            //var AWS_ACCESS_KEY = Environment.GetEnvironmentVariable("AWS_ACCESS_KEY") ?? "";
            //var AWS_BUCKET = Environment.GetEnvironmentVariable("AWS_BUCKET") ?? "";

            //_enabled = BooleanConverter.ToBoolean(AWS_ENABLED);

            if (_enabled)
            {
                var config = ConfigParams.FromTuples(
                    //"connection.region", AWS_REGION,
                    //"connection.account", AWS_ACCOUNT,
                    //"connection.resource", AWS_BUCKET,
                    //"credential.access_id", AWS_ACCESS_ID,
                    //"credential.access_key", AWS_ACCESS_KEY
                );

                _client = new BlobsAwsClientV1();
                _client.Configure(config);
                _client.OpenAsync(null).Wait();
                _client.ClearAsync(null).Wait();

                _fixture = new BlobsClientV1Fixture(_client);
            }
        }

        [Fact]
        public async Task TestBlobOperationsAsync()
        {
            if (_enabled)
                await _fixture.TestBlobOperationsAsync();
        }

        [Fact]
        public async Task TestReadingAndWritingDataAsync()
        {
            if (_enabled)
                await _fixture.TestReadingAndWritingDataAsync();
        }

        [Fact]
        public async Task TestWritingBlobUriAsync()
        {
            if (_enabled)
                await _fixture.TestWritingBlobUriAsync();
        }

    }
}
