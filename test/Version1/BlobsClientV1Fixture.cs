using System.IO;
using System.Threading.Tasks;
using PipServices.Blobs.Client.Version1;
using PipServices3.Commons.Data;
using Xunit;

namespace PipServices.Blobs.Client.Test.Version1
{
    public class BlobsClientV1Fixture
    {
        private BlobInfoV1 BLOB1 = RandomBlobInfoV1.Blob();
        private BlobInfoV1 BLOB2 = RandomBlobInfoV1.Blob();

        private IBlobsClientV1 _client;

        public BlobsClientV1Fixture(IBlobsClientV1 client)
        {
            _client = client;
        }

        public async Task TestBlobOperationsAsync()
        {
            // Writing blob
            var blobId = IdGenerator.NextLong();
            var blob = new BlobInfoV1
            {
                Id = blobId,
                Group = "test",
                Name = "file-" + blobId + ".dat",
                Size = 6,
                ContentType = "application/binary"
            };

            var readStream = new MemoryStream(new byte[] { 1, 2, 3, 4, 5, 6 });
            var blob1 = await _client.CreateBlobFromStreamAsync(null, blob, readStream);

            Assert.NotNull(blob);
            Assert.Equal(blob.Name, blob1.Name);
            Assert.Equal(blob.Group, blob1.Group);
            Assert.Equal(blob.ContentType, blob1.ContentType);
            Assert.Equal(6, blob1.Size);

            // Reading blob
            var writeStream = new MemoryStream();
            await _client.ReadBlobStreamByIdAsync(null, blobId, writeStream);

            Assert.Equal(6, writeStream.Length);

            // Get all blobs
            var blobs = await _client.GetBlobsByFilterAsync(
                null,
                FilterParams.FromTuples("group", "test"),
                new PagingParams()
            );

            Assert.NotNull(blobs);
            Assert.Single(blobs.Data);

            // Get the blob
            blob1 = await _client.GetBlobByIdAsync(null, blobId);

            Assert.NotNull(blob1);
            Assert.Equal(blob.Id, blob1.Id);

            // Update the blob
            blob1.Name = "file1.xxx";

            blob1 = await _client.UpdateBlobInfoAsync(null, blob1);

            Assert.NotNull(blob1);
            Assert.Equal(blob.Id, blob1.Id);
            Assert.Equal("file1.xxx", blob1.Name);

            // Delete the blob
            await _client.DeleteBlobByIdAsync(null, blobId);

            // Delete all blobs
            await _client.DeleteBlobsByIdsAsync(null, new string[] { blobId });

            // Try to get deleted file
            blob1 = await _client.GetBlobByIdAsync(null, blobId);

            Assert.Null(blob1);
        }       

        public async Task TestReadingAndWritingDataAsync()
        {
            // Writing blob
            var blobId = IdGenerator.NextLong();
            var blob = new BlobInfoV1
            {
                Id = blobId,
                Group = "test",
                Name = "file-" + blobId + ".dat",
                Size = 6,
                ContentType = "application/binary"
            };

            var data = new byte[] { 1, 2, 3, 4, 5, 6 };
            var blob1 = await _client.CreateBlobFromDataAsync(null, blob, data);

            Assert.NotNull(blob);
            Assert.Equal(blob.Name, blob1.Name);
            Assert.Equal(blob.Group, blob1.Group);
            Assert.Equal(blob.ContentType, blob1.ContentType);
            Assert.Equal(6, blob1.Size);

            // Reading blob
            data = await _client.GetBlobDataByIdAsync(null, blobId);

            Assert.Equal(6, data.Length);
        }   

        public async Task TestWritingBlobUriAsync()
        {
            // Writing blob
            var blobId = IdGenerator.NextLong();
            var blob = new BlobInfoV1
            {
                Id = blobId,
                Group = "test",
                Name = "file-" + blobId + ".dat",
                ContentType = "text/html"
            };

            var blob1 = await _client.CreateBlobFromUriAsync(null, blob, "https://www.google.com/images/branding/googlelogo/2x/googlelogo_color_272x92dp.png");

            Assert.NotNull(blob);
            Assert.Equal(blob.Name, blob1.Name);
            Assert.Equal(blob.Group, blob1.Group);
            Assert.Equal(blob.ContentType, blob1.ContentType);
            Assert.True(blob1.Size > 0);

            // Reading blob
            var data = await _client.GetBlobDataByIdAsync(null, blobId);

            Assert.True(data.Length > 0);
        }   

    }
}
