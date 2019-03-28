using PipServices.Blobs.Client.Version1;
using PipServices3.Commons.Config;
using PipServices3.Commons.Data;
using System;
using System.IO;

namespace run
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Press ENTER to exit...");
            Console.ReadLine();

            try
            {
                var correlationId = "123";
                var config = ConfigParams.FromTuples(
                    "connection.type", "http",
                    "connection.host", "localhost",
                    "connection.port", 8080
                );
                var client = new BlobsHttpClientV1();
                client.Configure(config);
                client.OpenAsync(correlationId);
                var readStream = new MemoryStream(new byte[] { 1, 2, 3, 4, 5, 6 });
                var blobId = IdGenerator.NextLong();
                var blob1 = client.CreateBlobFromStreamAsync(null, new BlobInfoV1
                {
                    Id = blobId,
                    Group = "test",
                    Name = "file-" + blobId + ".dat",
                    Size = 6,
                    ContentType = "application/binary"
                }, readStream);

                Console.WriteLine("Press ENTER to exit...");
                Console.ReadLine();

                client.CloseAsync(string.Empty);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
            }
        }
    }
}
