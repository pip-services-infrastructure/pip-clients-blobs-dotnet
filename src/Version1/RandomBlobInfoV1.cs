using PipServices3.Commons.Data;
using PipServices3.Commons.Random;
using System;

namespace PipServices.Blobs.Client.Version1
{
    public static class RandomBlobInfoV1
    {
        public static BlobInfoV1 Blob()
        {
            return new BlobInfoV1
            {
                Id = IdGenerator.NextLong(),
                Group = RandomText.Name(),
                Name = RandomText.Name(),
                Size = RandomLong.NextLong(100, 100000),
                ContentType = RandomArray.Pick(new string[] { "text/plain", "application/binary", "application/json" }),
                CreateTime = DateTime.UtcNow,
                ExpireTime = RandomDateTime.NextDateTime(DateTime.UtcNow, new DateTime(2010, 1, 1)),
                Completed = RandomBoolean.NextBoolean()
            };
        }
    }
}
