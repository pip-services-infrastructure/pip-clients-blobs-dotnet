using PipServices3.Commons.Data;
using System;
using System.Runtime.Serialization;

namespace PipServices.Blobs.Client.Version1
{
    [DataContract]
    public class BlobInfoV1 : IStringIdentifiable
    {
        /* Identification */
        [DataMember(Name = "id")]
        public string Id { get; set; }

        [DataMember(Name = "group")]
        public string Group { get; set; }

        [DataMember(Name = "name")]
        public string Name { get; set; }

        /* Content */
        [DataMember(Name = "size")]
        public long Size { get; set; }

        [DataMember(Name = "content_type")]
        public string ContentType { get; set; }

        [DataMember(Name = "create_time")]
        public DateTime CreateTime { get; set; }

        [DataMember(Name = "expire_time")]
        public DateTime? ExpireTime { get; set; }

        [DataMember(Name = "completed")]
        public bool Completed { get; set; }

    }
}
