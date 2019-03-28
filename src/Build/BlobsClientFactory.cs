using PipServices3.Components.Build;
using PipServices3.Commons.Refer;
using PipServices.Blobs.Client.Version1;

namespace PipServices.Blobs.Client.Build
{
    public class BlobsClientFactory : Factory
    {
        public static Descriptor Descriptor = new Descriptor("pip-services-blobs", "factory", "client", "default", "1.0");
        public static Descriptor NullClientDescriptor = new Descriptor("pip-services-blobs", "client", "null", "*", "1.0");
        public static Descriptor HttpClientDescriptor = new Descriptor("pip-services-blobs", "client", "http", "*", "1.0");
        public static Descriptor AwsClientDescriptor = new Descriptor("pip-services-blobs", "client", "aws", "*", "1.0");

        public BlobsClientFactory()
        {
            RegisterAsType(NullClientDescriptor, typeof(BlobsNullClientV1));
            RegisterAsType(HttpClientDescriptor, typeof(BlobsHttpClientV1));
            RegisterAsType(AwsClientDescriptor, typeof(BlobsAwsClientV1));
        }
    }}
