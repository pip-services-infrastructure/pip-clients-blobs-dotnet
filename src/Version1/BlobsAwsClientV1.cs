using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using System.Web;
using Amazon;
using Amazon.S3;
using Amazon.S3.Model;
using PipServices3.Aws.Connect;
using PipServices3.Commons.Config;
using PipServices3.Commons.Convert;
using PipServices3.Commons.Data;
using PipServices3.Commons.Errors;
using PipServices3.Commons.Refer;
using PipServices3.Commons.Run;
using PipServices3.Components.Count;
using PipServices3.Components.Log;

namespace PipServices.Blobs.Client.Version1
{
    public class BlobsAwsClientV1 : IBlobsClientV1, IBlobsStreamingV1,
        IConfigurable, IReferenceable, IOpenable, ICleanable
    {
        private CompositeLogger _logger = new CompositeLogger();
        private CompositeCounters _counters = new CompositeCounters();
        private AwsConnectionResolver _connectionResolver = new AwsConnectionResolver();
        private TempBlobStorage _storage = new TempBlobStorage("./data/temp");
        private BlobsStreamingControllerV1 _streamingController;
        private AmazonS3Client _client;

        private int _connectTimeout = 30000;
        private long _minChunkSize = 5 * 1024 * 1024;
        private long _maxBlobSize = 100 * 1024;
        private bool _reducedRedundancy = true;
        private long _maxTake = 100;
        private string _bucket;

        public BlobsAwsClientV1()
        {
            _streamingController = new BlobsStreamingControllerV1(this);
        }

        public void Configure(ConfigParams config)
        {
            _logger.Configure(config);
            _connectionResolver.Configure(config);
            _storage.Configure(config);
            _streamingController.Configure(config);

            _minChunkSize = config.GetAsLongWithDefault("options.min_chunk_size", _minChunkSize);
            _maxBlobSize = config.GetAsLongWithDefault("options.max_blob_size", _maxBlobSize);
            _reducedRedundancy = config.GetAsBooleanWithDefault("options.reduced_redundancy", _reducedRedundancy);
            _connectTimeout = config.GetAsIntegerWithDefault("options.connect_timeout", _connectTimeout);
            _maxTake = config.GetAsLongWithDefault("options.max_take", _maxTake);
        }

        public void SetReferences(IReferences references)
        {
            _logger.SetReferences(references);
            _counters.SetReferences(references);
            _connectionResolver.SetReferences(references);
        }

        public bool IsOpen()
        {
            return _client != null;
        }

        private void CheckOpened(string correlationId)
        {
            if (_client == null)
            {
                throw new InvalidStateException(
                    correlationId, "NOT_OPENED", "Component is not opened"
                );
            }
        }

        public async Task OpenAsync(string correlationId)
        {
            if (IsOpen()) return;

            await _storage.OpenAsync(correlationId);

            var awsConnection = await _connectionResolver.ResolveAsync(correlationId);

            // Assign service name
            awsConnection.ResourceType = "s3";

            if (!string.IsNullOrEmpty(awsConnection.Resource))
                _bucket = awsConnection.Resource;

            if (!string.IsNullOrEmpty(awsConnection.GetAsNullableString("bucket")))
                _bucket = awsConnection.GetAsNullableString("bucket");

            // Validate connection params
            var err = awsConnection.Validate(correlationId);
            if (err != null) throw err;

            // Create client
            var region = RegionEndpoint.GetBySystemName(awsConnection.Region);
            var config = new AmazonS3Config()
            {
                RegionEndpoint = region
            };
            _client = new AmazonS3Client(awsConnection.AccessId, awsConnection.AccessKey, config);

            _logger.Info(correlationId, "Connected to S3 bucket " + _bucket);
        }

        public async Task CloseAsync(string correlationId)
        {
            await _storage.CloseAsync(correlationId);

            _logger.Info(correlationId, "Disconnected from S3 bucket " + _bucket);
            _client = null;

            await Task.Delay(0);
        }

        public async Task ClearAsync(string correlationId)
        {
            CheckOpened(correlationId);

            var result = await _client.ListObjectsAsync(new ListObjectsRequest
            {
                BucketName = _bucket
            });

            if (result.S3Objects.Count > 0)
            {
                var objectKeys = new List<KeyVersion>();
                foreach (var obj in result.S3Objects)
                {
                    objectKeys.Add(new KeyVersion { Key = obj.Key });
                }

                await _client.DeleteObjectsAsync(new DeleteObjectsRequest
                {
                    BucketName = _bucket,
                    Objects = objectKeys
                });
            }
        }

        private string EncodeString(string value)
        {
            if (value == null) return null;
            return HttpUtility.UrlEncode(value);
        }

        private string DecodeString(string value)
        {
            if (value == null) return null;
            return HttpUtility.UrlDecode(value);
        }

        private bool MatchString(string value, string search)
        {
            if (value == null && search == null)
                return true;
            if (value == null || search == null)
                return false;
            return value.ToLower().IndexOf(search, StringComparison.InvariantCulture) >= 0;
        }

        private bool MatchSearch(BlobInfoV1 item, string search)
        {
            search = search.ToLower();
            if (MatchString(item.Name, search))
                return true;
            if (MatchString(item.Group, search))
                return true;
            return false;
        }

        private IList<Func<BlobInfoV1, bool>> ComposeFilter(FilterParams filter)
        {
            var result = new List<Func<BlobInfoV1, bool>>();

            filter = filter ?? new FilterParams();

            var search = filter.GetAsNullableString("search");
            if (!string.IsNullOrEmpty(search))
                result.Add(record => MatchSearch(record, search));

            var id = filter.GetAsNullableString("id");
            if (!string.IsNullOrEmpty(id))
                result.Add(record => record.Id == id);

            var name = filter.GetAsNullableString("name");
            if (!string.IsNullOrEmpty(name))
                result.Add(record => record.Name == name);

            var group = filter.GetAsNullableString("group");
            if (!string.IsNullOrEmpty(group))
                result.Add(record => record.Group == group);

            var completed = filter.GetAsNullableBoolean("completed");
            if (completed != null)
                result.Add(record => record.Completed == completed.Value);

            var expired = filter.GetAsNullableBoolean("expired");
            if (expired != null)
            {
                var now = DateTime.UtcNow;
                if (expired.Value == true)
                    result.Add(record => record.ExpireTime.HasValue && record.ExpireTime.Value <= now);
                else
                    result.Add(record => !record.ExpireTime.HasValue || record.ExpireTime.Value > now);
            }

            var fromCreateTime = filter.GetAsNullableDateTime("from_create_time");
            if (fromCreateTime != null)
                result.Add(record => record.CreateTime >= fromCreateTime.Value);

            var toCreateTime = filter.GetAsNullableDateTime("to_create_time");
            if (toCreateTime != null)
                result.Add(record => record.CreateTime < toCreateTime.Value);

            return result;
        }

        public async Task<DataPage<BlobInfoV1>> GetBlobsByFilterAsync(string correlationId,
           FilterParams filter, PagingParams paging)
        {
            var filterFuncs = ComposeFilter(filter);

            paging = paging ?? new PagingParams();
            var skip = paging.GetSkip(0);
            var take = paging.GetTake(_maxTake);

            var data = new List<BlobInfoV1>();
            var completed = false;
            string token = null;

            while (completed == false && data.Count < take)
            {
                var result = await _client.ListObjectsAsync(new ListObjectsRequest
                {
                    BucketName = _bucket,
                    Marker = token,
                    MaxKeys = (int)_maxTake
                });

                token = result.NextMarker;

                // If nothing is returned then exit
                if (result.S3Objects.Count == 0)
                {
                    completed = true;
                    break;
                }

                // Extract ids and retrieve objects
                var ids = new string[result.S3Objects.Count];
                for (var index = 0; index < ids.Length; index++)
                    ids[index] = result.S3Objects[index].Key;

                var items = await GetBlobsByIdsAsync(correlationId, ids);

                // Add items to data
                foreach (var item in items)
                {
                    // Filter items using provided criteria
                    var selected = true;
                    foreach (var filterFunc in filterFuncs)
                    {
                        selected = selected && filterFunc(item);
                    }
                    if (!selected) continue;


                    // Continue if skipped completely
                    if (skip > 0)
                    {
                        skip--;
                        continue;
                    }

                    // Include items until page is over
                    if (take > 0)
                    {
                        data.Add(item);
                        take--;
                    }
                }

                if (!result.IsTruncated)
                    break;
            }

            return new DataPage<BlobInfoV1>(data, 0);
        }

        public async Task<List<BlobInfoV1>> GetBlobsByIdsAsync(string correlationId, string[] ids)
        {
            var items = new List<BlobInfoV1>();

            Parallel.ForEach(ids, (id) =>
            {
                var item = GetBlobByIdAsync(correlationId, id).Result;
                if (item != null)
                    items.Add(item);
            });

            return await Task.FromResult(items);
        }

        public async Task<BlobInfoV1> GetBlobByIdAsync(string correlationId, string id)
        {
            CheckOpened(correlationId);

            try
            {
                var result = await _client.GetObjectMetadataAsync(new GetObjectMetadataRequest
                {
                    BucketName = _bucket,
                    Key = id
                });

                var item = new BlobInfoV1
                {
                    Id = id,
                    Group = DecodeString(result.Metadata["group"]),
                    Name = DecodeString(result.Metadata["name"]),
                    Size = result.ContentLength,
                    ContentType = result.Headers.ContentType,
                    CreateTime = result.LastModified,
                    ExpireTime = result.Headers.Expires,
                    Completed = BooleanConverter.ToBoolean(result.Metadata["completed"])
                };

                return item;
            }
            catch (AmazonS3Exception ex)
            {
                if (ex.ErrorCode == "NotFound")
                    return null;

                throw;
            }
        }

        public async Task<BlobInfoV1> UpdateBlobInfoAsync(string correlationId, BlobInfoV1 item)
        {
            item.Group = EncodeString(item.Group);
            item.Name = EncodeString(item.Name);
            var filename = item.Name ?? (item.Id + ".dat");

            var request = new CopyObjectRequest
            {
                SourceBucket = _bucket,
                SourceKey = item.Id,
                DestinationBucket = _bucket,
                DestinationKey = item.Id,
                CannedACL = S3CannedACL.PublicRead,
                ContentType = item.ContentType,
                StorageClass = _reducedRedundancy ? S3StorageClass.ReducedRedundancy : S3StorageClass.Standard
            };

            request.Headers.ContentDisposition = "inline; filename=" + filename;
            if (item.ExpireTime != null)
                request.Headers.Expires = item.ExpireTime;

            request.Metadata.Add("name", item.Name);
            request.Metadata.Add("group", item.Group);
            request.Metadata.Add("completed", StringConverter.ToString(item.Completed));

            await _client.CopyObjectAsync(request);

            return item;
        }

        public async Task MarkBlobsCompletedAsync(string correlationId, string[] ids)
        {
            Parallel.ForEach(ids, (id) =>
            {
                var item = GetBlobByIdAsync(correlationId, id).Result;
                if (!item.Completed)
                {
                    item.Completed = true;
                    UpdateBlobInfoAsync(correlationId, item).Wait();
                }
            });

            await Task.Delay(0);
        }

        public async Task DeleteBlobByIdAsync(string correlationId, string id)
        {
            try
            {
                await _client.DeleteObjectAsync(new DeleteObjectRequest
                {
                    BucketName = _bucket,
                    Key = id
                });
            }
            catch (AmazonS3Exception ex)
            {
                if (ex.ErrorCode == "NotFound")
                    return;

                throw;
            }
        }

        public async Task DeleteBlobsByIdsAsync(string correlationId, string[] ids)
        {
            try
            {
                var objectKeys = new List<KeyVersion>();
                foreach (var id in ids)
                {
                    objectKeys.Add(new KeyVersion { Key = id });
                }

                await _client.DeleteObjectsAsync(new DeleteObjectsRequest
                {
                    BucketName = _bucket,
                    Objects = objectKeys
                });
            }
            catch (AmazonS3Exception ex)
            {
                if (ex.ErrorCode == "NotFound")
                    return;

                throw;
            }
        }

        public async Task<string> GetBlobUriByIdAsync(string correlationId, string id)
        {
            CheckOpened(correlationId);

            var uri = _client.GetPreSignedURL(new GetPreSignedUrlRequest
            {
                BucketName = _bucket,
                Key = id,
                Expires = DateTime.Now.AddDays(1)
            });

            return await Task.FromResult(uri);
        }

        public async Task<BlobInfoV1> CreateBlobFromUriAsync(string correlationId, BlobInfoV1 blob, string uri)
        {
            return await _streamingController.CreateBlobFromUriAsync(correlationId, blob, uri);
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

        ////////// Blobs streaming API //////////

        public async Task<string> BeginBlobWriteAsync(string correlationId, BlobInfoV1 item)
        {
            CheckOpened(correlationId);

            item.Id = item.Id ?? IdGenerator.NextLong();
            item.Group = EncodeString(item.Group);
            item.Name = EncodeString(item.Name);
            var filename = item.Name ?? item.Id + ".dat";

            var request = new InitiateMultipartUploadRequest
            {
                BucketName = _bucket,
                Key = item.Id,
                CannedACL = S3CannedACL.PublicRead,
                ContentType = item.ContentType,
                StorageClass = _reducedRedundancy ? S3StorageClass.ReducedRedundancy : S3StorageClass.Standard
            };

            request.Headers.ContentDisposition = "inline; filename=" + filename;
            if (item.ExpireTime != null)
                request.Headers.Expires = item.ExpireTime;

            request.Metadata.Add("name", item.Name);
            request.Metadata.Add("group", item.Group);
            request.Metadata.Add("completed", StringConverter.ToString(item.Completed));

            var result = await _client.InitiateMultipartUploadAsync(request);

            var token = item.Id + ";" + result.UploadId;

            return token;
        }

        private async Task<string> UploadPartAsync(string correlationId, string token, Stream stream)
        {
            var tokens = (token ?? "").Split(';');

            if (tokens.Length < 2)
            {
                throw new BadRequestException(
                    correlationId,
                    "BAD_TOKEN",
                    "Token " + token + " is invalid"
                ).WithDetails("token", token);
            }

            var result = await _client.UploadPartAsync(new UploadPartRequest
            {
                BucketName = _bucket,
                Key = tokens[0],
                UploadId = tokens[1],
                PartNumber = tokens.Length - 1,
                InputStream = stream
            });

            token = token + ";" + result.ETag;

            return token;
        }

        private async Task<string> UploadAndDeleteChunksAsync(string correlationId, string token)
        {
            var tokens = (token ?? "").Split(';');

            if (tokens.Length < 2)
            {
                throw new BadRequestException(
                    correlationId,
                    "BAD_TOKEN",
                    "Token " + token + " is invalid"
                ).WithDetails("token", token);
            }

            var id = tokens[0];
            var filePath = _storage.MakeFileName(id);
            var stream = new FileStream(filePath, FileMode.Open);

            token = await UploadPartAsync(correlationId, token, stream);

            _storage.DeleteChunks(correlationId, id);

            return token;
        }

        public async Task<string> WriteBlobChunkAsync(string correlationId, string token, byte[] buffer)
        {
            CheckOpened(correlationId);

            var tokens = (token ?? "").Split(';');

            if (tokens.Length == 0)
            {
                throw new BadRequestException(
                    correlationId,
                    "BAD_TOKEN",
                    "Token " + token + " is invalid"
                ).WithDetails("token", token);
            }

            var id = tokens[0];

            var size = _storage.AppendChunk(correlationId, id, buffer);

            if (size >= _minChunkSize)
                return await UploadAndDeleteChunksAsync(correlationId, token);

            return token;
        }

        public async Task<BlobInfoV1> EndBlobWriteAsync(string correlationId, string token, byte[] buffer)
        {
            CheckOpened(correlationId);

            var tokens = (token ?? "").Split(';');

            if (tokens.Length < 2)
            {
                throw new BadRequestException(
                    correlationId,
                    "BAD_TOKEN",
                    "Token " + token + " is invalid"
                ).WithDetails("token", token);
            }

            var id = tokens[0];

            // Check if temp file exist
            var size = _storage.GetChunksSize(correlationId, id);

            // Upload temp file or chunks directly
            if (size > 0)
            {
                // If some chunks already stored in temp file - append then upload the entire file
                if (buffer != null)
                    _storage.AppendChunk(correlationId, id, buffer);
                token = await UploadAndDeleteChunksAsync(correlationId, token);
            }
            else
            {
                if (buffer != null)
                {
                    var stream = new MemoryStream(buffer);
                    // If it's the first chunk then upload it without writing to temp file
                    token = await UploadPartAsync(correlationId, token, stream);
                }
            }

            // Complete upload
            tokens = (token ?? "").Split(';');

            var parts = new List<PartETag>();
            for (var index = 2; index < tokens.Length; index++)
            {
                parts.Add(new PartETag
                {
                    ETag = tokens[index],
                    PartNumber = index - 1
                });
            }

            await _client.CompleteMultipartUploadAsync(new CompleteMultipartUploadRequest
            {
                BucketName = _bucket,
                Key = id,
                UploadId = tokens[1],
                PartETags = parts
            });

            return await GetBlobByIdAsync(correlationId, id);
        }

        public async Task AbortBlobWriteAsync(string correlationId, string token)
        {
            CheckOpened(correlationId);

            var tokens = (token ?? "").Split(';');

            if (tokens.Length < 2)
            {
                throw new BadRequestException(
                    correlationId,
                    "BAD_TOKEN",
                    "Token " + token + " is invalid"
                ).WithDetails("token", token);
            }

            await _client.AbortMultipartUploadAsync(new AbortMultipartUploadRequest
            {
                BucketName = _bucket,
                Key = tokens[0],
                UploadId = tokens[1]
            });
        }

        public async Task<BlobInfoV1> BeginBlobReadAsync(string correlationId, string id)
        {
            var item = await GetBlobByIdAsync(correlationId, id);

            if (item == null)
            {
                throw new NotFoundException(
                    correlationId,
                    "BLOB_NOT_FOUND",
                    "Blob " + id + " was not found"
                ).WithDetails("blob_id", id);
            }

            return item;
        }

        public async Task<byte[]> ReadBlobChunkAsync(string correlationId, string id, long skip, int take)
        {
            var result = await _client.GetObjectAsync(new GetObjectRequest
            {
                BucketName = _bucket,
                Key = id,
                ByteRange = new ByteRange(skip, skip + take - 1)
            });

            var stream = result.ResponseStream;
            var buffer = new byte[take];
            var size = stream.Read(buffer, 0, take);

            if (buffer.Length != size)
                Array.Resize(ref buffer, size);

            return buffer;
        }

        public async Task EndBlobReadAsync(string correlationId, string id)
        {
            await Task.Delay(0);
        }
    }}
