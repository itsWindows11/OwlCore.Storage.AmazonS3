using Owner = S3ServerLibrary.S3Objects.Owner;
using S3Object = S3ServerLibrary.S3Object;

namespace OwlCore.Storage.AmazonS3.Tests;

// TODO: Consider replacing Supabase w/ this server after investigating the
// many issues this comes up with.
internal sealed class S3TestServer : IAsyncDisposable
{
    private readonly ConcurrentDictionary<string, ConcurrentDictionary<string, StoredObject>> _buckets = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, MultipartUploadState> _uploads = new(StringComparer.Ordinal);
    private readonly Owner _owner = new("owner", "owner");

    private S3Server? _server;

    public string BucketName { get; private set; } = string.Empty;
    public IAmazonS3 Client { get; private set; } = default!;

    private S3TestServer()
    {
    }

    public static async Task<S3TestServer> CreateAsync()
    {
        Exception? lastException = null;

        for (var attempt = 0; attempt < 5; attempt++)
        {
            var instance = new S3TestServer();
            var port = GetFreeTcpPort();

            var settings = new S3ServerSettings
            {
                EnableSignatures = false,
                Webserver = new WebserverSettings("127.0.0.1", port, false)
            };

            var server = new S3Server(settings);
            instance.ConfigureCallbacks(server);

            try
            {
                server.Start();
                instance._server = server;

                instance.Client = new AmazonS3Client(new BasicAWSCredentials("access", "secret"), new AmazonS3Config
                {
                    ServiceURL = $"http://127.0.0.1:{port}",
                    ForcePathStyle = true,
                    UseHttp = true,
                    AuthenticationRegion = "us-east-1",
                    MaxErrorRetry = 0,
                    Timeout = TimeSpan.FromSeconds(10)
                });

                await instance.ResetBucketAsync();
                return instance;
            }
            catch (Exception ex)
            {
                lastException = ex;

                instance.Client?.Dispose();
                server.Stop();
                server.Dispose();

                await Task.Delay(100);
            }
        }

        throw new InvalidOperationException("Unable to start S3 test server.", lastException);
    }

    public async Task ResetBucketAsync()
    {
        BucketName = $"owlcore-{Guid.NewGuid():N}";

        Exception? bucketError = null;
        for (var bucketAttempt = 0; bucketAttempt < 20; bucketAttempt++)
        {
            try
            {
                await Client.PutBucketAsync(BucketName);
                bucketError = null;
                break;
            }
            catch (Exception ex)
            {
                bucketError = ex;
                await Task.Delay(100);
            }
        }

        if (bucketError is not null)
            throw bucketError;
    }

    private void ConfigureCallbacks(S3Server server)
    {
        server.Service.ListBuckets = _ => Task.FromResult(
            new ListAllMyBucketsResult(_owner, new Buckets([.. _buckets.Keys.Select(x => new Bucket(x, DateTime.UtcNow))])));
        server.Service.ServiceExists = _ => Task.FromResult("us-east-1");

        server.Bucket.Write = ctx =>
        {
            _buckets.TryAdd(ctx.Request.Bucket, new ConcurrentDictionary<string, StoredObject>(StringComparer.Ordinal));
            return Task.CompletedTask;
        };

        server.Bucket.Exists = ctx => Task.FromResult(_buckets.ContainsKey(ctx.Request.Bucket));

        server.Bucket.Delete = ctx =>
        {
            _buckets.TryRemove(ctx.Request.Bucket, out _);
            return Task.CompletedTask;
        };

        server.Bucket.Read = ctx => Task.FromResult(BuildListBucketResult(ctx));

        server.Object.Exists = ctx => Task.FromResult(BuildObjectMetadata(ctx));
        server.Object.Read = ctx => Task.FromResult(BuildReadObject(ctx, ranged: false));
        server.Object.ReadRange = ctx => Task.FromResult(BuildReadObject(ctx, ranged: true));

        server.Object.Write = async ctx =>
        {
            var bucket = GetExistingBucket(ctx.Request.Bucket);

            if (ctx.Request.HeaderExists("x-amz-copy-source"))
            {
                var (sourceBucket, sourceKey) = ParseCopySource(ctx.Request.RetrieveHeaderValue("x-amz-copy-source"));
                var sourceData = GetExistingBucket(sourceBucket);

                if (!sourceData.TryGetValue(sourceKey, out var source))
                    throw new S3Exception(new Error(ErrorCode.NoSuchKey));

                bucket[ctx.Request.Key] = source with
                {
                    LastModified = DateTime.UtcNow,
                    ETag = CreateETag(source.Data)
                };

                return;
            }

            var bytes = await ReadAllBytesAsync(ctx.Request.Data);
            bucket[ctx.Request.Key] = new StoredObject(
                bytes,
                ctx.Request.ContentType ?? "application/octet-stream",
                DateTime.UtcNow,
                CreateETag(bytes));
        };

        server.Object.Delete = ctx =>
        {
            if (_buckets.TryGetValue(ctx.Request.Bucket, out var bucket))
                bucket.TryRemove(ctx.Request.Key, out _);

            return Task.CompletedTask;
        };

        server.Object.DeleteMultiple = (ctx, request) =>
        {
            if (_buckets.TryGetValue(ctx.Request.Bucket, out var bucket) && request.Objects is not null)
            {
                foreach (var item in request.Objects)
                    bucket.TryRemove(item.Key, out _);
            }

            return Task.FromResult(new DeleteResult([], []));
        };

        server.Object.CreateMultipartUpload = ctx =>
        {
            _ = GetExistingBucket(ctx.Request.Bucket);

            var uploadId = Guid.NewGuid().ToString("N");
            _uploads[uploadId] = new MultipartUploadState(ctx.Request.Bucket, ctx.Request.Key);
            return Task.FromResult(new InitiateMultipartUploadResult(ctx.Request.Bucket, ctx.Request.Key, uploadId));
        };

        server.Object.UploadPart = async ctx =>
        {
            if (_uploads.TryGetValue(ctx.Request.UploadId, out var upload))
            {
                var bytes = await ReadAllBytesAsync(ctx.Request.Data);
                upload.Parts[ctx.Request.PartNumber] = bytes;
                ctx.Response.Headers["ETag"] = CreateETag(bytes);
            }
        };

        server.Object.ReadParts = ctx =>
        {
            var result = new ListPartsResult
            {
                Bucket = ctx.Request.Bucket,
                Key = ctx.Request.Key,
                UploadId = ctx.Request.UploadId,
                IsTruncated = false,
                MaxParts = 10_000,
                PartNumberMarker = 0,
                NextPartNumberMarker = 0,
                Owner = _owner,
                Initiator = _owner,
                StorageClass = StorageClassEnum.STANDARD
            };

            if (_uploads.TryGetValue(ctx.Request.UploadId, out var upload))
            {
                result.Parts = [.. upload.Parts
                    .OrderBy(x => x.Key)
                    .Select(x => new Part
                    {
                        PartNumber = x.Key,
                        Size = x.Value.Length,
                        LastModified = DateTime.UtcNow,
                        ETag = CreateETag(x.Value)
                    })];
            }

            return Task.FromResult(result);
        };

        server.Object.CompleteMultipartUpload = (ctx, request) =>
        {
            if (_uploads.TryRemove(ctx.Request.UploadId, out var upload))
            {
                var order = request.Parts?.Select(x => x.PartNumber).ToList() ?? [.. upload.Parts.Keys.OrderBy(x => x)];
                var payload = order
                    .Where(upload.Parts.ContainsKey)
                    .SelectMany(part => upload.Parts[part])
                    .ToArray();

                var bucket = GetExistingBucket(upload.Bucket);
                bucket[upload.Key] = new StoredObject(payload, "application/octet-stream", DateTime.UtcNow, CreateETag(payload));

                return Task.FromResult(new CompleteMultipartUploadResult
                {
                    Bucket = upload.Bucket,
                    Key = upload.Key,
                    ETag = CreateETag(payload),
                    Location = $"/{upload.Bucket}/{upload.Key}"
                });
            }

            return Task.FromResult(new CompleteMultipartUploadResult());
        };

        server.Object.AbortMultipartUpload = ctx =>
        {
            _uploads.TryRemove(ctx.Request.UploadId, out _);
            return Task.CompletedTask;
        };
    }

    public ValueTask DisposeAsync()
    {
        Client?.Dispose();

        _server?.Stop();
        _server?.Dispose();

        return ValueTask.CompletedTask;
    }

    private ConcurrentDictionary<string, StoredObject> GetExistingBucket(string bucketName)
    {
        if (_buckets.TryGetValue(bucketName, out var bucket))
            return bucket;

        throw new S3Exception(new Error(ErrorCode.NoSuchBucket));
    }

    private ListBucketResult BuildListBucketResult(S3Context ctx)
    {
        var bucket = GetExistingBucket(ctx.Request.Bucket);
        var prefix = ctx.Request.Prefix ?? string.Empty;
        var delimiter = ctx.Request.Delimiter;
        var maxKeys = ctx.Request.MaxKeys <= 0 ? 1000 : ctx.Request.MaxKeys;
        var cursor = string.IsNullOrEmpty(ctx.Request.ContinuationToken) ? ctx.Request.Marker : ctx.Request.ContinuationToken;

        var matching = bucket.Keys
            .Where(k => k.StartsWith(prefix, StringComparison.Ordinal))
            .OrderBy(k => k)
            .ToList();

        if (!string.IsNullOrEmpty(cursor))
            matching = [.. matching.Where(k => string.CompareOrdinal(k, cursor) > 0)];

        var contents = new List<ObjectMetadata>();
        var commonPrefixes = new HashSet<string>(StringComparer.Ordinal);

        if (string.IsNullOrEmpty(delimiter))
        {
            var page = matching.Take(maxKeys).ToList();

            foreach (var key in page)
            {
                var value = bucket[key];
                contents.Add(new ObjectMetadata(key, value.LastModified, value.ETag, value.Data.Length, _owner, StorageClassEnum.STANDARD)
                {
                    ContentType = value.ContentType
                });
            }

            var isTruncated = matching.Count > page.Count;
            var nextMarker = isTruncated && page.Count > 0 ? page[^1] : null;

            return new ListBucketResult(
                ctx.Request.Bucket,
                contents,
                contents.Count,
                maxKeys,
                prefix,
                cursor,
                delimiter,
                isTruncated,
                nextMarker,
                new CommonPrefixes([]),
                "us-east-1");
        }

        foreach (var key in matching)
        {
            var suffix = key[prefix.Length..];
            var delimiterIndex = suffix.IndexOf(delimiter, StringComparison.Ordinal);
            if (delimiterIndex >= 0)
            {
                commonPrefixes.Add(prefix + suffix[..(delimiterIndex + 1)]);
                continue;
            }

            var value = bucket[key];
            contents.Add(new ObjectMetadata(key, value.LastModified, value.ETag, value.Data.Length, _owner, StorageClassEnum.STANDARD)
            {
                ContentType = value.ContentType
            });
        }

        var commonPrefixList = commonPrefixes.OrderBy(x => x).ToList();

        return new ListBucketResult(
            ctx.Request.Bucket,
            contents,
            contents.Count + commonPrefixList.Count,
            maxKeys,
            prefix,
            cursor,
            delimiter,
            false,
            null,
            new CommonPrefixes(commonPrefixList),
            "us-east-1");
    }

    private ObjectMetadata BuildObjectMetadata(S3Context ctx)
    {
        var bucket = GetExistingBucket(ctx.Request.Bucket);
        if (!bucket.TryGetValue(ctx.Request.Key, out var value))
            throw new S3Exception(new Error(ErrorCode.NoSuchKey));

        return new ObjectMetadata(ctx.Request.Key, value.LastModified, value.ETag, value.Data.Length, _owner, StorageClassEnum.STANDARD)
        {
            ContentType = value.ContentType
        };
    }

    private S3Object BuildReadObject(S3Context ctx, bool ranged)
    {
        var bucket = GetExistingBucket(ctx.Request.Bucket);
        if (!bucket.TryGetValue(ctx.Request.Key, out var value))
            throw new S3Exception(new Error(ErrorCode.NoSuchKey));

        var data = value.Data;

        if (ranged && ctx.Request.RangeStart.HasValue)
        {
            var start = (int)Math.Clamp(ctx.Request.RangeStart.Value, 0, Math.Max(0, data.Length - 1));
            var end = ctx.Request.RangeEnd.HasValue
                ? (int)Math.Clamp(ctx.Request.RangeEnd.Value, start, Math.Max(start, data.Length - 1))
                : data.Length - 1;

            data = [.. data.Skip(start).Take((end - start) + 1)];
        }

        return new S3Object(
            ctx.Request.Key,
            "1",
            true,
            value.LastModified,
            value.ETag,
            data.Length,
            _owner,
            data,
            value.ContentType,
            StorageClassEnum.STANDARD);
    }

    private static (string Bucket, string Key) ParseCopySource(string value)
    {
        var copySource = Uri.UnescapeDataString(value).Trim('/');
        var slash = copySource.IndexOf('/');

        if (slash <= 0 || slash == copySource.Length - 1)
            throw new S3Exception(new Error(ErrorCode.InvalidArgument));

        return (copySource[..slash], copySource[(slash + 1)..]);
    }

    private static async Task<byte[]> ReadAllBytesAsync(Stream stream)
    {
        if (stream is MemoryStream ms)
            return ms.ToArray();

        using var memory = new MemoryStream();
        await stream.CopyToAsync(memory);
        return memory.ToArray();
    }

    private static string CreateETag(byte[] data)
    {
        var hash = MD5.HashData(data);
        return $"\"{Convert.ToHexString(hash).ToLowerInvariant()}\"";
    }

    private static int GetFreeTcpPort()
    {
        var listener = new global::System.Net.Sockets.TcpListener(global::System.Net.IPAddress.Loopback, 0);
        listener.Start();
        var port = ((global::System.Net.IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }

    private readonly record struct StoredObject(byte[] Data, string ContentType, DateTime LastModified, string ETag);

    private sealed class MultipartUploadState(string bucket, string key)
    {
        public string Bucket { get; } = bucket;
        public string Key { get; } = key;
        public ConcurrentDictionary<int, byte[]> Parts { get; } = new();
    }
}
