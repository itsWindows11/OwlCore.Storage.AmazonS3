using Amazon.S3;
using Amazon.S3.Model;
using System.Globalization;
using System.Net;

namespace OwlCore.Storage.AmazonS3;

public partial class S3Folder
{
    private static readonly DateTimeStyles CreatedAtDateParseStyles =
        DateTimeStyles.RoundtripKind | DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal;

    private static readonly string[] CreatedAtMetadataKeys =
    [
        "x-amz-meta-createdat",
        "x-amz-meta-created-at",
        "createdat",
        "created-at"
    ];

    private S3ClientCapabilities GetClientCapabilities()
        => S3ClientCapabilities.GetOrCreate(AmazonS3Client);

    private async Task<GetObjectMetadataResponse?> TryGetObjectMetadataAsync(string key, CancellationToken cancellationToken)
    {
        try
        {
            return await AmazonS3Client.GetObjectMetadataAsync(new GetObjectMetadataRequest
            {
                BucketName = BucketName,
                Key = key
            }, cancellationToken).ConfigureAwait(false);
        }
        catch (AmazonS3Exception ex) when (ex.StatusCode == HttpStatusCode.NotFound)
        {
            return null;
        }
    }

    private async Task TryDeleteObjectIfExistsAsync(string key, CancellationToken cancellationToken)
    {
        try
        {
            await AmazonS3Client.DeleteObjectAsync(new DeleteObjectRequest
            {
                BucketName = BucketName,
                Key = key
            }, cancellationToken).ConfigureAwait(false);
        }
        catch (AmazonS3Exception ex) when (ex.StatusCode == HttpStatusCode.NotFound)
        {
        }
    }

    private async Task DeleteObjectsByPrefixAsync(string prefix, CancellationToken cancellationToken)
    {
        string? continuationToken = null;

        do
        {
            var listResponse = await AmazonS3Client.ListObjectsV2Async(new ListObjectsV2Request
            {
                BucketName = BucketName,
                Prefix = prefix,
                ContinuationToken = continuationToken,
                MaxKeys = 1000
            }, cancellationToken).ConfigureAwait(false);

            if (listResponse.S3Objects?.Count > 0)
            {
                await AmazonS3Client.DeleteObjectsAsync(new DeleteObjectsRequest
                {
                    BucketName = BucketName,
                    Objects = [.. listResponse.S3Objects.Select(x => new KeyVersion { Key = x.Key })]
                }, cancellationToken).ConfigureAwait(false);
            }

            continuationToken = listResponse.NextContinuationToken;
        }
        while (!string.IsNullOrEmpty(continuationToken));
    }

    private static bool ShouldFallbackToStreamCopy(AmazonS3Exception ex)
        => S3ClientCapabilities.ShouldFallbackToStreamCopy(ex);

    private string ResolvePath(string value)
    {
        var normalized = Normalize(value);
        if (string.IsNullOrEmpty(Prefix))
            return normalized;

        if (normalized == Path || normalized.StartsWith(Prefix, StringComparison.Ordinal))
            return normalized;

        return CombinePath(Path, normalized);
    }

    private static string Normalize(string value)
        => (value ?? string.Empty).Replace('\\', '/').Trim('/');

    private static string CombinePath(string parent, string child)
    {
        var normalizedParent = Normalize(parent);
        var normalizedChild = Normalize(child);

        if (string.IsNullOrEmpty(normalizedParent))
            return normalizedChild;

        if (string.IsNullOrEmpty(normalizedChild))
            return normalizedParent;

        return $"{normalizedParent}/{normalizedChild}";
    }

    private async Task<bool> FolderExistsAsync(string folderPath, CancellationToken cancellationToken)
    {
        var normalizedPath = Normalize(folderPath);
        if (string.IsNullOrEmpty(normalizedPath))
            return true;

        var folderPrefix = normalizedPath.EndsWith("/", StringComparison.Ordinal) ? normalizedPath : $"{normalizedPath}/";

        var listResponse = await AmazonS3Client.ListObjectsV2Async(new ListObjectsV2Request
        {
            BucketName = BucketName,
            Prefix = folderPrefix,
            MaxKeys = 1
        }, cancellationToken).ConfigureAwait(false);

        return listResponse.S3Objects?.Count > 0;
    }

    private static bool TryGetCreatedAt(GetObjectMetadataResponse metadata, out DateTime createdAt)
    {
        foreach (var key in CreatedAtMetadataKeys)
        {
            var value = metadata.Metadata[key];
            if (!string.IsNullOrWhiteSpace(value) && DateTime.TryParse(value, CultureInfo.InvariantCulture, CreatedAtDateParseStyles, out createdAt))
                return true;
        }

        createdAt = default;
        return false;
    }

    private async Task<IChildFile?> TryCreateS3CopyAsync(S3File s3Source, string destinationName, bool overwrite, CancellationToken cancellationToken)
    {
        var normalizedName = Normalize(destinationName);
        var destinationKey = CombinePath(Path, normalizedName);

        if (!overwrite)
        {
            if (await TryGetObjectMetadataAsync(destinationKey, cancellationToken).ConfigureAwait(false) is not null)
                throw new FileAlreadyExistsException(normalizedName);

            if (await FolderExistsAsync(destinationKey, cancellationToken).ConfigureAwait(false))
                throw new FileAlreadyExistsException($"{destinationKey}/ (folder)");
        }

        var capabilities = GetClientCapabilities();
        if (capabilities.SupportsServerSideCopy == false)
            return null;

        try
        {
            await AmazonS3Client.CopyObjectAsync(new CopyObjectRequest
            {
                SourceBucket = s3Source.BucketName,
                SourceKey = s3Source.Key,
                DestinationBucket = BucketName,
                DestinationKey = destinationKey
            }, cancellationToken).ConfigureAwait(false);

            capabilities.SupportsServerSideCopy = true;
            return new S3File(AmazonS3Client, BucketName, destinationKey, normalizedName, assumeNormalized: true);
        }
        // Some S3-compatible services (e.g. Supabase) may not support server-side copying.
        // In that case, fall back to a stream copy.
        catch (AmazonS3Exception ex) when (ShouldFallbackToStreamCopy(ex))
        {
            capabilities.SupportsServerSideCopy = false;
            return null;
        }
    }

    private async Task<IChildFile?> TryCreateS3MoveAsync(S3File s3Source, string destinationName, bool overwrite, CancellationToken cancellationToken)
    {
        var normalizedName = Normalize(destinationName);
        var destinationKey = CombinePath(Path, normalizedName);

        if (!overwrite)
        {
            if (await TryGetObjectMetadataAsync(destinationKey, cancellationToken).ConfigureAwait(false) is not null)
                throw new FileAlreadyExistsException(normalizedName);

            if (await FolderExistsAsync(destinationKey, cancellationToken).ConfigureAwait(false))
                throw new FileAlreadyExistsException($"{destinationKey}/ (folder)");
        }

        var capabilities = GetClientCapabilities();
        if (capabilities.SupportsServerSideCopy == false)
            return null;

        try
        {
            await AmazonS3Client.CopyObjectAsync(new CopyObjectRequest
            {
                SourceBucket = s3Source.BucketName,
                SourceKey = s3Source.Key,
                DestinationBucket = BucketName,
                DestinationKey = destinationKey
            }, cancellationToken).ConfigureAwait(false);

            await s3Source.AmazonS3Client.DeleteObjectAsync(new DeleteObjectRequest
            {
                BucketName = s3Source.BucketName,
                Key = s3Source.Key
            }, cancellationToken).ConfigureAwait(false);

            capabilities.SupportsServerSideCopy = true;
            return new S3File(AmazonS3Client, BucketName, destinationKey, normalizedName, assumeNormalized: true);
        }
        catch (AmazonS3Exception ex) when (ShouldFallbackToStreamCopy(ex))
        {
            capabilities.SupportsServerSideCopy = false;
            return null;
        }
    }
}
