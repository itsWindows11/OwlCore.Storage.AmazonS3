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

    private async Task<GetObjectMetadataResponse?> TryGetObjectMetadataAsync(string key, CancellationToken cancellationToken)
    {
        try
        {
            return await AmazonS3Client.GetObjectMetadataAsync(new GetObjectMetadataRequest
            {
                BucketName = BucketName,
                Key = key
            }, cancellationToken);
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
            }, cancellationToken);
        }
        catch (AmazonS3Exception ex) when (ex.StatusCode == HttpStatusCode.NotFound)
        {
        }
    }

    private static bool ShouldFallbackToStreamCopy(AmazonS3Exception ex)
    {
        if (ex.StatusCode == HttpStatusCode.BadRequest)
            return true;

        if (string.Equals(ex.ErrorCode, "InvalidRequest", StringComparison.Ordinal)
            || string.Equals(ex.ErrorCode, "InvalidArgument", StringComparison.Ordinal))
            return true;

        return !string.IsNullOrWhiteSpace(ex.Message)
            && ex.Message.Contains("copysource");
    }

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

        if (await TryGetObjectMetadataAsync(folderPrefix, cancellationToken) is not null)
            return true;

        var listResponse = await AmazonS3Client.ListObjectsV2Async(new ListObjectsV2Request
        {
            BucketName = BucketName,
            Prefix = folderPrefix,
            MaxKeys = 1
        }, cancellationToken);

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
            if (await TryGetObjectMetadataAsync(destinationKey, cancellationToken) is not null)
                throw new FileAlreadyExistsException(normalizedName);

            if (await FolderExistsAsync(destinationKey, cancellationToken))
                throw new FileAlreadyExistsException($"{destinationKey}/ (folder)");
        }

        try
        {
            await AmazonS3Client.CopyObjectAsync(new CopyObjectRequest
            {
                SourceBucket = s3Source.BucketName,
                SourceKey = s3Source.Key,
                DestinationBucket = BucketName,
                DestinationKey = destinationKey
            }, cancellationToken);

            return new S3File(AmazonS3Client, BucketName, Path, normalizedName);
        }
        // Some S3-compatible services (e.g. Supabase) may not support server-side copying.
        // In that case, fall back to a stream copy.
        catch (AmazonS3Exception ex) when (ShouldFallbackToStreamCopy(ex))
        {
            return null;
        }
    }

    private async Task<IChildFile?> TryCreateS3MoveAsync(S3File s3Source, string destinationName, bool overwrite, CancellationToken cancellationToken)
    {
        var normalizedName = Normalize(destinationName);
        var destinationKey = CombinePath(Path, normalizedName);

        if (!overwrite)
        {
            if (await TryGetObjectMetadataAsync(destinationKey, cancellationToken) is not null)
                throw new FileAlreadyExistsException(normalizedName);

            if (await FolderExistsAsync(destinationKey, cancellationToken))
                throw new FileAlreadyExistsException($"{destinationKey}/ (folder)");
        }

        try
        {
            await AmazonS3Client.CopyObjectAsync(new CopyObjectRequest
            {
                SourceBucket = s3Source.BucketName,
                SourceKey = s3Source.Key,
                DestinationBucket = BucketName,
                DestinationKey = destinationKey
            }, cancellationToken);

            await s3Source.AmazonS3Client.DeleteObjectAsync(new DeleteObjectRequest
            {
                BucketName = s3Source.BucketName,
                Key = s3Source.Key
            }, cancellationToken);

            return new S3File(AmazonS3Client, BucketName, Path, normalizedName);
        }
        catch (AmazonS3Exception ex) when (ShouldFallbackToStreamCopy(ex))
        {
            return null;
        }
    }
}
