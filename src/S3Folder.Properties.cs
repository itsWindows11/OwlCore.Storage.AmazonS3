using Amazon.S3;
using Amazon.S3.Model;

namespace OwlCore.Storage.AmazonS3;

using Properties;

public partial class S3Folder : ICreatedAt, ILastModifiedAt
{
    /// <inheritdoc />
    public ICreatedAtProperty CreatedAt => field ??= new S3FolderCreatedAtProperty(this);

    /// <inheritdoc />
    public ILastModifiedAtProperty LastModifiedAt => field ??= new S3FolderLastModifiedAtProperty(this);

    internal async Task<DateTime?> GetCreatedAtValueAsync(CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(Prefix))
            return null;

        var markerMetadata = await TryGetObjectMetadataAsync(Prefix, cancellationToken);
        if (markerMetadata is null)
            return null;

        if (TryGetCreatedAt(markerMetadata, out var createdAt))
            return createdAt;

        return markerMetadata.LastModified == default ? null : markerMetadata.LastModified;
    }

    internal async Task<DateTime?> GetLastModifiedAtValueAsync(CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(Prefix))
            return null;

        var markerMetadata = await TryGetObjectMetadataAsync(Prefix, cancellationToken);
        if (markerMetadata is null)
            return null;

        return markerMetadata.LastModified == default ? null : markerMetadata.LastModified;
    }

    internal async Task SetCreatedAtValueAsync(DateTime? value, CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(Prefix))
            return;

        var markerKey = Prefix.EndsWith("/", StringComparison.Ordinal) ? Prefix : $"{Prefix}/";
        
        try
        {
            var metadata = await TryGetObjectMetadataAsync(markerKey, cancellationToken);
            if (metadata is null)
                return;

            var copyRequest = new CopyObjectRequest
            {
                SourceBucket = BucketName,
                SourceKey = markerKey,
                DestinationBucket = BucketName,
                DestinationKey = markerKey,
                MetadataDirective = S3MetadataDirective.REPLACE
            };

            if (value.HasValue)
            {
                copyRequest.Metadata["x-amz-meta-created-at"] = value.Value.ToString("O");
            }

            // Copy existing metadata
            if (metadata.Metadata != null)
            {
                foreach (var key in metadata.Metadata.Keys)
                {
                    if (key != "x-amz-meta-created-at")
                    {
                        copyRequest.Metadata[key] = metadata.Metadata[key];
                    }
                }
            }

            await AmazonS3Client.CopyObjectAsync(copyRequest, cancellationToken);
        }
        catch (AmazonS3Exception)
        {
            // Silently fail if metadata update fails
        }
    }

    internal Task SetLastModifiedAtValueAsync(DateTime? value, CancellationToken cancellationToken)
    {
        // LastModifiedAt is managed by S3 and cannot be directly modified
        // This is a no-op intentionally
        return Task.CompletedTask;
    }
}
