using Amazon.S3;
using Amazon.S3.Model;

namespace OwlCore.Storage.AmazonS3;

using Properties;

public partial class S3File : ICreatedAt, ILastModifiedAt
{
    /// <inheritdoc />
    public ICreatedAtProperty CreatedAt => field ??= new S3FileCreatedAtProperty(this);

    /// <inheritdoc />
    public ILastModifiedAtProperty LastModifiedAt => field ??= new S3FileLastModifiedAtProperty(this);

    internal async Task<DateTime?> GetCreatedAtValueAsync(CancellationToken cancellationToken)
    {
        var metadata = await GetMetadataAsync(cancellationToken);

        if (TryGetCreatedAt(metadata, out var createdAt))
            return createdAt;

        return metadata.LastModified == default ? null : metadata.LastModified;
    }

    internal async Task<DateTime?> GetLastModifiedAtValueAsync(CancellationToken cancellationToken)
    {
        var metadata = await GetMetadataAsync(cancellationToken);
        return metadata.LastModified == default ? null : metadata.LastModified;
    }

    internal async Task SetCreatedAtValueAsync(DateTime? value, CancellationToken cancellationToken)
    {
        try
        {
            var metadata = await GetMetadataAsync(cancellationToken);
            
            var copyRequest = new CopyObjectRequest
            {
                SourceBucket = BucketName,
                SourceKey = Key,
                DestinationBucket = BucketName,
                DestinationKey = Key,
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
