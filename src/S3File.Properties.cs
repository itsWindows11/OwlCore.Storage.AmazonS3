namespace OwlCore.Storage.AmazonS3;

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
}
