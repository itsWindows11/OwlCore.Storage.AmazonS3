namespace OwlCore.Storage.AmazonS3;

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
}
