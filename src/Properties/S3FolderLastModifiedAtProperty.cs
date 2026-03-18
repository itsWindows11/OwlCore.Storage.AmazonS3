namespace OwlCore.Storage.AmazonS3.ItemProperties;

internal sealed class S3FolderLastModifiedAtProperty(S3Folder folder) : ILastModifiedAtProperty
{
    /// <inheritdoc />
    public string Id => $"{folder.Id}:LastModifiedAt";

    /// <inheritdoc />
    public string Name => "LastModifiedAt";

    /// <inheritdoc />
    public Task<DateTime?> GetValueAsync(CancellationToken cancellationToken = default)
        => folder.GetLastModifiedAtValueAsync(cancellationToken);
}
