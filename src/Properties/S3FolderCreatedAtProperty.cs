namespace OwlCore.Storage.AmazonS3.ItemProperties;

internal sealed class S3FolderCreatedAtProperty(S3Folder folder) : ICreatedAtProperty
{
    /// <inheritdoc />
    public string Id => $"{folder.Id}:CreatedAt";

    /// <inheritdoc />
    public string Name => "CreatedAt";

    /// <inheritdoc />
    public Task<DateTime?> GetValueAsync(CancellationToken cancellationToken = default)
        => folder.GetCreatedAtValueAsync(cancellationToken);
}
