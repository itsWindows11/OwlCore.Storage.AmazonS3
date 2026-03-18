namespace OwlCore.Storage.AmazonS3.ItemProperties;

internal sealed class S3FileLastModifiedAtProperty(S3File file) : ILastModifiedAtProperty
{
    /// <inheritdoc />
    public string Id => $"{file.Id}:LastModifiedAt";

    /// <inheritdoc />
    public string Name => "LastModifiedAt";

    /// <inheritdoc />
    public async Task<DateTime?> GetValueAsync(CancellationToken cancellationToken = default)
        => await file.GetLastModifiedAtValueAsync(cancellationToken);
}
