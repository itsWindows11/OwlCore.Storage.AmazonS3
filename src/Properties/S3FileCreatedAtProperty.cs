namespace OwlCore.Storage.AmazonS3.ItemProperties;

internal sealed class S3FileCreatedAtProperty(S3File file) : ICreatedAtProperty
{
    /// <inheritdoc />
    public string Id => $"{file.Id}:CreatedAt";

    /// <inheritdoc />
    public string Name => "CreatedAt";

    /// <inheritdoc />
    public async Task<DateTime?> GetValueAsync(CancellationToken cancellationToken = default)
        => await file.GetCreatedAtValueAsync(cancellationToken);
}
