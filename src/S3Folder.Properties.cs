namespace OwlCore.Storage.AmazonS3;

public partial class S3Folder : ICreatedAt, ILastModifiedAt
{
    /// <inheritdoc />
    public ICreatedAtProperty CreatedAt => field ??= new S3FolderDateTimeProperty(this, isCreatedAt: true);

    /// <inheritdoc />
    public ILastModifiedAtProperty LastModifiedAt => field ??= new S3FolderDateTimeProperty(this, isCreatedAt: false);

    private async Task<DateTime?> GetCreatedAtValueAsync(CancellationToken cancellationToken)
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

    private async Task<DateTime?> GetLastModifiedAtValueAsync(CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(Prefix))
            return null;

        var markerMetadata = await TryGetObjectMetadataAsync(Prefix, cancellationToken);
        if (markerMetadata is null)
            return null;

        return markerMetadata.LastModified == default ? null : markerMetadata.LastModified;
    }

    private sealed class S3FolderDateTimeProperty(S3Folder folder, bool isCreatedAt) : ICreatedAtProperty, ILastModifiedAtProperty
    {
        /// <inheritdoc />
        public string Id => $"{folder.Id}:{Name}";

        /// <inheritdoc />
        public string Name => isCreatedAt ? "CreatedAt" : "LastModifiedAt";

        /// <inheritdoc />
        public Task<DateTime?> GetValueAsync(CancellationToken cancellationToken = default)
            => isCreatedAt
                ? folder.GetCreatedAtValueAsync(cancellationToken)
                : folder.GetLastModifiedAtValueAsync(cancellationToken);
    }
}
