namespace OwlCore.Storage.AmazonS3;

public partial class S3File : ICreatedAt, ILastModifiedAt
{
    /// <inheritdoc />
    public ICreatedAtProperty CreatedAt => field ??= new S3FileDateTimeProperty(this, isCreatedAt: true);

    /// <inheritdoc />
    public ILastModifiedAtProperty LastModifiedAt => field ??= new S3FileDateTimeProperty(this, isCreatedAt: false);

    private sealed class S3FileDateTimeProperty(S3File file, bool isCreatedAt) : ICreatedAtProperty, ILastModifiedAtProperty
    {
        /// <inheritdoc />
        public string Id => $"{file.Id}:{Name}";

        /// <inheritdoc />
        public string Name => isCreatedAt ? "CreatedAt" : "LastModifiedAt";

        /// <inheritdoc />
        public async Task<DateTime?> GetValueAsync(CancellationToken cancellationToken = default)
        {
            var metadata = await file.GetMetadataAsync(cancellationToken);

            if (isCreatedAt && TryGetCreatedAt(metadata, out var createdAt))
                return createdAt;

            return metadata.LastModified == default ? null : metadata.LastModified;
        }
    }
}
