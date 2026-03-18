namespace OwlCore.Storage.AmazonS3.Properties;

internal sealed class S3FileLastModifiedAtProperty(S3File file)
    : SimpleStorageProperty<DateTime?>(
        id: $"{file.Id}:LastModifiedAt",
        name: "LastModifiedAt",
        asyncGetter: cancellationToken => GetValueAsync(file, cancellationToken)),
    ILastModifiedAtProperty
{
    private static async Task<DateTime?> GetValueAsync(S3File file, CancellationToken cancellationToken)
    {
        var metadata = await file.GetMetadataAsync(cancellationToken);
        return metadata.LastModified == default ? null : metadata.LastModified;
    }
}
