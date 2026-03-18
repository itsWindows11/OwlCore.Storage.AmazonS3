namespace OwlCore.Storage.AmazonS3.Properties;

internal sealed class S3FileCreatedAtProperty(S3File file)
    : SimpleModifiableStorageProperty<DateTime?>(
        id: $"{file.Id}:CreatedAt",
        name: "CreatedAt",
        asyncGetter: file.GetCreatedAtValueAsync,
        asyncSetter: file.SetCreatedAtValueAsync),
    ICreatedAtProperty
{
}
