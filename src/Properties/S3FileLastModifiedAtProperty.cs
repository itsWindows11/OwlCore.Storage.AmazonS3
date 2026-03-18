namespace OwlCore.Storage.AmazonS3.Properties;

internal sealed class S3FileLastModifiedAtProperty(S3File file)
    : SimpleModifiableStorageProperty<DateTime?>(
        id: $"{file.Id}:LastModifiedAt",
        name: "LastModifiedAt",
        asyncGetter: file.GetLastModifiedAtValueAsync,
        asyncSetter: file.SetLastModifiedAtValueAsync),
    ILastModifiedAtProperty
{
}
