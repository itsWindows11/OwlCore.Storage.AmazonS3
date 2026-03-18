namespace OwlCore.Storage.AmazonS3.Properties;

internal sealed class S3FolderCreatedAtProperty(S3Folder folder)
    : SimpleModifiableStorageProperty<DateTime?>(
        id: $"{folder.Id}:CreatedAt",
        name: "CreatedAt",
        asyncGetter: folder.GetCreatedAtValueAsync,
        asyncSetter: folder.SetCreatedAtValueAsync),
    ICreatedAtProperty
{
}
