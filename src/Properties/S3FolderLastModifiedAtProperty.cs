namespace OwlCore.Storage.AmazonS3.Properties;

internal sealed class S3FolderLastModifiedAtProperty(S3Folder folder)
    : SimpleModifiableStorageProperty<DateTime?>(
        id: $"{folder.Id}:LastModifiedAt",
        name: "LastModifiedAt",
        asyncGetter: folder.GetLastModifiedAtValueAsync,
        asyncSetter: folder.SetLastModifiedAtValueAsync),
    ILastModifiedAtProperty
{
}
