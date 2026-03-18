using Amazon.S3;
using Amazon.S3.Model;

namespace OwlCore.Storage.AmazonS3;

using Properties;

public partial class S3Folder : ICreatedAt, ILastModifiedAt
{
    /// <inheritdoc />
    public ICreatedAtProperty CreatedAt => field ??= new S3FolderCreatedAtProperty(this);

    /// <inheritdoc />
    public ILastModifiedAtProperty LastModifiedAt => field ??= new S3FolderLastModifiedAtProperty(this);
}
