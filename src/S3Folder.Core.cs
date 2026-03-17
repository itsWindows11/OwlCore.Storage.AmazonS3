using Amazon.S3;

namespace OwlCore.Storage.AmazonS3;

public partial class S3Folder
{
    /// <summary>
    /// Initializes a new instance of <see cref="S3Folder"/>.
    /// </summary>
    /// <param name="amazonS3">The S3 client used for all folder operations.</param>
    /// <param name="bucketName">The bucket that contains this folder.</param>
    /// <param name="path">The folder path inside <paramref name="bucketName"/>.</param>
    public S3Folder(IAmazonS3 amazonS3, string bucketName, string path)
    {
        if (amazonS3 is null)
            throw new ArgumentNullException(nameof(amazonS3));

        if (string.IsNullOrWhiteSpace(bucketName))
            throw new ArgumentException("Value cannot be null or whitespace.", nameof(bucketName));

        AmazonS3Client = amazonS3;
        BucketName = bucketName;
        Path = (path ?? string.Empty).Replace('\\', '/').Trim('/');

        Id = string.IsNullOrEmpty(Path) ? "/" : $"/{Path}";
        if (string.IsNullOrEmpty(Path))
            Name = bucketName;
        else
        {
            var lastSlashIndex = Path.LastIndexOf('/');
            Name = lastSlashIndex >= 0 ? Path[(lastSlashIndex + 1)..] : Path;
        }
    }

    /// <summary>
    /// Gets the S3 client used by this folder.
    /// </summary>
    public IAmazonS3 AmazonS3Client { get; }

    /// <summary>
    /// Gets the bucket name for this folder.
    /// </summary>
    public string BucketName { get; }

    /// <summary>
    /// Gets the normalized path for this folder inside the bucket.
    /// </summary>
    public string Path { get; }

    /// <inheritdoc />
    public string Id { get; }

    /// <inheritdoc />
    public string Name { get; }

    private string Prefix => string.IsNullOrEmpty(Path) ? string.Empty : $"{Path}/";
}
