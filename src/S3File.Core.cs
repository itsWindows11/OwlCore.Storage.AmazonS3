using Amazon.S3;

namespace OwlCore.Storage.AmazonS3;

public partial class S3File
{
    /// <summary>
    /// Initializes a new instance of <see cref="S3File"/>.
    /// </summary>
    /// <param name="amazonS3">The S3 client used for file operations.</param>
    /// <param name="bucketName">The bucket that contains this file.</param>
    /// <param name="path">The parent folder path inside <paramref name="bucketName"/>.</param>
    /// <param name="key">The file key or file name.</param>
    public S3File(IAmazonS3 amazonS3, string bucketName, string path, string key)
    {
        if (amazonS3 is null)
            throw new ArgumentNullException(nameof(amazonS3));

        if (string.IsNullOrWhiteSpace(bucketName))
            throw new ArgumentException("Value cannot be null or whitespace.", nameof(bucketName));

        if (string.IsNullOrWhiteSpace(key))
            throw new ArgumentException("Value cannot be null or whitespace.", nameof(key));

        var normalizedPath = (path ?? string.Empty).Replace('\\', '/').Trim('/');
        var normalizedKey = key.Replace('\\', '/').Trim('/');

        AmazonS3Client = amazonS3;
        BucketName = bucketName;

        Key = string.IsNullOrEmpty(normalizedPath)
            ? normalizedKey
            : $"{normalizedPath}/{normalizedKey}";

        Id = Key;

        var lastSlashIndex = Key.LastIndexOf('/');
        Name = lastSlashIndex >= 0 ? Key[(lastSlashIndex + 1)..] : Key;
    }

    /// <summary>
    /// Gets the S3 client used by this file.
    /// </summary>
    public IAmazonS3 AmazonS3Client { get; }

    /// <summary>
    /// Gets the bucket name for this file.
    /// </summary>
    public string BucketName { get; }

    /// <summary>
    /// Gets the normalized object key for this file.
    /// </summary>
    public string Key { get; }

    /// <inheritdoc />
    public string Id { get; }

    /// <inheritdoc />
    public string Name { get; }
}
