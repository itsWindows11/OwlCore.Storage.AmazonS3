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
        : this(amazonS3, bucketName, BuildObjectKey(path, key), nameOverride: null, assumeNormalized: true)
    {
    }

    internal S3File(IAmazonS3 amazonS3, string bucketName, string objectKey, string? nameOverride, bool assumeNormalized)
    {
        if (amazonS3 is null)
            throw new ArgumentNullException(nameof(amazonS3));

        if (string.IsNullOrWhiteSpace(bucketName))
            throw new ArgumentException("Value cannot be null or whitespace.", nameof(bucketName));

        var normalizedObjectKey = assumeNormalized
            ? (objectKey ?? string.Empty)
            : (objectKey ?? string.Empty).Replace('\\', '/').Trim('/');

        if (string.IsNullOrWhiteSpace(normalizedObjectKey))
            throw new ArgumentException("Value cannot be null or whitespace.", nameof(objectKey));

        AmazonS3Client = amazonS3;
        BucketName = bucketName;
        Key = normalizedObjectKey;
        Id = normalizedObjectKey;

        if (nameOverride is { Length: > 0 } resolvedName)
        {
            Name = resolvedName;
        }
        else
        {
            var lastSlashIndex = normalizedObjectKey.LastIndexOf('/');
            Name = lastSlashIndex >= 0 ? normalizedObjectKey[(lastSlashIndex + 1)..] : normalizedObjectKey;
        }
    }

    private static string BuildObjectKey(string path, string key)
    {
        if (string.IsNullOrWhiteSpace(key))
            throw new ArgumentException("Value cannot be null or whitespace.", nameof(key));

        var normalizedPath = (path ?? string.Empty).Replace('\\', '/').Trim('/');
        var normalizedKey = key.Replace('\\', '/').Trim('/');

        return string.IsNullOrEmpty(normalizedPath)
            ? normalizedKey
            : $"{normalizedPath}/{normalizedKey}";
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
