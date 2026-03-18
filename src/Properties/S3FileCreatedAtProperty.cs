using Amazon.S3;
using Amazon.S3.Model;
using System.Globalization;

namespace OwlCore.Storage.AmazonS3.Properties;

internal sealed class S3FileCreatedAtProperty(S3File file)
    : SimpleModifiableStorageProperty<DateTime?>(
        id: $"{file.Id}:CreatedAt",
        name: "CreatedAt",
        asyncGetter: cancellationToken => GetValueAsync(file, cancellationToken),
        asyncSetter: (value, cancellationToken) => SetValueAsync(file, value, cancellationToken)),
    ICreatedAtProperty
{
    private static readonly DateTimeStyles CreatedAtDateParseStyles =
        DateTimeStyles.RoundtripKind | DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal;

    private static readonly string[] CreatedAtMetadataKeys =
    [
        "x-amz-meta-createdat",
        "x-amz-meta-created-at",
        "createdat",
        "created-at"
    ];

    private static async Task<DateTime?> GetValueAsync(S3File file, CancellationToken cancellationToken)
    {
        var metadata = await file.GetMetadataAsync(cancellationToken);

        if (TryGetCreatedAt(metadata, out var createdAt))
            return createdAt;

        return metadata.LastModified == default ? null : metadata.LastModified;
    }

    private static async Task SetValueAsync(S3File file, DateTime? value, CancellationToken cancellationToken)
    {
        try
        {
            var metadata = await file.GetMetadataAsync(cancellationToken);

            var copyRequest = new CopyObjectRequest
            {
                SourceBucket = file.BucketName,
                SourceKey = file.Key,
                DestinationBucket = file.BucketName,
                DestinationKey = file.Key,
                MetadataDirective = S3MetadataDirective.REPLACE
            };

            if (value.HasValue)
                copyRequest.Metadata["x-amz-meta-created-at"] = value.Value.ToString("O");

            if (metadata.Metadata is not null)
            {
                foreach (var key in metadata.Metadata.Keys)
                {
                    if (!string.Equals(key, "x-amz-meta-created-at", StringComparison.OrdinalIgnoreCase))
                        copyRequest.Metadata[key] = metadata.Metadata[key];
                }
            }

            await file.AmazonS3Client.CopyObjectAsync(copyRequest, cancellationToken);
        }
        catch (AmazonS3Exception)
        {
        }
    }

    private static bool TryGetCreatedAt(GetObjectMetadataResponse metadata, out DateTime createdAt)
    {
        foreach (var key in CreatedAtMetadataKeys)
        {
            var value = metadata.Metadata[key];
            if (!string.IsNullOrWhiteSpace(value) && DateTime.TryParse(value, CultureInfo.InvariantCulture, CreatedAtDateParseStyles, out createdAt))
                return true;
        }

        createdAt = default;
        return false;
    }
}
