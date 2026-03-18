using Amazon.S3;
using Amazon.S3.Model;
using System.Globalization;
using System.Net;

namespace OwlCore.Storage.AmazonS3.Properties;

internal sealed class S3FolderCreatedAtProperty(S3Folder folder)
    : SimpleModifiableStorageProperty<DateTime?>(
        id: $"{folder.Id}:CreatedAt",
        name: "CreatedAt",
        asyncGetter: cancellationToken => GetValueAsync(folder, cancellationToken),
        asyncSetter: (value, cancellationToken) => SetValueAsync(folder, value, cancellationToken)),
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

    private static async Task<DateTime?> GetValueAsync(S3Folder folder, CancellationToken cancellationToken)
    {
        var markerMetadata = await GetMarkerMetadataAsync(folder, cancellationToken);
        if (markerMetadata is null)
            return null;

        if (TryGetCreatedAt(markerMetadata, out var createdAt))
            return createdAt;

        return markerMetadata.LastModified == default ? null : markerMetadata.LastModified;
    }

    private static async Task SetValueAsync(S3Folder folder, DateTime? value, CancellationToken cancellationToken)
    {
        var markerKey = GetMarkerKey(folder);
        if (string.IsNullOrEmpty(markerKey))
            return;

        try
        {
            var metadata = await GetMarkerMetadataAsync(folder, cancellationToken);
            if (metadata is null)
                return;

            var copyRequest = new CopyObjectRequest
            {
                SourceBucket = folder.BucketName,
                SourceKey = markerKey,
                DestinationBucket = folder.BucketName,
                DestinationKey = markerKey,
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

            await folder.AmazonS3Client.CopyObjectAsync(copyRequest, cancellationToken);
        }
        catch (AmazonS3Exception)
        {
        }
    }

    private static string? GetMarkerKey(S3Folder folder)
    {
        if (string.IsNullOrEmpty(folder.Path))
            return null;

        return folder.Path.EndsWith("/", StringComparison.Ordinal) ? folder.Path : $"{folder.Path}/";
    }

    private static async Task<GetObjectMetadataResponse?> GetMarkerMetadataAsync(S3Folder folder, CancellationToken cancellationToken)
    {
        var markerKey = GetMarkerKey(folder);
        if (string.IsNullOrEmpty(markerKey))
            return null;

        try
        {
            return await folder.AmazonS3Client.GetObjectMetadataAsync(new GetObjectMetadataRequest
            {
                BucketName = folder.BucketName,
                Key = markerKey
            }, cancellationToken);
        }
        catch (AmazonS3Exception ex) when (ex.StatusCode == HttpStatusCode.NotFound)
        {
            return null;
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
