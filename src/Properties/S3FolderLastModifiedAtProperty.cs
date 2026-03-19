using Amazon.S3;
using Amazon.S3.Model;
using System.Net;

namespace OwlCore.Storage.AmazonS3.Properties;

internal sealed class S3FolderLastModifiedAtProperty(S3Folder folder)
    : SimpleStorageProperty<DateTime?>(
        id: $"{folder.Id}:LastModifiedAt",
        name: "LastModifiedAt",
        asyncGetter: cancellationToken => GetValueAsync(folder, cancellationToken)),
    ILastModifiedAtProperty
{
    private static async Task<DateTime?> GetValueAsync(S3Folder folder, CancellationToken cancellationToken)
    {
        var markerKey = GetMarkerKey(folder);
        if (string.IsNullOrEmpty(markerKey))
            return null;

        try
        {
            var metadata = await folder.AmazonS3Client.GetObjectMetadataAsync(new GetObjectMetadataRequest
            {
                BucketName = folder.BucketName,
                Key = markerKey
            }, cancellationToken);

            return metadata.LastModified == default ? null : metadata.LastModified;
        }
        catch (AmazonS3Exception ex) when (ex.StatusCode == HttpStatusCode.NotFound)
        {
            return null;
        }
    }

    private static string? GetMarkerKey(S3Folder folder)
    {
        if (string.IsNullOrEmpty(folder.Path))
            return null;

        return folder.Path.EndsWith("/", StringComparison.Ordinal) ? folder.Path : $"{folder.Path}/";
    }
}
