using Amazon.S3.Model;
using System.Globalization;

namespace OwlCore.Storage.AmazonS3;

public partial class S3File
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
