using Amazon.S3;
using System.Net;
using System.Runtime.CompilerServices;

namespace OwlCore.Storage.AmazonS3;

/// <summary>
/// Tracks detected capabilities for an <see cref="IAmazonS3"/> client instance.
/// Capabilities are discovered at runtime and cached per client instance.
/// </summary>
internal sealed class S3ClientCapabilities
{
    private static readonly ConditionalWeakTable<IAmazonS3, S3ClientCapabilities> Cache = new();

    /// <summary>
    /// Gets or sets whether the client supports server-side object copying.
    /// <see langword="null"/> means not yet determined.
    /// </summary>
    public bool? SupportsServerSideCopy { get; set; }

    /// <summary>
    /// Gets or creates the capabilities entry for the given S3 client.
    /// </summary>
    public static S3ClientCapabilities GetOrCreate(IAmazonS3 client)
        => Cache.GetValue(client, static _ => new S3ClientCapabilities());

    /// <summary>
    /// Determines whether an <see cref="AmazonS3Exception"/> indicates that server-side
    /// copying is not supported by the client, and a stream-based fallback should be used.
    /// </summary>
    public static bool ShouldFallbackToStreamCopy(AmazonS3Exception ex)
    {
        if (ex.StatusCode == HttpStatusCode.BadRequest)
            return true;

        if (string.Equals(ex.ErrorCode, "InvalidRequest", StringComparison.Ordinal)
            || string.Equals(ex.ErrorCode, "InvalidArgument", StringComparison.Ordinal))
            return true;

        return !string.IsNullOrWhiteSpace(ex.Message)
            && ex.Message.Contains("copysource", StringComparison.OrdinalIgnoreCase);
    }
}
