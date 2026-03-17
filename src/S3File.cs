using Amazon.S3.Model;
using OwlCore.Storage.AmazonS3.Streams;

namespace OwlCore.Storage.AmazonS3;

/// <summary>
/// Represents an Amazon S3-backed file implementing OwlCore storage abstractions.
/// </summary>
public partial class S3File : IChildFile
{
    /// <inheritdoc />
    public Task<IFolder?> GetParentAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var lastSlashIndex = Key.LastIndexOf('/');
        if (lastSlashIndex < 0)
            return Task.FromResult<IFolder?>(null);

        var parentPath = Key[..lastSlashIndex];
        if (string.IsNullOrEmpty(parentPath))
            return Task.FromResult<IFolder?>(null);

        return Task.FromResult<IFolder?>(new S3Folder(AmazonS3Client, BucketName, parentPath));
    }

    /// <summary>
    /// Retrieves object metadata for this file from Amazon S3.
    /// </summary>
    /// <param name="cancellationToken">A token that can be used to cancel the operation.</param>
    /// <returns>The object metadata response.</returns>
    public Task<GetObjectMetadataResponse> GetMetadataAsync(CancellationToken cancellationToken = default)
        => AmazonS3Client.GetObjectMetadataAsync(new GetObjectMetadataRequest
        {
            BucketName = BucketName,
            Key = Key
        }, cancellationToken);

    /// <inheritdoc />
    public async Task<Stream> OpenStreamAsync(FileAccess accessMode, CancellationToken cancellationToken = default)
        => await S3ReadWriteStream.CreateAsync(AmazonS3Client, BucketName, Key, accessMode, cancellationToken);
}