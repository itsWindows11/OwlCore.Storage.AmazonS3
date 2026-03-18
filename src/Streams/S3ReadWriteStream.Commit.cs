using Amazon.S3;
using Amazon.S3.Model;
using System.Buffers;
using System.Net;

namespace OwlCore.Storage.AmazonS3.Streams;

public sealed partial class S3ReadWriteStream
{
    private async Task CommitAsync(CancellationToken cancellationToken)
    {
        if (_accessMode == FileAccess.Read || _finalized)
            return;

        try
        {
            if (!_hasWrites)
            {
                _finalized = true;
                return;
            }

            await EnsureSourceUnchangedAsync(cancellationToken).ConfigureAwait(false);

            if (_length == 0)
            {
                await _amazonS3Client.PutObjectAsync(new PutObjectRequest
                {
                    BucketName = _bucketName,
                    Key = _key,
                    InputStream = Stream.Null,
                    AutoCloseStream = false
                }, cancellationToken).ConfigureAwait(false);

                _finalized = true;
                return;
            }

            if (_length <= PartSize)
            {
                var buffer = new byte[_length];
                await ReadAtCoreAsync(0, buffer.AsMemory(0, (int)_length), cancellationToken).ConfigureAwait(false);
                using var singlePutStream = new MemoryStream(buffer, writable: false);
                await _amazonS3Client.PutObjectAsync(new PutObjectRequest
                {
                    BucketName = _bucketName,
                    Key = _key,
                    InputStream = singlePutStream,
                    AutoCloseStream = false
                }, cancellationToken).ConfigureAwait(false);

                _finalized = true;
                return;
            }

            var initiate = await _amazonS3Client.InitiateMultipartUploadAsync(new InitiateMultipartUploadRequest
            {
                BucketName = _bucketName,
                Key = _key
            }, cancellationToken).ConfigureAwait(false);

            var uploadId = initiate.UploadId;
            var partEtags = new List<PartETag>();
            var rentedPartBuffer = ArrayPool<byte>.Shared.Rent(PartSize);

            try
            {
                var partNumber = 1;
                for (long start = 0; start < _length; start += PartSize)
                {
                    var partLength = (int)Math.Min(PartSize, _length - start);
                    await ReadAtCoreAsync(start, rentedPartBuffer.AsMemory(0, partLength), cancellationToken).ConfigureAwait(false);

                    using var partStream = new MemoryStream(rentedPartBuffer, 0, partLength, writable: false, publiclyVisible: true);
                    var uploadPart = await _amazonS3Client.UploadPartAsync(new UploadPartRequest
                    {
                        BucketName = _bucketName,
                        Key = _key,
                        UploadId = uploadId,
                        PartNumber = partNumber,
                        PartSize = partLength,
                        InputStream = partStream
                    }, cancellationToken).ConfigureAwait(false);

                    partEtags.Add(new PartETag(partNumber, uploadPart.ETag));
                    partNumber++;
                }

                await EnsureSourceUnchangedAsync(cancellationToken).ConfigureAwait(false);

                await _amazonS3Client.CompleteMultipartUploadAsync(new CompleteMultipartUploadRequest
                {
                    BucketName = _bucketName,
                    Key = _key,
                    UploadId = uploadId,
                    PartETags = partEtags
                }, cancellationToken).ConfigureAwait(false);
            }
            catch
            {
                await _amazonS3Client.AbortMultipartUploadAsync(new AbortMultipartUploadRequest
                {
                    BucketName = _bucketName,
                    Key = _key,
                    UploadId = uploadId
                }, cancellationToken).ConfigureAwait(false);

                throw;
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(rentedPartBuffer, clearArray: true);
            }

            _finalized = true;
        }
        finally
        {
            _finalized = true;
        }
    }

    private async Task EnsureSourceUnchangedAsync(CancellationToken cancellationToken)
    {
        if (_sourceExists)
        {
            try
            {
                var metadata = await _amazonS3Client.GetObjectMetadataAsync(new GetObjectMetadataRequest
                {
                    BucketName = _bucketName,
                    Key = _key
                }, cancellationToken).ConfigureAwait(false);

                if (!string.Equals(metadata.ETag, _sourceETag, StringComparison.Ordinal))
                    throw new IOException("Source object changed while stream was open.");

                return;
            }
            catch (AmazonS3Exception ex) when (ex.StatusCode == HttpStatusCode.NotFound)
            {
                throw new IOException("Source object was deleted while stream was open.", ex);
            }
        }

        try
        {
            await _amazonS3Client.GetObjectMetadataAsync(new GetObjectMetadataRequest
            {
                BucketName = _bucketName,
                Key = _key
            }, cancellationToken).ConfigureAwait(false);

            throw new IOException("Source object was created while stream was open.");
        }
        catch (AmazonS3Exception ex) when (ex.StatusCode == HttpStatusCode.NotFound)
        {
        }
    }
}
