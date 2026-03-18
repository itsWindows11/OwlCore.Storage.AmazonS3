using Amazon.S3.Model;

namespace OwlCore.Storage.AmazonS3.Streams;

public sealed partial class S3ReadWriteStream
{
    private byte[] GetWritableBlockSync(long blockIndex)
    {
        // For write-only mode, dirty blocks are always present
        if (_dirtyBlocks != null && _dirtyBlocks.TryGetValue(blockIndex, out var dirty))
            return dirty;

        var sourceBlock = GetSourceBlockSync(blockIndex);
        var clone = new byte[BlockSize];
        Buffer.BlockCopy(sourceBlock, 0, clone, 0, BlockSize);

        // Only cache if we have dirty block tracking
        if (_dirtyBlocks != null)
            _dirtyBlocks[blockIndex] = clone;

        return clone;
    }

    private async Task<byte[]> GetWritableBlockAsync(long blockIndex, CancellationToken cancellationToken)
    {
        // For write-only mode, dirty blocks are always present
        if (_dirtyBlocks != null && _dirtyBlocks.TryGetValue(blockIndex, out var dirty))
            return dirty;

        var sourceBlock = await GetSourceBlockAsync(blockIndex, cancellationToken).ConfigureAwait(false);
        var clone = new byte[BlockSize];
        Buffer.BlockCopy(sourceBlock, 0, clone, 0, BlockSize);

        // Only cache if we have dirty block tracking
        if (_dirtyBlocks != null)
            _dirtyBlocks[blockIndex] = clone;

        return clone;
    }

    private byte[] GetReadableBlockSync(long blockIndex)
    {
        // Check dirty blocks if they exist (read-write or write-only mode)
        if (_dirtyBlocks != null && _dirtyBlocks.TryGetValue(blockIndex, out var dirty))
            return dirty;

        return GetSourceBlockSync(blockIndex);
    }

    private async Task<byte[]> GetReadableBlockAsync(long blockIndex, CancellationToken cancellationToken)
    {
        // Check dirty blocks if they exist (read-write or write-only mode)
        if (_dirtyBlocks != null && _dirtyBlocks.TryGetValue(blockIndex, out var dirty))
            return dirty;

        return await GetSourceBlockAsync(blockIndex, cancellationToken).ConfigureAwait(false);
    }

    private byte[] GetSourceBlockSync(long blockIndex)
        => GetSourceBlockAsync(blockIndex, CancellationToken.None).GetAwaiter().GetResult();

    private async Task<byte[]> GetSourceBlockAsync(long blockIndex, CancellationToken cancellationToken)
    {
        // Use cache only if source block cache is available (read-write or read-only mode)
        if (_sourceBlockCache != null && _sourceBlockCache.TryGetValue(blockIndex, out var cached))
            return cached;

        var block = new byte[BlockSize];
        if (_sourceExists)
        {
            var blockStart = blockIndex * BlockSize;
            if (blockStart < _sourceLength)
            {
                var blockEnd = Math.Min(_sourceLength - 1, blockStart + BlockSize - 1);

                var request = new GetObjectRequest
                {
                    BucketName = _bucketName,
                    Key = _key,
                    ByteRange = new ByteRange(blockStart, blockEnd)
                };

                if (!string.IsNullOrWhiteSpace(_sourceETag))
                    request.EtagToMatch = _sourceETag;

                using var response = await _amazonS3Client.GetObjectAsync(request, cancellationToken).ConfigureAwait(false);

                var total = 0;
                while (total < block.Length)
                {
#if NETSTANDARD2_0
                    var read = await response.ResponseStream.ReadAsync(block, total, block.Length - total, cancellationToken).ConfigureAwait(false);
#else
                    var read = await response.ResponseStream.ReadAsync(block.AsMemory(total, block.Length - total), cancellationToken).ConfigureAwait(false);
#endif
                    if (read == 0)
                        break;

                    total += read;
                }
            }
        }

        // Cache only if source block cache is available
        if (_sourceBlockCache != null)
            _sourceBlockCache[blockIndex] = block;

        return block;
    }
}
