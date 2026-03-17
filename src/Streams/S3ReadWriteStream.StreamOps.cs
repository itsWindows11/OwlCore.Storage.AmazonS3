namespace OwlCore.Storage.AmazonS3.Streams;

public sealed partial class S3ReadWriteStream
{
#if NETSTANDARD2_0
    private async Task<int> ReadAsyncCore(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
    {
        await _sync.WaitAsync(cancellationToken);
        try
        {
            EnsureCanRead();

            var toRead = (int)Math.Min(count, Math.Max(0, _length - _position));
            if (toRead == 0)
                return 0;

            await ReadAtCoreAsync(_position, buffer, offset, toRead, cancellationToken);
            _position += toRead;
            return toRead;
        }
        finally
        {
            _sync.Release();
        }
    }

    private async Task WriteAsyncCore(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
    {
        await _sync.WaitAsync(cancellationToken);
        try
        {
            EnsureCanWrite();
            await WriteAtCoreAsync(_position, buffer, offset, count, cancellationToken);
            _position += count;
            _length = Math.Max(_length, _position);
            _hasWrites = true;
        }
        finally
        {
            _sync.Release();
        }
    }
#endif

#if NETSTANDARD2_0
    private void ReadAtCoreSync(long absoluteOffset, byte[] destination, int destinationOffset, int count)
    {
        Array.Clear(destination, destinationOffset, count);

        var remaining = count;
        var destOffset = destinationOffset;
        var sourceOffset = absoluteOffset;

        while (remaining > 0)
        {
            var blockIndex = sourceOffset / BlockSize;
            var blockOffset = (int)(sourceOffset % BlockSize);
            var bytesThisBlock = Math.Min(remaining, BlockSize - blockOffset);

            var block = GetReadableBlockSync(blockIndex);
            Buffer.BlockCopy(block, blockOffset, destination, destOffset, bytesThisBlock);

            sourceOffset += bytesThisBlock;
            destOffset += bytesThisBlock;
            remaining -= bytesThisBlock;
        }
    }
#else
    private void ReadAtCoreSync(long absoluteOffset, Span<byte> destination)
    {
        destination.Clear();

        var remaining = destination.Length;
        var destOffset = 0;
        var sourceOffset = absoluteOffset;

        while (remaining > 0)
        {
            var blockIndex = sourceOffset / BlockSize;
            var blockOffset = (int)(sourceOffset % BlockSize);
            var bytesThisBlock = Math.Min(remaining, BlockSize - blockOffset);

            var block = GetReadableBlockSync(blockIndex);
            block.AsSpan(blockOffset, bytesThisBlock).CopyTo(destination.Slice(destOffset, bytesThisBlock));

            sourceOffset += bytesThisBlock;
            destOffset += bytesThisBlock;
            remaining -= bytesThisBlock;
        }
    }
#endif

#if NETSTANDARD2_0
    private async Task ReadAtCoreAsync(long absoluteOffset, byte[] destination, int destinationOffset, int count, CancellationToken cancellationToken)
    {
        Array.Clear(destination, destinationOffset, count);

        var remaining = count;
        var destOffset = destinationOffset;
        var sourceOffset = absoluteOffset;

        while (remaining > 0)
        {
            var blockIndex = sourceOffset / BlockSize;
            var blockOffset = (int)(sourceOffset % BlockSize);
            var bytesThisBlock = Math.Min(remaining, BlockSize - blockOffset);

            var block = await GetReadableBlockAsync(blockIndex, cancellationToken);
            Buffer.BlockCopy(block, blockOffset, destination, destOffset, bytesThisBlock);

            sourceOffset += bytesThisBlock;
            destOffset += bytesThisBlock;
            remaining -= bytesThisBlock;
        }
    }
#else
    private async Task ReadAtCoreAsync(long absoluteOffset, Memory<byte> destination, CancellationToken cancellationToken)
    {
        destination.Span.Clear();

        var remaining = destination.Length;
        var destOffset = 0;
        var sourceOffset = absoluteOffset;

        while (remaining > 0)
        {
            var blockIndex = sourceOffset / BlockSize;
            var blockOffset = (int)(sourceOffset % BlockSize);
            var bytesThisBlock = Math.Min(remaining, BlockSize - blockOffset);

            var block = await GetReadableBlockAsync(blockIndex, cancellationToken);
            block.AsMemory(blockOffset, bytesThisBlock).CopyTo(destination.Slice(destOffset, bytesThisBlock));

            sourceOffset += bytesThisBlock;
            destOffset += bytesThisBlock;
            remaining -= bytesThisBlock;
        }
    }
#endif

#if NETSTANDARD2_0
    private void WriteAtCoreSync(long absoluteOffset, byte[] source, int sourceOffset, int count)
    {
        var remaining = count;
        var localSourceOffset = sourceOffset;
        var writeOffset = absoluteOffset;

        while (remaining > 0)
        {
            var blockIndex = writeOffset / BlockSize;
            var blockOffset = (int)(writeOffset % BlockSize);
            var bytesThisBlock = Math.Min(remaining, BlockSize - blockOffset);

            var block = GetWritableBlockSync(blockIndex);
            Buffer.BlockCopy(source, localSourceOffset, block, blockOffset, bytesThisBlock);
            _dirtyBlocks[blockIndex] = block;

            writeOffset += bytesThisBlock;
            localSourceOffset += bytesThisBlock;
            remaining -= bytesThisBlock;
        }
    }
#else
    private void WriteAtCoreSync(long absoluteOffset, ReadOnlySpan<byte> source)
    {
        var remaining = source.Length;
        var sourceOffset = 0;
        var writeOffset = absoluteOffset;

        while (remaining > 0)
        {
            var blockIndex = writeOffset / BlockSize;
            var blockOffset = (int)(writeOffset % BlockSize);
            var bytesThisBlock = Math.Min(remaining, BlockSize - blockOffset);

            var block = GetWritableBlockSync(blockIndex);
            source.Slice(sourceOffset, bytesThisBlock).CopyTo(block.AsSpan(blockOffset, bytesThisBlock));
            _dirtyBlocks[blockIndex] = block;

            writeOffset += bytesThisBlock;
            sourceOffset += bytesThisBlock;
            remaining -= bytesThisBlock;
        }
    }
#endif

#if NETSTANDARD2_0
    private async Task WriteAtCoreAsync(long absoluteOffset, byte[] source, int sourceOffset, int count, CancellationToken cancellationToken)
    {
        var remaining = count;
        var localSourceOffset = sourceOffset;
        var writeOffset = absoluteOffset;

        while (remaining > 0)
        {
            var blockIndex = writeOffset / BlockSize;
            var blockOffset = (int)(writeOffset % BlockSize);
            var bytesThisBlock = Math.Min(remaining, BlockSize - blockOffset);

            var block = await GetWritableBlockAsync(blockIndex, cancellationToken);
            Buffer.BlockCopy(source, localSourceOffset, block, blockOffset, bytesThisBlock);
            _dirtyBlocks[blockIndex] = block;

            writeOffset += bytesThisBlock;
            localSourceOffset += bytesThisBlock;
            remaining -= bytesThisBlock;
        }
    }
#else
    private async Task WriteAtCoreAsync(long absoluteOffset, ReadOnlyMemory<byte> source, CancellationToken cancellationToken)
    {
        var remaining = source.Length;
        var sourceOffset = 0;
        var writeOffset = absoluteOffset;

        while (remaining > 0)
        {
            var blockIndex = writeOffset / BlockSize;
            var blockOffset = (int)(writeOffset % BlockSize);
            var bytesThisBlock = Math.Min(remaining, BlockSize - blockOffset);

            var block = await GetWritableBlockAsync(blockIndex, cancellationToken);
            source.Slice(sourceOffset, bytesThisBlock).Span.CopyTo(block.AsSpan(blockOffset, bytesThisBlock));
            _dirtyBlocks[blockIndex] = block;

            writeOffset += bytesThisBlock;
            sourceOffset += bytesThisBlock;
            remaining -= bytesThisBlock;
        }
    }
#endif
}
