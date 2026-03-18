namespace OwlCore.Storage.AmazonS3.Streams;

public sealed partial class S3ReadWriteStream
{
    private async Task<int> ReadAsyncCore(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
    {
        await _sync.WaitAsync(cancellationToken);
        try
        {
            EnsureCanRead();

            var toRead = (int)Math.Min(count, Math.Max(0, _length - _position));
            if (toRead == 0)
                return 0;

            await ReadAtCoreAsync(_position, buffer.AsMemory(offset, toRead), cancellationToken);
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
            await WriteAtCoreAsync(_position, buffer.AsMemory(offset, count), cancellationToken);
            _position += count;
            _length = Math.Max(_length, _position);
            _hasWrites = true;
        }
        finally
        {
            _sync.Release();
        }
    }

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
}
