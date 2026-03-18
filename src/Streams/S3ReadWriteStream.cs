using Amazon.S3;
using Amazon.S3.Model;
using System.Net;

namespace OwlCore.Storage.AmazonS3.Streams;

public sealed partial class S3ReadWriteStream : Stream
{
    private const int PartSize = 8 * 1024 * 1024;
    private const int BlockSize = 1024 * 1024;

    private readonly IAmazonS3 _amazonS3Client;
    private readonly string _bucketName;
    private readonly string _key;
    private readonly FileAccess _accessMode;

    private readonly Dictionary<long, byte[]> _dirtyBlocks = new Dictionary<long, byte[]>();
    private readonly Dictionary<long, byte[]> _sourceBlockCache = new Dictionary<long, byte[]>();
    private readonly SemaphoreSlim _sync = new SemaphoreSlim(1, 1);

    private readonly bool _sourceExists;
    private readonly long _sourceLength;
    private readonly string? _sourceETag;

    private long _length;
    private long _position;
    private bool _hasWrites;
    private bool _finalized;
    private bool _disposed;

    public override bool CanRead => !_disposed && _accessMode != FileAccess.Write;
    public override bool CanSeek => !_disposed;
    public override bool CanWrite => !_disposed && _accessMode != FileAccess.Read;

    public override long Length
    {
        get
        {
            _sync.Wait();
            try
            {
                ThrowIfDisposedOrFinalized();
                return _length;
            }
            finally
            {
                _sync.Release();
            }
        }
    }

    public override long Position
    {
        get
        {
            _sync.Wait();
            try
            {
                ThrowIfDisposedOrFinalized();
                return _position;
            }
            finally
            {
                _sync.Release();
            }
        }
        set
        {
            _sync.Wait();
            try
            {
                ThrowIfDisposedOrFinalized();
                ThrowHelper.ThrowIfNegative(value, nameof(value));

                _position = value;
            }
            finally
            {
                _sync.Release();
            }
        }
    }

    private S3ReadWriteStream(
        IAmazonS3 amazonS3Client,
        string bucketName,
        string key,
        FileAccess accessMode,
        bool sourceExists,
        long sourceLength,
        string? sourceETag)
    {
        _amazonS3Client = amazonS3Client;
        _bucketName = bucketName;
        _key = key;
        _accessMode = accessMode;
        _sourceExists = sourceExists;
        _sourceLength = sourceLength;
        _sourceETag = sourceETag;
        _length = sourceLength;
    }

    public static async Task<S3ReadWriteStream> CreateAsync(
        IAmazonS3 amazonS3Client,
        string bucketName,
        string key,
        FileAccess accessMode,
        CancellationToken cancellationToken = default)
    {
        if (accessMode < FileAccess.Read || accessMode > FileAccess.ReadWrite)
            throw new ArgumentOutOfRangeException(nameof(accessMode), accessMode, "Unsupported file access mode.");

        bool sourceExists = false;
        long sourceLength = 0;
        string? sourceETag = null;

        try
        {
            var metadata = await amazonS3Client.GetObjectMetadataAsync(new GetObjectMetadataRequest
            {
                BucketName = bucketName,
                Key = key
            }, cancellationToken);

            sourceExists = true;
            sourceLength = metadata.ContentLength;
            sourceETag = metadata.ETag;
        }
        catch (AmazonS3Exception ex) when (ex.StatusCode == HttpStatusCode.NotFound && accessMode != FileAccess.Read)
        {
        }

        if (!sourceExists && accessMode == FileAccess.Read)
            throw new FileNotFoundException($"S3 object '{bucketName}/{key}' was not found.");

        return new S3ReadWriteStream(amazonS3Client, bucketName, key, accessMode, sourceExists, sourceLength, sourceETag);
    }

    public override void Flush()
    {
        _sync.Wait();
        try
        {
            ThrowIfDisposedOrFinalized();
        }
        finally
        {
            _sync.Release();
        }
    }

    public override async Task FlushAsync(CancellationToken cancellationToken)
    {
        await _sync.WaitAsync(cancellationToken);
        try
        {
            ThrowIfDisposedOrFinalized();
        }
        finally
        {
            _sync.Release();
        }
    }

    public override int Read(byte[] buffer, int offset, int count)
    {
        _sync.Wait();
        try
        {
            EnsureCanRead();
            ValidateBufferArgs(buffer, offset, count);

            var toRead = (int)Math.Min(count, Math.Max(0, _length - _position));
            if (toRead == 0)
                return 0;

            ReadAtCoreSync(_position, buffer.AsSpan(offset, toRead));
            _position += toRead;
            return toRead;
        }
        finally
        {
            _sync.Release();
        }
    }

#if !NETSTANDARD2_0
    public override int Read(Span<byte> buffer)
    {
        _sync.Wait();
        try
        {
            EnsureCanRead();

            var toRead = (int)Math.Min(buffer.Length, Math.Max(0, _length - _position));
            if (toRead == 0)
                return 0;

            ReadAtCoreSync(_position, buffer[..toRead]);
            _position += toRead;
            return toRead;
        }
        finally
        {
            _sync.Release();
        }
    }
#endif

    public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
    {
#if NETSTANDARD2_0
        return ReadAsyncCore(buffer, offset, count, cancellationToken);
#else
        return ReadAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();
#endif
    }

#if !NETSTANDARD2_0
    public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
    {
        await _sync.WaitAsync(cancellationToken);
        try
        {
            EnsureCanRead();

            var toRead = (int)Math.Min(buffer.Length, Math.Max(0, _length - _position));
            if (toRead == 0)
                return 0;

            await ReadAtCoreAsync(_position, buffer[..toRead], cancellationToken);
            _position += toRead;
            return toRead;
        }
        finally
        {
            _sync.Release();
        }
    }
#endif

    public override long Seek(long offset, SeekOrigin origin)
    {
        _sync.Wait();
        try
        {
            ThrowIfDisposedOrFinalized();

            var newPosition = origin switch
            {
                SeekOrigin.Begin => offset,
                SeekOrigin.Current => _position + offset,
                SeekOrigin.End => _length + offset,
                _ => throw new ArgumentOutOfRangeException(nameof(origin))
            };

            if (newPosition < 0)
                throw new IOException("Attempted to seek before beginning of stream.");

            _position = newPosition;
            return _position;
        }
        finally
        {
            _sync.Release();
        }
    }

    public override void SetLength(long value)
    {
        _sync.Wait();
        try
        {
            EnsureCanWrite();
            ThrowHelper.ThrowIfNegative(value, nameof(value));

            if (value < _length)
            {
                var lastBlockToKeep = value == 0 ? -1 : (value - 1) / BlockSize;
                var blocksToRemove = _dirtyBlocks.Keys.Where(x => x > lastBlockToKeep).ToArray();
                foreach (var block in blocksToRemove)
                    _dirtyBlocks.Remove(block);

                if (value > 0)
                {
                    var tailBlockIndex = (value - 1) / BlockSize;
                    var tailUsedBytes = (int)(value - (tailBlockIndex * BlockSize));
                    if (_dirtyBlocks.TryGetValue(tailBlockIndex, out var tailBlock) && tailUsedBytes < tailBlock.Length)
                        Array.Clear(tailBlock, tailUsedBytes, tailBlock.Length - tailUsedBytes);
                }
                else
                {
                    _dirtyBlocks.Clear();
                }
            }

            _length = value;
            if (_position > _length)
                _position = _length;

            _hasWrites = true;
        }
        finally
        {
            _sync.Release();
        }
    }

    public override void Write(byte[] buffer, int offset, int count)
    {
        _sync.Wait();
        try
        {
            EnsureCanWrite();
            ValidateBufferArgs(buffer, offset, count);
            WriteAtCoreSync(_position, buffer.AsSpan(offset, count));
            _position += count;
            _length = Math.Max(_length, _position);
            _hasWrites = true;
        }
        finally
        {
            _sync.Release();
        }
    }

#if !NETSTANDARD2_0
    public override void Write(ReadOnlySpan<byte> buffer)
    {
        _sync.Wait();
        try
        {
            EnsureCanWrite();
            WriteAtCoreSync(_position, buffer);
            _position += buffer.Length;
            _length = Math.Max(_length, _position);
            _hasWrites = true;
        }
        finally
        {
            _sync.Release();
        }
    }
#endif

    public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
    {
#if NETSTANDARD2_0
        return WriteAsyncCore(buffer, offset, count, cancellationToken);
#else
        return WriteAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();
#endif
    }

#if !NETSTANDARD2_0
    public override async ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
    {
        await _sync.WaitAsync(cancellationToken);
        try
        {
            EnsureCanWrite();
            await WriteAtCoreAsync(_position, buffer, cancellationToken);
            _position += buffer.Length;
            _length = Math.Max(_length, _position);
            _hasWrites = true;
        }
        finally
        {
            _sync.Release();
        }
    }
#endif

    protected override void Dispose(bool disposing)
    {
        if (!disposing)
        {
            base.Dispose(disposing);
            return;
        }

        if (_disposed)
        {
            base.Dispose(disposing);
            return;
        }

        var lockTaken = false;
        try
        {
            _sync.Wait();
            lockTaken = true;

            if (_disposed)
            {
                base.Dispose(disposing);
                return;
            }

            if (!_finalized)
                CommitAsync(CancellationToken.None).GetAwaiter().GetResult();

            _disposed = true;
        }
        catch (ObjectDisposedException)
        {
            _disposed = true;
        }
        finally
        {
            if (lockTaken)
                _sync.Release();
        }

        _sync.Dispose();
        base.Dispose(disposing);
    }

#if !NETSTANDARD2_0
    public override async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;

        var lockTaken = false;
        try
        {
            await _sync.WaitAsync(CancellationToken.None);
            lockTaken = true;

            if (_disposed)
                return;

            if (!_finalized)
                await CommitAsync(CancellationToken.None);

            _disposed = true;
        }
        catch (ObjectDisposedException)
        {
            _disposed = true;
            return;
        }
        finally
        {
            if (lockTaken)
                _sync.Release();
        }

        _sync.Dispose();
        GC.SuppressFinalize(this);
    }
#endif

    private void EnsureCanRead()
    {
        ThrowIfDisposedOrFinalized();
        if (_accessMode == FileAccess.Write)
            throw new NotSupportedException("Stream does not support reading.");
    }

    private void EnsureCanWrite()
    {
        ThrowIfDisposedOrFinalized();
        if (_accessMode == FileAccess.Read)
            throw new NotSupportedException("Stream does not support writing.");
    }

    private void ThrowIfDisposedOrFinalized()
    {
        ThrowHelper.ThrowIfDisposed(_disposed || _finalized, this);
    }

    private static void ValidateBufferArgs(byte[] buffer, int offset, int count)
    {
        ThrowHelper.ThrowIfNull(buffer, nameof(buffer));
        ThrowHelper.ThrowIfNegative(offset, nameof(offset));
        ThrowHelper.ThrowIfNegative(count, nameof(count));

        if (offset > buffer.Length - count)
            throw new ArgumentException("Offset and count exceed buffer length.", nameof(offset));
    }
}
