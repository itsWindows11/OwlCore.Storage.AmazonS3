using Amazon.S3;
using Amazon.S3.Model;
using System.IO;
using System.Runtime.CompilerServices;

namespace OwlCore.Storage.AmazonS3;

/// <summary>
/// Represents an Amazon S3-backed folder implementing OwlCore storage folder abstractions.
/// </summary>
public partial class S3Folder :
    IModifiableFolder,
    IChildFolder,
    IGetItem,
    IGetItemRecursive,
    ICreateRenamedCopyOf,
    IMoveRenamedFrom
{
    /// <inheritdoc />
    public Task<IFolder?> GetParentAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (string.IsNullOrEmpty(Path))
            return Task.FromResult<IFolder?>(null);

        var lastSlashIndex = Path.LastIndexOf('/');
        var parentPath = lastSlashIndex >= 0 ? Path[..lastSlashIndex] : string.Empty;

        if (string.IsNullOrEmpty(parentPath))
            return Task.FromResult<IFolder?>(null);

        return Task.FromResult<IFolder?>(new S3Folder(AmazonS3Client, BucketName, parentPath));
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<IStorableChild> GetItemsAsync(
        StorableType type = StorableType.All,
        [EnumeratorCancellation] CancellationToken cancellationToken = default
    )
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (type == StorableType.None)
            throw new ArgumentOutOfRangeException(nameof(type));

        var request = new ListObjectsV2Request
        {
            BucketName = BucketName,
            Prefix = Prefix,
            Delimiter = "/"
        };

        cancellationToken.ThrowIfCancellationRequested();

        do
        {
            cancellationToken.ThrowIfCancellationRequested();

            var response = await AmazonS3Client.ListObjectsV2Async(request, cancellationToken);

            if (type.HasFlag(StorableType.Folder))
            {
                cancellationToken.ThrowIfCancellationRequested();

                var emittedFolders = new HashSet<string>(StringComparer.Ordinal);

                foreach (var commonPrefix in response.CommonPrefixes ?? Enumerable.Empty<string>())
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    var trimmed = commonPrefix.Trim('/');
                    if (string.IsNullOrEmpty(trimmed))
                        continue;

                    var folderName = trimmed[(trimmed.LastIndexOf('/') + 1)..];
                    if (string.IsNullOrEmpty(folderName) || !emittedFolders.Add(folderName))
                        continue;

                    yield return new S3Folder(AmazonS3Client, BucketName, CombinePath(Path, folderName));
                }

                foreach (var s3Object in response.S3Objects ?? Enumerable.Empty<S3Object>())
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    var key = s3Object.Key;
                    if (key == Prefix || !key.EndsWith("/", StringComparison.Ordinal))
                        continue;

                    var relative = Prefix.Length == 0 ? key : key[Prefix.Length..];
                    if (string.IsNullOrEmpty(relative))
                        continue;

                    var slashIndex = relative.IndexOf('/');
                    if (slashIndex < 0)
                        continue;

                    var folderName = relative[..slashIndex];
                    if (string.IsNullOrEmpty(folderName) || !emittedFolders.Add(folderName))
                        continue;

                    yield return new S3Folder(AmazonS3Client, BucketName, CombinePath(Path, folderName));
                }
            }

            if (type.HasFlag(StorableType.File))
            {
                cancellationToken.ThrowIfCancellationRequested();

                foreach (var s3Object in response.S3Objects ?? Enumerable.Empty<S3Object>())
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    if (s3Object.Key == Prefix
                        || s3Object.Key.EndsWith("/", StringComparison.Ordinal)
                        || string.Equals(Normalize(s3Object.Key), Path, StringComparison.Ordinal))
                        continue;

                    var fileName = Prefix.Length == 0 ? s3Object.Key : s3Object.Key[Prefix.Length..];
                    if (fileName.Contains('/'))
                        continue;

                    // Placeholder files for empty folders are a common pattern in S3. Ignore those by default.
                    if (string.Equals(fileName, ".emptyFolderPlaceholder", StringComparison.Ordinal)
                        || string.Equals(fileName, ".keep", StringComparison.Ordinal)
                        || string.Equals(fileName, ".folder", StringComparison.Ordinal))
                        continue;

                    yield return new S3File(AmazonS3Client, BucketName, Path, fileName);
                }
            }

            request.ContinuationToken = response.NextContinuationToken;

            cancellationToken.ThrowIfCancellationRequested();
        }
        while (!string.IsNullOrEmpty(request.ContinuationToken));
    }

    /// <inheritdoc />
    public Task<IFolderWatcher> GetFolderWatcherAsync(CancellationToken cancellationToken = default)
        => throw new NotSupportedException("Folder watcher is not supported for Amazon S3.");

    /// <inheritdoc />
    public async Task DeleteAsync(IStorableChild item, CancellationToken cancellationToken = default)
    {
        if (item is null)
            throw new ArgumentNullException(nameof(item));

        var itemId = Normalize(item.Id);

        if (item is IChildFolder)
        {
            var folderPrefix = itemId.EndsWith("/", StringComparison.Ordinal) ? itemId : $"{itemId}/";
            string? continuationToken = null;

            do
            {
                var listResponse = await AmazonS3Client.ListObjectsV2Async(new ListObjectsV2Request
                {
                    BucketName = BucketName,
                    Prefix = folderPrefix,
                    ContinuationToken = continuationToken
                }, cancellationToken);

                if (listResponse.S3Objects?.Count > 0)
                {
                    var deleteRequest = new DeleteObjectsRequest
                    {
                        BucketName = BucketName,
                        Objects = [..listResponse.S3Objects.Select(x => new KeyVersion { Key = x.Key })]
                    };

                    await AmazonS3Client.DeleteObjectsAsync(deleteRequest, cancellationToken);
                }

                continuationToken = listResponse.NextContinuationToken;
            }
            while (!string.IsNullOrEmpty(continuationToken));

            await TryDeleteObjectIfExistsAsync(folderPrefix, cancellationToken);

            return;
        }

        await TryDeleteObjectIfExistsAsync(itemId, cancellationToken);
    }

    /// <inheritdoc />
    public async Task<IChildFolder> CreateFolderAsync(string name, bool overwrite = false, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(name))
            throw new ArgumentException("Value cannot be null or whitespace.", nameof(name));

        var normalizedName = Normalize(name);
        var folderPath = CombinePath(Path, normalizedName);
        var markerKey = $"{folderPath}/";

        var fileExists = await TryGetObjectMetadataAsync(folderPath, cancellationToken) is not null;
        var folderExists = await FolderExistsAsync(folderPath, cancellationToken);

        if (!overwrite)
        {
            if (folderExists)
                return new S3Folder(AmazonS3Client, BucketName, folderPath);

            if (fileExists)
                throw new IOException($"A file already exists at '{folderPath}'.");
        }
        else
        {
            if (fileExists)
                await TryDeleteObjectIfExistsAsync(folderPath, cancellationToken);

            if (folderExists)
            {
                string? continuationToken = null;

                do
                {
                    var listResponse = await AmazonS3Client.ListObjectsV2Async(new ListObjectsV2Request
                    {
                        BucketName = BucketName,
                        Prefix = markerKey,
                        ContinuationToken = continuationToken
                    }, cancellationToken);

                    if (listResponse.S3Objects?.Count > 0)
                    {
                        await AmazonS3Client.DeleteObjectsAsync(new DeleteObjectsRequest
                        {
                            BucketName = BucketName,
                            Objects = [..listResponse.S3Objects.Select(x => new KeyVersion { Key = x.Key })]
                        }, cancellationToken);
                    }

                    continuationToken = listResponse.NextContinuationToken;
                }
                while (!string.IsNullOrEmpty(continuationToken));

                await TryDeleteObjectIfExistsAsync(markerKey, cancellationToken);
            }
        }

        await AmazonS3Client.PutObjectAsync(new PutObjectRequest
        {
            BucketName = BucketName,
            Key = markerKey,
            ContentBody = string.Empty
        }, cancellationToken);

        return new S3Folder(AmazonS3Client, BucketName, folderPath);
    }

    /// <inheritdoc />
    public async Task<IChildFile> CreateFileAsync(string name, bool overwrite = false, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(name))
            throw new ArgumentException("Value cannot be null or whitespace.", nameof(name));

        var normalizedName = Normalize(name);
        var key = CombinePath(Path, normalizedName);

        if (!overwrite)
        {
            if (await TryGetObjectMetadataAsync(key, cancellationToken) is not null)
                return new S3File(AmazonS3Client, BucketName, Path, normalizedName);

            if (await FolderExistsAsync(key, cancellationToken))
                throw new IOException($"A folder already exists at '{key}/'.");
        }

        await AmazonS3Client.PutObjectAsync(new PutObjectRequest
        {
            BucketName = BucketName,
            Key = key,
            ContentBody = string.Empty
        }, cancellationToken);

        return new S3File(AmazonS3Client, BucketName, Path, normalizedName);
    }

    /// <inheritdoc />
    public async Task<IStorableChild> GetItemAsync(string id, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(id))
            throw new ArgumentException("Value cannot be null or whitespace.", nameof(id));

        var requestedFolder = id.Replace('\\', '/').EndsWith("/", StringComparison.Ordinal);
        var fullId = ResolvePath(id);

        var fileMetadata = requestedFolder
            ? null
            : await TryGetObjectMetadataAsync(fullId, cancellationToken);

        var folderExists = await FolderExistsAsync(fullId, cancellationToken);

        // S3 can contain both an object key ("foo") and a pseudo-folder prefix ("foo/").
        // Treat that as ambiguous and force callers to disambiguate with a trailing slash.
        if (fileMetadata is not null && folderExists)
            throw new IOException($"The path '{fullId}' is ambiguous in S3 (both file and folder exist). Use a trailing '/' to target the folder.");

        if (fileMetadata is not null)
        {
            var fileName = fullId[(fullId.LastIndexOf('/') + 1)..];
            var parentPath = fullId.Contains('/') ? fullId[..fullId.LastIndexOf('/')] : string.Empty;
            return new S3File(AmazonS3Client, BucketName, parentPath, fileName);
        }

        if (folderExists)
            return new S3Folder(AmazonS3Client, BucketName, fullId);

        throw new FileNotFoundException($"Item '{fullId}' was not found.");
    }

    /// <inheritdoc />
    public async Task<IStorableChild> GetItemRecursiveAsync(string id, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(id))
            throw new ArgumentException("Value cannot be null or whitespace.", nameof(id));

        var requestedFolder = id.Replace('\\', '/').EndsWith("/", StringComparison.Ordinal);
        var fullId = ResolvePath(id);

        var fileMetadata = requestedFolder
            ? null
            : await TryGetObjectMetadataAsync(fullId, cancellationToken);

        var folderExists = await FolderExistsAsync(fullId, cancellationToken);

        // S3 can contain both an object key ("foo") and a pseudo-folder prefix ("foo/").
        // Treat that as ambiguous and force callers to disambiguate with a trailing slash.
        if (fileMetadata is not null && folderExists)
            throw new IOException($"The path '{fullId}' is ambiguous in S3 (both file and folder exist). Use a trailing '/' to target the folder.");

        if (fileMetadata is not null)
        {
            var fileName = fullId[(fullId.LastIndexOf('/') + 1)..];
            var parentPath = fullId.Contains('/') ? fullId[..fullId.LastIndexOf('/')] : string.Empty;
            return new S3File(AmazonS3Client, BucketName, parentPath, fileName);
        }

        if (folderExists)
            return new S3Folder(AmazonS3Client, BucketName, fullId);

        throw new FileNotFoundException($"Item '{fullId}' was not found.");
    }

    /// <inheritdoc cref="ICreateCopyOf.CreateCopyOfAsync(IFile, bool, CancellationToken, CreateCopyOfDelegate)"/>
    public async Task<IChildFile> CreateCopyOfAsync(IFile sourceFile, bool overwrite, CancellationToken cancellationToken, CreateCopyOfDelegate fallback)
    {
        if (sourceFile is null)
            throw new ArgumentNullException(nameof(sourceFile));

        if (sourceFile is not S3File s3Source)
            return await fallback(this, sourceFile, overwrite, cancellationToken);

        var result = await TryCreateS3CopyAsync(s3Source, sourceFile.Name, overwrite, cancellationToken);
        return result ?? await fallback(this, sourceFile, overwrite, cancellationToken);
    }

    /// <inheritdoc cref="ICreateRenamedCopyOf.CreateCopyOfAsync(IFile, bool, string, CancellationToken, CreateRenamedCopyOfDelegate)"/>
    public async Task<IChildFile> CreateCopyOfAsync(IFile sourceFile, bool overwrite, string desiredName, CancellationToken cancellationToken, CreateRenamedCopyOfDelegate fallback)
    {
        if (sourceFile is null)
            throw new ArgumentNullException(nameof(sourceFile));

        if (sourceFile is not S3File s3Source)
            return await fallback(this, sourceFile, overwrite, desiredName, cancellationToken);

        var fileName = string.IsNullOrWhiteSpace(desiredName) ? sourceFile.Name : desiredName;
        if (string.IsNullOrWhiteSpace(fileName))
            throw new ArgumentException("Value cannot be null or whitespace.", nameof(desiredName));

        var result = await TryCreateS3CopyAsync(s3Source, fileName, overwrite, cancellationToken);
        return result ?? await fallback(this, sourceFile, overwrite, desiredName, cancellationToken);
    }

    /// <inheritdoc cref="IMoveFrom.MoveFromAsync(IChildFile, IModifiableFolder, bool, CancellationToken, MoveFromDelegate)"/>
    public async Task<IChildFile> MoveFromAsync(IChildFile sourceFile, IModifiableFolder sourceFolder, bool overwrite, CancellationToken cancellationToken, MoveFromDelegate fallback)
    {
        if (sourceFile is null)
            throw new ArgumentNullException(nameof(sourceFile));

        if (sourceFolder is null)
            throw new ArgumentNullException(nameof(sourceFolder));

        if (sourceFile is not S3File s3Source)
            return await fallback(this, sourceFile, sourceFolder, overwrite, cancellationToken);

        var result = await TryCreateS3MoveAsync(s3Source, sourceFile.Name, overwrite, cancellationToken);
        return result ?? await fallback(sourceFolder, sourceFile, this, overwrite, cancellationToken);
    }

    /// <inheritdoc cref="IMoveRenamedFrom.MoveFromAsync(IChildFile, IModifiableFolder, bool, string, CancellationToken, MoveRenamedFromDelegate)"/>
    public async Task<IChildFile> MoveFromAsync(IChildFile sourceFile, IModifiableFolder sourceFolder, bool overwrite, string desiredName, CancellationToken cancellationToken, MoveRenamedFromDelegate fallback)
    {
        if (sourceFile is null)
            throw new ArgumentNullException(nameof(sourceFile));

        if (sourceFolder is null)
            throw new ArgumentNullException(nameof(sourceFolder));

        if (sourceFile is not S3File s3Source)
            return await fallback(this, sourceFile, sourceFolder, overwrite, desiredName, cancellationToken);

        var fileName = string.IsNullOrWhiteSpace(desiredName) ? sourceFile.Name : desiredName;
        if (string.IsNullOrWhiteSpace(fileName))
            throw new ArgumentException("Value cannot be null or whitespace.", nameof(desiredName));

        var result = await TryCreateS3MoveAsync(s3Source, fileName, overwrite, cancellationToken);
        return result ?? await fallback(sourceFolder, sourceFile, this, overwrite, desiredName, cancellationToken);
    }
}
