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

        return Task.FromResult<IFolder?>(new S3Folder(AmazonS3Client, BucketName, parentPath, nameOverride: null, assumeNormalized: true));
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

        var prefix = Prefix;
        var prefixLength = prefix.Length;
        var path = Path;

        var request = new ListObjectsV2Request
        {
            BucketName = BucketName,
            Prefix = prefix,
            Delimiter = "/"
        };

        cancellationToken.ThrowIfCancellationRequested();

        do
        {
            cancellationToken.ThrowIfCancellationRequested();

            var response = await AmazonS3Client.ListObjectsV2Async(request, cancellationToken).ConfigureAwait(false);

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

                    var lastSlashIndex = trimmed.LastIndexOf('/');
                    var folderName = lastSlashIndex >= 0 ? trimmed[(lastSlashIndex + 1)..] : trimmed;
                    if (string.IsNullOrEmpty(folderName) || !emittedFolders.Add(folderName))
                        continue;

                    var childPath = string.IsNullOrEmpty(path) ? folderName : $"{path}/{folderName}";
                    yield return new S3Folder(AmazonS3Client, BucketName, childPath, folderName, assumeNormalized: true);
                }

                foreach (var s3Object in response.S3Objects ?? Enumerable.Empty<S3Object>())
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    var key = s3Object.Key;
                    if (key == prefix || !key.EndsWith("/", StringComparison.Ordinal))
                        continue;

                    var relative = prefixLength == 0 ? key : key[prefixLength..];
                    if (string.IsNullOrEmpty(relative))
                        continue;

                    var slashIndex = relative.IndexOf('/');
                    if (slashIndex < 0)
                        continue;

                    var folderName = relative[..slashIndex];
                    if (string.IsNullOrEmpty(folderName) || !emittedFolders.Add(folderName))
                        continue;

                    var childPath = string.IsNullOrEmpty(path) ? folderName : $"{path}/{folderName}";
                    yield return new S3Folder(AmazonS3Client, BucketName, childPath, folderName, assumeNormalized: true);
                }
            }

            if (type.HasFlag(StorableType.File))
            {
                cancellationToken.ThrowIfCancellationRequested();

                foreach (var s3Object in response.S3Objects ?? Enumerable.Empty<S3Object>())
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    var key = s3Object.Key;
                    if (key == prefix
                        || key.EndsWith("/", StringComparison.Ordinal)
                        || string.Equals(key, path, StringComparison.Ordinal))
                        continue;

                    var fileName = prefixLength == 0 ? key : key[prefixLength..];
                    if (fileName.Contains('/'))
                        continue;

                    if (string.Equals(fileName, ".emptyFolderPlaceholder", StringComparison.Ordinal)
                        || string.Equals(fileName, ".keep", StringComparison.Ordinal)
                        || string.Equals(fileName, ".folder", StringComparison.Ordinal))
                        continue;

                    yield return new S3File(AmazonS3Client, BucketName, key, fileName, assumeNormalized: true);
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
            await DeleteObjectsByPrefixAsync(folderPrefix, cancellationToken).ConfigureAwait(false);
            await TryDeleteObjectIfExistsAsync(folderPrefix, cancellationToken).ConfigureAwait(false);
            return;
        }

        await TryDeleteObjectIfExistsAsync(itemId, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<IChildFolder> CreateFolderAsync(string name, bool overwrite = false, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(name))
            throw new ArgumentException("Value cannot be null or whitespace.", nameof(name));

        var normalizedName = Normalize(name);
        var folderPath = CombinePath(Path, normalizedName);
        var markerKey = $"{folderPath}/";

        var fileExists = await TryGetObjectMetadataAsync(folderPath, cancellationToken).ConfigureAwait(false) is not null;
        var folderExists = await FolderExistsAsync(folderPath, cancellationToken).ConfigureAwait(false);

        if (!overwrite)
        {
            if (folderExists)
                return new S3Folder(AmazonS3Client, BucketName, folderPath, nameOverride: null, assumeNormalized: true);
        }
        else
        {
            if (fileExists)
                await TryDeleteObjectIfExistsAsync(folderPath, cancellationToken).ConfigureAwait(false);

            if (folderExists)
            {
                await DeleteObjectsByPrefixAsync(markerKey, cancellationToken).ConfigureAwait(false);
                await TryDeleteObjectIfExistsAsync(markerKey, cancellationToken).ConfigureAwait(false);
            }
        }

        await AmazonS3Client.PutObjectAsync(new PutObjectRequest
        {
            BucketName = BucketName,
            Key = markerKey,
            ContentBody = string.Empty
        }, cancellationToken).ConfigureAwait(false);

        return new S3Folder(AmazonS3Client, BucketName, folderPath, nameOverride: null, assumeNormalized: true);
    }

    /// <inheritdoc />
    public async Task<IChildFile> CreateFileAsync(string name, bool overwrite = false, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(name))
            throw new ArgumentException("Value cannot be null or whitespace.", nameof(name));

        var normalizedName = Normalize(name);
        var key = CombinePath(Path, normalizedName);

        if (!overwrite && await TryGetObjectMetadataAsync(key, cancellationToken).ConfigureAwait(false) is not null)
            return new S3File(AmazonS3Client, BucketName, key, normalizedName, assumeNormalized: true);

        await AmazonS3Client.PutObjectAsync(new PutObjectRequest
        {
            BucketName = BucketName,
            Key = key,
            ContentBody = string.Empty
        }, cancellationToken).ConfigureAwait(false);

        return new S3File(AmazonS3Client, BucketName, key, normalizedName, assumeNormalized: true);
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
            : await TryGetObjectMetadataAsync(fullId, cancellationToken).ConfigureAwait(false);

        var folderExists = await FolderExistsAsync(fullId, cancellationToken).ConfigureAwait(false);

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
            return new S3Folder(AmazonS3Client, BucketName, fullId, nameOverride: null, assumeNormalized: true);

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
            : await TryGetObjectMetadataAsync(fullId, cancellationToken).ConfigureAwait(false);

        var folderExists = await FolderExistsAsync(fullId, cancellationToken).ConfigureAwait(false);

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
            return new S3Folder(AmazonS3Client, BucketName, fullId, nameOverride: null, assumeNormalized: true);

        throw new FileNotFoundException($"Item '{fullId}' was not found.");
    }

    /// <inheritdoc cref="ICreateCopyOf.CreateCopyOfAsync(IFile, bool, CancellationToken, CreateCopyOfDelegate)"/>
    public async Task<IChildFile> CreateCopyOfAsync(IFile sourceFile, bool overwrite, CancellationToken cancellationToken, CreateCopyOfDelegate fallback)
    {
        if (sourceFile is null)
            throw new ArgumentNullException(nameof(sourceFile));

        if (sourceFile is not S3File s3Source)
            return await fallback(this, sourceFile, overwrite, cancellationToken).ConfigureAwait(false);

        var result = await TryCreateS3CopyAsync(s3Source, sourceFile.Name, overwrite, cancellationToken).ConfigureAwait(false);
        return result ?? await fallback(this, sourceFile, overwrite, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc cref="ICreateRenamedCopyOf.CreateCopyOfAsync(IFile, bool, string, CancellationToken, CreateRenamedCopyOfDelegate)"/>
    public async Task<IChildFile> CreateCopyOfAsync(IFile sourceFile, bool overwrite, string desiredName, CancellationToken cancellationToken, CreateRenamedCopyOfDelegate fallback)
    {
        if (sourceFile is null)
            throw new ArgumentNullException(nameof(sourceFile));

        if (sourceFile is not S3File s3Source)
            return await fallback(this, sourceFile, overwrite, desiredName, cancellationToken).ConfigureAwait(false);

        var fileName = string.IsNullOrWhiteSpace(desiredName) ? sourceFile.Name : desiredName;
        if (string.IsNullOrWhiteSpace(fileName))
            throw new ArgumentException("Value cannot be null or whitespace.", nameof(desiredName));

        var result = await TryCreateS3CopyAsync(s3Source, fileName, overwrite, cancellationToken).ConfigureAwait(false);
        return result ?? await fallback(this, sourceFile, overwrite, desiredName, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc cref="IMoveFrom.MoveFromAsync(IChildFile, IModifiableFolder, bool, CancellationToken, MoveFromDelegate)"/>
    public async Task<IChildFile> MoveFromAsync(IChildFile sourceFile, IModifiableFolder sourceFolder, bool overwrite, CancellationToken cancellationToken, MoveFromDelegate fallback)
    {
        if (sourceFile is null)
            throw new ArgumentNullException(nameof(sourceFile));

        if (sourceFolder is null)
            throw new ArgumentNullException(nameof(sourceFolder));

        if (sourceFile is not S3File s3Source)
            return await fallback(this, sourceFile, sourceFolder, overwrite, cancellationToken).ConfigureAwait(false);

        var result = await TryCreateS3MoveAsync(s3Source, sourceFile.Name, overwrite, cancellationToken).ConfigureAwait(false);
        return result ?? await fallback(this, sourceFile, sourceFolder, overwrite, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc cref="IMoveRenamedFrom.MoveFromAsync(IChildFile, IModifiableFolder, bool, string, CancellationToken, MoveRenamedFromDelegate)"/>
    public async Task<IChildFile> MoveFromAsync(IChildFile sourceFile, IModifiableFolder sourceFolder, bool overwrite, string desiredName, CancellationToken cancellationToken, MoveRenamedFromDelegate fallback)
    {
        if (sourceFile is null)
            throw new ArgumentNullException(nameof(sourceFile));

        if (sourceFolder is null)
            throw new ArgumentNullException(nameof(sourceFolder));

        if (sourceFile is not S3File s3Source)
            return await fallback(this, sourceFile, sourceFolder, overwrite, desiredName, cancellationToken).ConfigureAwait(false);

        var fileName = string.IsNullOrWhiteSpace(desiredName) ? sourceFile.Name : desiredName;
        if (string.IsNullOrWhiteSpace(fileName))
            throw new ArgumentException("Value cannot be null or whitespace.", nameof(desiredName));

        var result = await TryCreateS3MoveAsync(s3Source, fileName, overwrite, cancellationToken).ConfigureAwait(false);
        return result ?? await fallback(this, sourceFile, sourceFolder, overwrite, desiredName, cancellationToken).ConfigureAwait(false);
    }
}
