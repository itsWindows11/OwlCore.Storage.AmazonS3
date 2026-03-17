namespace OwlCore.Storage.AmazonS3.Tests;

[TestClass]
public class S3FolderTests : CommonIModifiableFolderTests
{
    private const int TestFileSizeBytes = 5 * 1024 * 1024;
    private const string FolderTestsContainer = "oc-foldertests";

    private static readonly string FolderTestsRootPath = $"{FolderTestsContainer}/{Ulid.NewUlid().ToString().ToLowerInvariant()}";

    private static IAmazonS3? _client;
    private static string _bucketName = string.Empty;

    [ClassInitialize]
    public static async Task InitializeAsync(TestContext _)
    {
        var endpoint = Environment.GetEnvironmentVariable("SUPABASE_S3_ENDPOINT");
        var accessKey = Environment.GetEnvironmentVariable("SUPABASE_S3_ACCESS_KEY");
        var secretKey = Environment.GetEnvironmentVariable("SUPABASE_S3_SECRET_KEY");
        var bucket = Environment.GetEnvironmentVariable("SUPABASE_S3_BUCKET");

        if (string.IsNullOrWhiteSpace(endpoint)
            || string.IsNullOrWhiteSpace(accessKey)
            || string.IsNullOrWhiteSpace(secretKey)
            || string.IsNullOrWhiteSpace(bucket))
        {
            Assert.Inconclusive("Supabase S3 test configuration is missing. Set SUPABASE_S3_ENDPOINT, SUPABASE_S3_ACCESS_KEY, SUPABASE_S3_SECRET_KEY, and SUPABASE_S3_BUCKET.");
            return;
        }

        _bucketName = bucket;
        _client = new AmazonS3Client(new BasicAWSCredentials(accessKey, secretKey), new AmazonS3Config
        {
            ServiceURL = endpoint,
            ForcePathStyle = !bool.TryParse(Environment.GetEnvironmentVariable("SUPABASE_S3_FORCE_PATH_STYLE"), out var forcePathStyle) || forcePathStyle,
            AuthenticationRegion = Environment.GetEnvironmentVariable("SUPABASE_S3_REGION") ?? "us-east-1"
        });

        await ClearPrefixAsync(_client!, _bucketName, FolderTestsRootPath);
    }

    [TestInitialize]
    public async Task TestInitializeAsync()
    {
        if (_client is null || string.IsNullOrWhiteSpace(_bucketName))
            Assert.Inconclusive("Supabase S3 client is not initialized.");

        await ClearPrefixAsync(_client!, _bucketName, FolderTestsRootPath);
    }

    public override async Task<IModifiableFolder> CreateModifiableFolderAsync()
    {
        var root = new S3Folder(_client!, _bucketName, FolderTestsRootPath);
        var folderName = Ulid.NewUlid().ToString().ToLowerInvariant();
        return (await root.CreateFolderAsync(folderName)) as IModifiableFolder ?? throw new InvalidOperationException();
    }

    public override async Task<IModifiableFolder> CreateModifiableFolderWithItems(int fileCount, int folderCount)
    {
        var folder = await CreateModifiableFolderAsync();
        var baseName = Ulid.NewUlid().ToString().ToLowerInvariant();

        for (var i = 0; i < fileCount; i++)
            await folder.CreateFileAsync($"{baseName}_{i}.txt");

        for (var i = 0; i < folderCount; i++)
            await folder.CreateFolderAsync($"{baseName}_{i}");

        return folder;
    }

    // Have to override these tests for Supabase to respect 50MB limit.
    // TODO: Remove test once done with Supabase testing & refactored
    // to local server, or do file size limits in the tests upstream.
    [TestMethod]
    public new async Task CreateCopyOfAsyncTest()
    {
        var sourceFolder = await CreateModifiableFolderAsync();
        var destinationFolder = await CreateModifiableFolderAsync();

        var sourceFile = await sourceFolder.CreateFileAsync($"{Ulid.NewUlid()}.bin");

        var payload = new byte[TestFileSizeBytes];
        Random.Shared.NextBytes(payload);
        await sourceFile.WriteBytesAsync(payload);

        var copiedFile = await destinationFolder.CreateCopyOfAsync(sourceFile, overwrite: false);
        var copiedBytes = await copiedFile.ReadBytesAsync();

        Assert.AreEqual(payload.Length, copiedBytes.Length);
        CollectionAssert.AreEqual(payload, copiedBytes);
    }

    // Have to override these tests for Supabase to respect 50MB limit.
    // TODO: Remove test once done with Supabase testing & refactored
    // to local server, or do file size limits in the tests upstream.
    [TestMethod]
    public new async Task MoveFromAsyncTest()
    {
        var sourceFolder = await CreateModifiableFolderAsync();
        var destinationFolder = await CreateModifiableFolderAsync();

        var sourceFile = await sourceFolder.CreateFileAsync($"{Ulid.NewUlid()}.bin");

        var payload = new byte[TestFileSizeBytes];
        Random.Shared.NextBytes(payload);
        await sourceFile.WriteBytesAsync(payload);

        var movedFile = await destinationFolder.MoveFromAsync(sourceFile, sourceFolder, overwrite: false);
        var movedBytes = await movedFile.ReadBytesAsync();

        Assert.AreEqual(payload.Length, movedBytes.Length);
        CollectionAssert.AreEqual(payload, movedBytes);

        var sourceCount = 0;
        await foreach (var _ in sourceFolder.GetItemsAsync(StorableType.File))
            sourceCount++;

        Assert.AreEqual(0, sourceCount);
    }

    [ClassCleanup]
    public static async Task ClassCleanupAsync()
    {
        if (_client is not null && !string.IsNullOrWhiteSpace(_bucketName))
            await ClearPrefixAsync(_client, _bucketName, FolderTestsRootPath);

        _client?.Dispose();
        _client = null;
        _bucketName = string.Empty;
    }

    private static async Task ClearPrefixAsync(IAmazonS3 client, string bucketName, string prefix)
    {
        string? continuationToken = null;

        do
        {
            var page = await client.ListObjectsV2Async(new ListObjectsV2Request
            {
                BucketName = bucketName,
                Prefix = prefix,
                ContinuationToken = continuationToken
            });

            if (page.S3Objects?.Count > 0)
            {
                await client.DeleteObjectsAsync(new DeleteObjectsRequest
                {
                    BucketName = bucketName,
                    Objects = [..(page.S3Objects ?? []).Select(x => new KeyVersion { Key = x.Key })]
                });
            }

            continuationToken = page.IsTruncated == true ? page.NextContinuationToken : null;
        }
        while (!string.IsNullOrWhiteSpace(continuationToken));
    }
}
