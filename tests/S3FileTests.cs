namespace OwlCore.Storage.AmazonS3.Tests;

[TestClass]
public class S3FileTests : CommonIFileTests
{
    private const int TestFileSizeBytes = 5 * 1024 * 1024;
    private const string FileTestsContainer = "oc-filetests";

    private static readonly string FileTestsRootPath = $"{FileTestsContainer}/{Ulid.NewUlid().ToString().ToLowerInvariant()}";
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

        await ClearPrefixAsync(_client, _bucketName, FileTestsRootPath);
    }

    public override async Task<IFile> CreateFileAsync()
    {
        var root = new S3Folder(_client!, _bucketName, FileTestsRootPath);
        var folder = await root.CreateFolderAsync(Ulid.NewUlid().ToString()) as IModifiableFolder;
        var file = await folder!.CreateFileAsync($"{Ulid.NewUlid()}.bin");

        var random = new byte[TestFileSizeBytes];
        Random.Shared.NextBytes(random);
        await file.WriteBytesAsync(random);

        return file;
    }

    [ClassCleanup]
    public static async Task ClassCleanupAsync()
    {
        if (_client is not null && !string.IsNullOrWhiteSpace(_bucketName))
            await ClearPrefixAsync(_client, _bucketName, FileTestsRootPath);

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
