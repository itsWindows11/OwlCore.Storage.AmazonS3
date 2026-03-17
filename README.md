# OwlCore.Storage.AmazonS3

[![CI](https://github.com/itsWindows11/OwlCore.Storage.AmazonS3/actions/workflows/ci.yml/badge.svg)](https://github.com/itsWindows11/OwlCore.Storage.AmazonS3/actions/workflows/ci.yml)
[![NuGet](https://img.shields.io/nuget/v/OwlCore.Storage.AmazonS3.svg)](https://www.nuget.org/packages/OwlCore.Storage.AmazonS3)

Amazon S3 (and S3-compatible) storage implementation for [`OwlCore.Storage`](https://github.com/Arlodotexe/OwlCore.Storage).

## Target frameworks

The library currently targets:

- `netstandard2.0`
- `net8.0`
- `net9.0`
- `net10.0`

## Packages

Main dependencies:

- `AWSSDK.S3` (`4.0.19`)
- `OwlCore.Storage` (`0.15.0`)

For `netstandard2.0`, compatibility packages are included automatically (`System.Buffers`, `System.Memory`, `System.Threading.Tasks.Extensions`, `PolySharp`).

## Main types

- `S3Folder`
- `S3File`
- `S3ReadWriteStream`

## Behavior notes

- Folder semantics are represented over object keys.
- Stream writes use multipart upload when needed.
- `CreateCopyOfAsync` / `MoveFromAsync` use server-side S3 copy when available.
- If a provider rejects S3 copy semantics (for example copy-source related 400 responses), the implementation falls back to stream-copy for compatibility.

## Basic usage

```csharp
using Amazon.Runtime;
using Amazon.S3;
using OwlCore.Storage.AmazonS3;

var client = new AmazonS3Client(
    new BasicAWSCredentials("accessKey", "secretKey"),
    new AmazonS3Config
    {
        ServiceURL = "https://your-s3-endpoint",
        ForcePathStyle = true,
        AuthenticationRegion = "us-east-1"
    });

var folder = new S3Folder(client, "my-bucket", "app-data");
var file = await folder.CreateFileAsync("hello.txt");
await file.WriteBytesAsync("Hello"u8.ToArray());
```

## Build

```bash
dotnet restore
dotnet build
```

## Tests

Test project:

- `tests/OwlCore.Storage.AmazonS3.Tests.csproj`

### Local required environment variables

- `SUPABASE_S3_ENDPOINT`
- `SUPABASE_S3_ACCESS_KEY`
- `SUPABASE_S3_SECRET_KEY`
- `SUPABASE_S3_BUCKET`

Optional:

- `SUPABASE_S3_REGION` (default `us-east-1`)
- `SUPABASE_S3_FORCE_PATH_STYLE` (default `true`)

A `tests.runsettings` file is included at repo root.

Run tests:

```bash
dotnet test tests/OwlCore.Storage.AmazonS3.Tests.csproj --settings tests.runsettings
```

The test project also defines `RunSettingsFilePath`, so this also works:

```bash
dotnet test tests/OwlCore.Storage.AmazonS3.Tests.csproj
```

### GitHub Actions test workflow

Workflow file:

- `.github/workflows/ci.yml`

`build` runs on every push/PR to `main`.

`test-supabase` runs only when required secrets are set.

Set these repository **Secrets**:

- `SUPABASE_S3_ENDPOINT`
- `SUPABASE_S3_ACCESS_KEY`
- `SUPABASE_S3_SECRET_KEY`
- `SUPABASE_S3_BUCKET`

Optional repository **Variables**:

- `SUPABASE_S3_REGION` (recommended `us-east-1`)
- `SUPABASE_S3_FORCE_PATH_STYLE` (`true` recommended)

### Test data cleanup

- folder tests are scoped under `oc-foldertests/...`
- file tests are scoped under `oc-filetests/...`
- those prefixes are cleaned before/after class execution

## License

See `LICENSE`.
