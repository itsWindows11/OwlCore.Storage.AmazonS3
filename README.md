# OwlCore.Storage.AmazonS3

AWS S3 storage implementation for OwlCore.Storage abstractions.

## Target Frameworks

This library supports multiple target frameworks:

- **.NET Standard 2.0** - For maximum compatibility with .NET Framework 4.6.1+ and .NET Core 2.0+
- **.NET 8.0** - For .NET 8 applications
- **.NET 9.0** - For .NET 9 applications

## NuGet Dependencies

### All Targets
- `AWSSDK.S3` (>= 4.0.19)
- `OwlCore.Storage` (>= 0.15.0)

### .NET Standard 2.0 Only
When targeting .NET Standard 2.0, the following additional polyfill packages are automatically included:

- `System.Buffers` (>= 4.6.1) - Provides `ArrayPool<T>` support
- `System.Memory` (>= 4.6.3) - Provides `Memory<T>` and `Span<T>` types
- `System.Threading.Tasks.Extensions` (>= 4.6.3) - Provides `ValueTask` and `ValueTask<T>` support
- `PolySharp` (>= 1.15.0)  </br> Provides support for modern C# features in older frameworks.

## Features

- **S3 Stream Operations**: Read and write operations with efficient block-based caching
- **Multi-part Upload Support**: Automatic handling of large file uploads
- **Async/Await**: Full asynchronous API support across all target frameworks
- **Conditional Compilation**: Optimized implementations per target framework for best performance

## API Compatibility Notes

### .NET Standard 2.0
On .NET Standard 2.0, certain modern Stream overloads (`Span<byte>`, `Memory<byte>`) are not available in the public API, but the library automatically uses array-based alternatives internally for full compatibility.

### .NET 8.0 & 9.0
Modern .NET versions benefit from zero-allocation `Span<T>` and `Memory<T>` APIs for improved performance.

## Usage

```csharp
using OwlCore.Storage.AmazonS3;
using Amazon.S3;

// Create S3 client
var amazonS3Client = new AmazonS3Client("your-access-key", "your-secret-key", Amazon.RegionEndpoint.USEast1);

// Create S3 file wrapper
var s3File = new S3File(amazonS3Client, "my-bucket", "path/to", "file.txt");

// Open stream for reading or writing
using var stream = await s3File.OpenStreamAsync(FileAccess.ReadWrite);

// Perform stream operations
await stream.WriteAsync(buffer, cancellationToken);
await stream.ReadAsync(buffer, cancellationToken);
```

## Building

```bash
# Restore packages
dotnet restore

# Build all target frameworks
dotnet build

# Build specific target
dotnet build -f netstandard2.0
dotnet build -f net8.0
dotnet build -f net9.0
```

## License

See LICENSE file for details.

<ItemGroup Condition="'$(TargetFramework)' == 'netstandard2.0'">
  <PackageReference Include="PolySharp" Version="1.15.0" PrivateAssets="all" />
</ItemGroup>
