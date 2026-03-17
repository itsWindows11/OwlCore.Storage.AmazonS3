namespace OwlCore.Storage.AmazonS3;

internal static class ThrowHelper
{
    public static void ThrowIfNull(object? argument, string paramName)
    {
        if (argument is null)
            throw new ArgumentNullException(paramName);
    }

    public static void ThrowIfNegative(int value, string paramName)
    {
        if (value < 0)
            throw new ArgumentOutOfRangeException(paramName, value, "Value must be non-negative.");
    }

    public static void ThrowIfNegative(long value, string paramName)
    {
        if (value < 0)
            throw new ArgumentOutOfRangeException(paramName, value, "Value must be non-negative.");
    }

    public static void ThrowIfDisposed(bool condition, object instance)
    {
        if (condition)
            throw new ObjectDisposedException(instance.GetType().Name);
    }
}
