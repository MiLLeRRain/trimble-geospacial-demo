using System.Security.Cryptography;
using System.Text;
using Microsoft.Extensions.Options;
using Trimble.Geospatial.Demo.Functions.Options;

namespace Trimble.Geospatial.Demo.Functions;

public sealed class WebhookSignatureValidator
{
    private readonly WebhookOptions _options;

    public WebhookSignatureValidator(IOptions<WebhookOptions> options)
    {
        _options = options.Value;
    }

    public bool IsValidSignature(string? providedSignature, byte[] bodyBytes)
    {
        if (string.IsNullOrWhiteSpace(providedSignature))
        {
            return false;
        }

        if (string.IsNullOrWhiteSpace(_options.Secret))
        {
            return false;
        }

        var normalized = NormalizeSignature(providedSignature);
        var expected = ComputeSignature(bodyBytes, _options.Secret);

        return CryptographicOperations.FixedTimeEquals(
            Encoding.UTF8.GetBytes(normalized),
            Encoding.UTF8.GetBytes(expected));
    }

    private static string ComputeSignature(byte[] bodyBytes, string secret)
    {
        using var hmac = new HMACSHA256(Encoding.UTF8.GetBytes(secret));
        var hash = hmac.ComputeHash(bodyBytes);
        return Convert.ToHexString(hash).ToLowerInvariant();
    }

    private static string NormalizeSignature(string signature)
    {
        var trimmed = signature.Trim();
        if (trimmed.StartsWith("sha256=", StringComparison.OrdinalIgnoreCase))
        {
            trimmed = trimmed["sha256=".Length..];
        }

        return trimmed.ToLowerInvariant();
    }
}
