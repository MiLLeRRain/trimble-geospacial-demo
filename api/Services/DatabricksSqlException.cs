using System.Net;

namespace Trimble.Geospatial.Api.Services;

public sealed class DatabricksSqlException : Exception
{
    public DatabricksSqlException(string message, HttpStatusCode statusCode, string? errorCode = null, bool isTransient = false) : base(message)
    {
        StatusCode = statusCode;
        ErrorCode = errorCode;
        IsTransient = isTransient;
    }

    public HttpStatusCode StatusCode { get; }
    public string? ErrorCode { get; }
    public bool IsTransient { get; }
}
