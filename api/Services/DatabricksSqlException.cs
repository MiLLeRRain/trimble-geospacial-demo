using System.Net;

namespace Trimble.Geospatial.Api.Services;

public sealed class DatabricksSqlException : Exception
{
    public DatabricksSqlException(string message, HttpStatusCode statusCode, string? errorCode = null) : base(message)
    {
        StatusCode = statusCode;
        ErrorCode = errorCode;
    }

    public HttpStatusCode StatusCode { get; }
    public string? ErrorCode { get; }
}
