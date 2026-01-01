using System.ComponentModel.DataAnnotations;

namespace Trimble.Geospatial.Api.Models;

public sealed class ApiError
{
    [Required]
    public string ErrorCode { get; init; } = string.Empty;

    [Required]
    public string Message { get; init; } = string.Empty;

    [Required]
    public string CorrelationId { get; init; } = string.Empty;

    public static ApiError From(string errorCode, string message, string correlationId)
    {
        return new ApiError
        {
            ErrorCode = errorCode,
            Message = message,
            CorrelationId = correlationId
        };
    }
}
