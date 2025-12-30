using System.Text.Json;
using System.Text.Json.Serialization;

namespace Trimble.Geospatial.Api.Models;

public sealed class DatabricksStatementResponse
{
    [JsonPropertyName("statement_id")]
    public string StatementId { get; init; } = string.Empty;

    [JsonPropertyName("status")]
    public DatabricksStatementStatus Status { get; init; } = new();

    [JsonPropertyName("result")]
    public DatabricksStatementResult? Result { get; init; }
}

public sealed class DatabricksStatementStatus
{
    [JsonPropertyName("state")]
    public string State { get; init; } = string.Empty;

    [JsonPropertyName("error")]
    public DatabricksStatementError? Error { get; init; }
}

public sealed class DatabricksStatementError
{
    [JsonPropertyName("error_code")]
    public string ErrorCode { get; init; } = string.Empty;

    [JsonPropertyName("message")]
    public string Message { get; init; } = string.Empty;
}

public sealed class DatabricksStatementResult
{
    [JsonPropertyName("data_array")]
    public JsonElement DataArray { get; init; }
}
