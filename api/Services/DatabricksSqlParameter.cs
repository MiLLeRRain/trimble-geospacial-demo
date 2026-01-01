using System.Text.Json.Serialization;

namespace Trimble.Geospatial.Api.Services;

public sealed class DatabricksSqlParameter
{
    [JsonPropertyName("name")]
    public string Name { get; init; } = string.Empty;

    [JsonPropertyName("value")]
    public object? Value { get; init; }

    [JsonPropertyName("type")]
    public string Type { get; init; } = "STRING";

    public static DatabricksSqlParameter String(string name, string value)
    {
        return new DatabricksSqlParameter
        {
            Name = name,
            Value = value,
            Type = "STRING"
        };
    }

    public static DatabricksSqlParameter Long(string name, long value)
    {
        return new DatabricksSqlParameter
        {
            Name = name,
            Value = value,
            Type = "LONG"
        };
    }

    public static DatabricksSqlParameter Int(string name, int value)
    {
        return new DatabricksSqlParameter
        {
            Name = name,
            Value = value,
            Type = "INT"
        };
    }

    public static DatabricksSqlParameter Bool(string name, bool value)
    {
        return new DatabricksSqlParameter
        {
            Name = name,
            Value = value,
            Type = "BOOLEAN"
        };
    }

    public static DatabricksSqlParameter Double(string name, double value)
    {
        return new DatabricksSqlParameter
        {
            Name = name,
            Value = value,
            Type = "DOUBLE"
        };
    }

}
