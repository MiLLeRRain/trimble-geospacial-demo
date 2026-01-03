using System.ComponentModel.DataAnnotations;

namespace Trimble.Geospatial.Api.Options;

public sealed class ServiceBusOptions
{
    [Required]
    public string? ConnectionString { get; set; }

    [Required]
    public string? QueueName { get; set; }
}
