using Confluent.Kafka;
using Shared;
using Shared.Kafka;
using Shared.Models;

namespace Order.Api.Services;

public sealed class OrderProducer : IDisposable
{
    private readonly IProducer<string, OrderCreatedEvent> _producer;
    private readonly ILogger<OrderProducer> _logger;
    private readonly IProducer<string, string> _statusProducer;

    public OrderProducer(IConfiguration configuration, ILogger<OrderProducer> logger)
    {
        _logger = logger;
        var bootstrap = configuration["Kafka:BootstrapServers"] ?? KafkaDefaults.BootstrapServers;
        var acks = configuration["Kafka:Acks"] ?? "all"; // Step 2: acks=all vs acks=1

        var config = new ProducerConfig
        {
            BootstrapServers = bootstrap,
            Acks = ParseAcks(acks),
            EnableIdempotence = true, // Step 2: idempotent producer
            MessageSendMaxRetries = 5,
            LingerMs = 5,
            CompressionType = CompressionType.Snappy
        };

        // Idempotence requires acks=all and max.in.flight <= 5 (library enforces when enabled)
        _producer = new ProducerBuilder<string, OrderCreatedEvent>(config)
            .SetKeySerializer(Serializers.Utf8)
            .SetValueSerializer(new JsonSerializer<OrderCreatedEvent>())
            .SetErrorHandler((_, e) => _logger.LogError("Kafka producer error: {Reason}", e.Reason))
            .Build();

        _statusProducer = new ProducerBuilder<string, string>(config)
            .SetKeySerializer(Serializers.Utf8)
            .SetValueSerializer(Serializers.Utf8)
            .Build();
    }

    public async Task<DeliveryResult<string, OrderCreatedEvent>> ProduceOrderAsync(
        OrderCreatedEvent order,
        CancellationToken cancellationToken = default)
    {
        // Partition key = CustomerId → ordering guaranteed per customer, not globally
        var message = new Message<string, OrderCreatedEvent>
        {
            Key = order.CustomerId,
            Value = order,
            Headers = new Headers
            {
                { "event-type", System.Text.Encoding.UTF8.GetBytes("OrderCreated") }
            }
        };

        var result = await _producer.ProduceAsync(KafkaTopics.Orders, message, cancellationToken);
        _logger.LogInformation(
            "Produced OrderId={OrderId} CustomerId={CustomerId} → partition={Partition} offset={Offset}",
            order.OrderId, order.CustomerId, result.Partition.Value, result.Offset.Value);

        // Step 7 helper: also mirror latest status into compacted topic
        await _statusProducer.ProduceAsync(
            KafkaTopics.CustomerLatestStatus,
            new Message<string, string>
            {
                Key = order.CustomerId,
                Value = $"OrderCreated:{order.OrderId}:{order.CreatedAt:O}"
            },
            cancellationToken);

        return result;
    }

    private static Acks ParseAcks(string value) => value.Trim().ToLowerInvariant() switch
    {
        "0" or "none" => Acks.None,
        "1" or "leader" => Acks.Leader,
        _ => Acks.All
    };

    public void Dispose()
    {
        _producer.Flush(TimeSpan.FromSeconds(5));
        _producer.Dispose();
        _statusProducer.Flush(TimeSpan.FromSeconds(5));
        _statusProducer.Dispose();
    }
}
