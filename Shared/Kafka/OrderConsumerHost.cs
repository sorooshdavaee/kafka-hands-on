using Confluent.Kafka;

namespace Shared.Kafka;

/// <summary>
/// Shared consumer loop: manual commit after processing (at-least-once).
/// Set <c>COMMIT_BEFORE_PROCESS=true</c> to demo at-most-once.
/// Set <c>PROCESS_DELAY_MS</c> to induce lag (Step 4).
/// Set <c>PARTITION_ASSIGNMENT</c> to Range or CooperativeSticky (Step 3).
/// </summary>
public static class OrderConsumerHost
{
    public static async Task RunAsync(
        string serviceName,
        string groupId,
        Func<Models.OrderCreatedEvent, ConsumeResult<string, Models.OrderCreatedEvent>, CancellationToken, Task> onMessage,
        CancellationToken cancellationToken = default)
    {
        var bootstrap = Environment.GetEnvironmentVariable("KAFKA_BOOTSTRAP") ?? KafkaDefaults.BootstrapServers;
        var assignor = Environment.GetEnvironmentVariable("PARTITION_ASSIGNMENT") ?? "CooperativeSticky";
        var commitBefore = string.Equals(
            Environment.GetEnvironmentVariable("COMMIT_BEFORE_PROCESS"), "true", StringComparison.OrdinalIgnoreCase);
        var delayMs = int.TryParse(Environment.GetEnvironmentVariable("PROCESS_DELAY_MS"), out var d) ? d : 0;

        var strategy = assignor.Equals("Range", StringComparison.OrdinalIgnoreCase)
            ? PartitionAssignmentStrategy.Range
            : PartitionAssignmentStrategy.CooperativeSticky;

        var config = new ConsumerConfig
        {
            BootstrapServers = bootstrap,
            GroupId = groupId,
            ClientId = $"{serviceName}-{Environment.ProcessId}",
            EnableAutoCommit = false,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            PartitionAssignmentStrategy = strategy,
            // Helps observe rebalance in Kafka UI / logs
            SessionTimeoutMs = 10000,
            HeartbeatIntervalMs = 3000
        };

        using var consumer = new ConsumerBuilder<string, Models.OrderCreatedEvent>(config)
            .SetKeyDeserializer(Deserializers.Utf8)
            .SetValueDeserializer(new JsonDeserializer<Models.OrderCreatedEvent>())
            .SetPartitionsAssignedHandler((c, partitions) =>
            {
                Console.ForegroundColor = ConsoleColor.Cyan;
                Console.WriteLine($"[{serviceName}] Partitions assigned: {string.Join(", ", partitions.Select(p => p.Partition.Value))}");
                Console.ResetColor();
            })
            .SetPartitionsRevokedHandler((c, partitions) =>
            {
                Console.ForegroundColor = ConsoleColor.Yellow;
                Console.WriteLine($"[{serviceName}] Partitions revoked: {string.Join(", ", partitions.Select(p => p.Partition.Value))}");
                Console.ResetColor();
            })
            .Build();

        consumer.Subscribe(KafkaTopics.Orders);
        Console.WriteLine($"[{serviceName}] Listening on '{KafkaTopics.Orders}' as group '{groupId}' (assignor={strategy}, delayMs={delayMs}, commitBefore={commitBefore})");

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    var cr = consumer.Consume(cancellationToken);
                    if (cr?.Message?.Value is null) continue;

                    if (commitBefore)
                    {
                        // At-most-once: commit first — crash after this loses the message
                        consumer.Commit(cr);
                    }

                    if (delayMs > 0)
                        await Task.Delay(delayMs, cancellationToken);

                    await onMessage(cr.Message.Value, cr, cancellationToken);

                    if (!commitBefore)
                    {
                        // At-least-once: commit after process — crash before this may redeliver
                        consumer.Commit(cr);
                    }
                }
                catch (ConsumeException ex)
                {
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine($"[{serviceName}] Consume error: {ex.Error.Reason}");
                    Console.ResetColor();
                }
            }
        }
        catch (OperationCanceledException)
        {
            // graceful shutdown
        }
        finally
        {
            consumer.Close();
            Console.WriteLine($"[{serviceName}] Stopped.");
        }
    }
}
