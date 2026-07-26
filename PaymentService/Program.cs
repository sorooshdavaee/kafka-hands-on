using Confluent.Kafka;
using Shared;
using Shared.Kafka;
using Shared.Models;

// Step 3 + 5: payment-group + Exactly-once (Begin/SendOffsets/CommitTransaction)
// Env:
//   SIMULATE_FAILURE=true
//   PROCESS_DELAY_MS=500
//   PARTITION_ASSIGNMENT=Range|CooperativeSticky

var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); };

var bootstrap = Environment.GetEnvironmentVariable("KAFKA_BOOTSTRAP") ?? KafkaDefaults.BootstrapServers;
var simulateFailure = string.Equals(
    Environment.GetEnvironmentVariable("SIMULATE_FAILURE"), "true", StringComparison.OrdinalIgnoreCase);
var delayMs = int.TryParse(Environment.GetEnvironmentVariable("PROCESS_DELAY_MS"), out var d) ? d : 0;
var assignor = Environment.GetEnvironmentVariable("PARTITION_ASSIGNMENT") ?? "CooperativeSticky";
var strategy = assignor.Equals("Range", StringComparison.OrdinalIgnoreCase)
    ? PartitionAssignmentStrategy.Range
    : PartitionAssignmentStrategy.CooperativeSticky;

var transactionalId = $"payment-tx-{Guid.NewGuid():N}";

var producerConfig = new ProducerConfig
{
    BootstrapServers = bootstrap,
    TransactionalId = transactionalId,
    EnableIdempotence = true,
    Acks = Acks.All
};

using var resultProducer = new ProducerBuilder<string, PaymentResultEvent>(producerConfig)
    .SetKeySerializer(Serializers.Utf8)
    .SetValueSerializer(new JsonSerializer<PaymentResultEvent>())
    .Build();

resultProducer.InitTransactions(TimeSpan.FromSeconds(30));
Console.WriteLine($"[PaymentService] TransactionalId={transactionalId}");

var consumerConfig = new ConsumerConfig
{
    BootstrapServers = bootstrap,
    GroupId = ConsumerGroups.Payment,
    ClientId = $"PaymentService-{Environment.ProcessId}",
    EnableAutoCommit = false,
    AutoOffsetReset = AutoOffsetReset.Earliest,
    PartitionAssignmentStrategy = strategy,
    // Required so isolation.level can read only committed transactional records if needed
    IsolationLevel = IsolationLevel.ReadCommitted
};

using var consumer = new ConsumerBuilder<string, OrderCreatedEvent>(consumerConfig)
    .SetKeyDeserializer(Deserializers.Utf8)
    .SetValueDeserializer(new JsonDeserializer<OrderCreatedEvent>())
    .SetPartitionsAssignedHandler((_, partitions) =>
    {
        Console.ForegroundColor = ConsoleColor.Cyan;
        Console.WriteLine($"[PaymentService] Assigned: {string.Join(", ", partitions.Select(p => p.Partition.Value))}");
        Console.ResetColor();
    })
    .SetPartitionsRevokedHandler((_, partitions) =>
    {
        Console.ForegroundColor = ConsoleColor.Yellow;
        Console.WriteLine($"[PaymentService] Revoked: {string.Join(", ", partitions.Select(p => p.Partition.Value))}");
        Console.ResetColor();
    })
    .Build();

consumer.Subscribe(KafkaTopics.Orders);
Console.WriteLine($"[PaymentService] Listening on '{KafkaTopics.Orders}' group='{ConsumerGroups.Payment}' assignor={strategy}");

try
{
    while (!cts.IsCancellationRequested)
    {
        try
        {
            var cr = consumer.Consume(cts.Token);
            if (cr?.Message?.Value is null) continue;

            var order = cr.Message.Value;
            Console.WriteLine(
                $"[PaymentService] OrderId={order.OrderId} Customer={order.CustomerId} p={cr.Partition.Value} off={cr.Offset.Value}");

            if (delayMs > 0)
                await Task.Delay(delayMs, cts.Token);

            try
            {
                resultProducer.BeginTransaction();

                var payment = new PaymentResultEvent
                {
                    OrderId = order.OrderId,
                    CustomerId = order.CustomerId,
                    Status = "Paid",
                    Amount = order.Amount - order.Discount,
                    ProcessedAt = DateTimeOffset.UtcNow
                };

                await resultProducer.ProduceAsync(
                    KafkaTopics.PaymentResults,
                    new Message<string, PaymentResultEvent>
                    {
                        Key = order.CustomerId,
                        Value = payment
                    },
                    cts.Token);

                if (simulateFailure)
                    throw new InvalidOperationException("Simulated failure mid-transaction.");

                // Atomic: payment-results produce + orders offset commit
                resultProducer.SendOffsetsToTransaction(
                    new[] { new TopicPartitionOffset(cr.TopicPartition, cr.Offset + 1) },
                    consumer.ConsumerGroupMetadata,
                    TimeSpan.FromSeconds(10));

                resultProducer.CommitTransaction(TimeSpan.FromSeconds(30));

                Console.ForegroundColor = ConsoleColor.Green;
                Console.WriteLine($"[PaymentService] TX committed for {order.OrderId}");
                Console.ResetColor();
            }
            catch (Exception ex)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine($"[PaymentService] Aborting TX: {ex.Message}");
                Console.ResetColor();
                try { resultProducer.AbortTransaction(TimeSpan.FromSeconds(30)); }
                catch (KafkaException abortEx)
                {
                    Console.WriteLine($"[PaymentService] Abort warning: {abortEx.Message}");
                }
                // Offset NOT committed → message will be redelivered (at-least-once / EOS abort)
            }
        }
        catch (ConsumeException ex)
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine($"[PaymentService] Consume error: {ex.Error.Reason}");
            Console.ResetColor();
        }
    }
}
catch (OperationCanceledException)
{
    // shutdown
}
finally
{
    consumer.Close();
    Console.WriteLine("[PaymentService] Stopped.");
}
