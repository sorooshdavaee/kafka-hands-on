using System.Collections.Concurrent;
using System.Text.Json;
using Confluent.Kafka;
using Shared;
using Shared.Kafka;
using Shared.Models;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddSingleton<InMemoryReadModelStore>();
builder.Services.AddHostedService<OrderPlacedConsumer>();
builder.Services.AddHostedService<ReadModelHttpHost>();

var host = builder.Build();
await host.RunAsync();

public sealed class OrderReadModel
{
    public Guid OrderId { get; set; }
    public string CustomerId { get; set; } = "";
    public string ProductId { get; set; } = "";
    public int Quantity { get; set; }
    public decimal Amount { get; set; }
    public decimal Discount { get; set; }
    public string Status { get; set; } = "Placed";
    public DateTimeOffset UpdatedAt { get; set; }
}

public interface IReadModelStore
{
    void Upsert(OrderReadModel model);
    OrderReadModel? Get(Guid orderId);
    IReadOnlyCollection<OrderReadModel> GetAll();
}

public sealed class InMemoryReadModelStore : IReadModelStore
{
    private readonly ConcurrentDictionary<Guid, OrderReadModel> _store = new();

    public void Upsert(OrderReadModel model) => _store[model.OrderId] = model;
    public OrderReadModel? Get(Guid orderId) => _store.TryGetValue(orderId, out var m) ? m : null;
    public IReadOnlyCollection<OrderReadModel> GetAll() => _store.Values.OrderByDescending(x => x.UpdatedAt).ToList();
}

/// <summary>
/// Simulates Kafka Streams KTable: consume orders → local state → produce compacted topic.
/// </summary>
public sealed class OrderPlacedConsumer(
    InMemoryReadModelStore store,
    IConfiguration config,
    ILogger<OrderPlacedConsumer> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var bootstrap = config["Kafka:BootstrapServers"] ?? KafkaDefaults.BootstrapServers;
        var group = config["Kafka:GroupId"] ?? "readmodel-processor";

        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = bootstrap,
            GroupId = group,
            EnableAutoCommit = false,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            ClientId = $"ReadModel-{Environment.ProcessId}"
        };

        var producerConfig = new ProducerConfig
        {
            BootstrapServers = bootstrap,
            Acks = Acks.All,
            EnableIdempotence = true
        };

        using var consumer = new ConsumerBuilder<string, OrderCreatedEvent>(consumerConfig)
            .SetKeyDeserializer(Deserializers.Utf8)
            .SetValueDeserializer(new JsonDeserializer<OrderCreatedEvent>())
            .Build();

        using var producer = new ProducerBuilder<string, string>(producerConfig)
            .SetKeySerializer(Serializers.Utf8)
            .SetValueSerializer(Serializers.Utf8)
            .Build();

        consumer.Subscribe(KafkaTopics.Orders);
        logger.LogInformation("ReadModel listening on {Topic} → compacted {Out}",
            KafkaTopics.Orders, KafkaTopics.OrderReadModel);

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    var cr = consumer.Consume(stoppingToken);
                    if (cr?.Message?.Value is null) continue;

                    var e = cr.Message.Value;
                    var model = new OrderReadModel
                    {
                        OrderId = e.OrderId,
                        CustomerId = e.CustomerId,
                        ProductId = e.ProductId,
                        Quantity = e.Quantity,
                        Amount = e.Amount,
                        Discount = e.Discount,
                        Status = "Placed",
                        UpdatedAt = DateTimeOffset.UtcNow
                    };

                    store.Upsert(model);

                    // KTable-like compacted changelog (key = OrderId)
                    await producer.ProduceAsync(
                        KafkaTopics.OrderReadModel,
                        new Message<string, string>
                        {
                            Key = e.OrderId.ToString(),
                            Value = JsonSerializer.Serialize(model, JsonKafka.Options)
                        },
                        stoppingToken);

                    consumer.Commit(cr);
                    logger.LogInformation("ReadModel upsert OrderId={OrderId} Customer={CustomerId}", e.OrderId, e.CustomerId);
                }
                catch (ConsumeException ex)
                {
                    logger.LogError(ex, "Consume failed");
                }
            }
        }
        catch (OperationCanceledException) { }
        finally
        {
            consumer.Close();
        }
    }
}

/// <summary>Tiny HTTP surface to query the in-memory read model (CQRS query side).</summary>
public sealed class ReadModelHttpHost(
    InMemoryReadModelStore store,
    IConfiguration config,
    ILogger<ReadModelHttpHost> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var port = config.GetValue("HttpPort", 5181);
        using var listener = new System.Net.HttpListener();
        listener.Prefixes.Add($"http://localhost:{port}/");
        listener.Start();
        logger.LogInformation("ReadModel query API on http://localhost:{Port}/orders", port);

        while (!stoppingToken.IsCancellationRequested)
        {
            var ctxTask = listener.GetContextAsync();
            var completed = await Task.WhenAny(ctxTask, Task.Delay(Timeout.Infinite, stoppingToken));
            if (completed != ctxTask) break;

            var ctx = await ctxTask;
            _ = Task.Run(() => Handle(ctx), stoppingToken);
        }

        listener.Stop();
    }

    private void Handle(System.Net.HttpListenerContext ctx)
    {
        try
        {
            var path = ctx.Request.Url?.AbsolutePath ?? "/";
            object body;
            int status = 200;

            if (path.Equals("/orders", StringComparison.OrdinalIgnoreCase))
            {
                body = store.GetAll();
            }
            else if (path.StartsWith("/orders/", StringComparison.OrdinalIgnoreCase)
                     && Guid.TryParse(path["/orders/".Length..], out var id))
            {
                var item = store.Get(id);
                if (item is null) { status = 404; body = new { error = "not found" }; }
                else body = item;
            }
            else if (path is "/" or "/health")
            {
                body = new { service = "ReadModel.Processor", count = store.GetAll().Count };
            }
            else
            {
                status = 404;
                body = new { error = "not found" };
            }

            var json = JsonSerializer.Serialize(body, JsonKafka.Options);
            var bytes = System.Text.Encoding.UTF8.GetBytes(json);
            ctx.Response.StatusCode = status;
            ctx.Response.ContentType = "application/json";
            ctx.Response.OutputStream.Write(bytes);
            ctx.Response.Close();
        }
        catch
        {
            try { ctx.Response.Abort(); } catch { /* ignore */ }
        }
    }
}
