using System.Net.Http.Json;
using Bogus;
using Shared.Models;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddHttpClient("orders", c =>
{
    c.BaseAddress = new Uri(builder.Configuration["OrderApi:BaseUrl"] ?? "http://localhost:5180/");
});
builder.Services.AddHostedService<TrafficGeneratorWorker>();
builder.Services.AddHostedService<ChaosInjectorWorker>();

var host = builder.Build();
await host.RunAsync();

public sealed class TrafficGeneratorWorker(
    IHttpClientFactory httpClientFactory,
    IConfiguration config,
    ILogger<TrafficGeneratorWorker> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var enabled = config.GetValue("Traffic:Enabled", true);
        if (!enabled)
        {
            logger.LogInformation("Traffic generator disabled.");
            return;
        }

        await Task.Delay(3000, stoppingToken); // wait for API

        var faker = new Faker<CreateOrderRequest>()
            .RuleFor(o => o.CustomerId, f => $"cust-{f.Random.Number(1, 50)}")
            .RuleFor(o => o.ProductId, f => $"sku-{f.Random.Number(100, 999)}")
            .RuleFor(o => o.Quantity, f => f.Random.Number(1, 5))
            .RuleFor(o => o.Amount, f => Math.Round(f.Random.Decimal(10, 5000), 2))
            .RuleFor(o => o.Discount, f => Math.Round(f.Random.Decimal(0, 50), 2));

        var client = httpClientFactory.CreateClient("orders");
        logger.LogInformation("Traffic generator started → {Base}", client.BaseAddress);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var order = faker.Generate();
                var response = await client.PostAsJsonAsync("orders", new
                {
                    customerId = order.CustomerId,
                    customerName = order.CustomerId,
                    productId = order.ProductId,
                    quantity = order.Quantity,
                    amount = order.Amount,
                    discount = order.Discount
                }, stoppingToken);

                logger.LogInformation("POST /orders → {Status} customer={Customer}",
                    (int)response.StatusCode, order.CustomerId);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                logger.LogWarning(ex, "Traffic POST failed (API down?)");
            }

            var delay = Random.Shared.Next(
                config.GetValue("Traffic:MinDelayMs", 500),
                config.GetValue("Traffic:MaxDelayMs", 3000));
            await Task.Delay(delay, stoppingToken);
        }
    }
}

/// <summary>
/// Chaos switch: when Chaos:KillApiAfterOrders is set, after N posts log kill instruction.
/// Actual docker kill is left to the operator (or set Chaos:RunDockerKill=true).
/// </summary>
public sealed class ChaosInjectorWorker(
    IConfiguration config,
    ILogger<ChaosInjectorWorker> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var after = config.GetValue("Chaos:KillApiAfterOrders", 0);
        if (after <= 0)
        {
            logger.LogInformation("Chaos injector idle (Chaos:KillApiAfterOrders=0).");
            return;
        }

        logger.LogWarning("Chaos armed: after ~{Count} traffic ticks will suggest killing Order.Api", after);
        await Task.Delay(TimeSpan.FromSeconds(after * 2), stoppingToken);

        logger.LogWarning("""
            CHAOS: Kill Order.Api mid-flight to observe Outbox consistency.
            Example: stop the Order.Api process, then restart it.
            Pending rows stay in outbox_messages; Debezium resumes from WAL — no lost events.
            """);

        if (config.GetValue("Chaos:RunDockerKill", false))
        {
            // Optional: only kills a container named order-api if you dockerize the API later
            logger.LogWarning("Chaos:RunDockerKill=true but Order.Api is typically a local process — kill the console window manually.");
        }
    }
}
