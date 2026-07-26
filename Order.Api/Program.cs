using Order.Api.Services;
using Shared.Models;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddSingleton<OrderProducer>();
builder.Services.AddOpenApi();

var app = builder.Build();

if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

app.MapGet("/", () => Results.Ok(new
{
    service = "Order.Api",
    endpoints = new[] { "POST /orders", "GET /health" }
}));

app.MapGet("/health", () => Results.Ok(new { status = "healthy" }));

app.MapPost("/orders", async (CreateOrderRequest request, OrderProducer producer, CancellationToken ct) =>
{
    if (string.IsNullOrWhiteSpace(request.CustomerId))
        return Results.BadRequest(new { error = "CustomerId is required (used as Kafka partition key)." });

    if (string.IsNullOrWhiteSpace(request.ProductId))
        return Results.BadRequest(new { error = "ProductId is required." });

    var order = new OrderCreatedEvent
    {
        OrderId = Guid.NewGuid(),
        CustomerId = request.CustomerId.Trim(),
        ProductId = request.ProductId.Trim(),
        Quantity = request.Quantity <= 0 ? 1 : request.Quantity,
        Amount = request.Amount,
        Discount = request.Discount,
        CreatedAt = DateTimeOffset.UtcNow
    };

    var delivery = await producer.ProduceOrderAsync(order, ct);

    return Results.Accepted($"/orders/{order.OrderId}", new
    {
        order.OrderId,
        order.CustomerId,
        order.ProductId,
        order.Quantity,
        order.Amount,
        order.Discount,
        partition = delivery.Partition.Value,
        offset = delivery.Offset.Value,
        topic = delivery.Topic
    });
});

app.Run();
