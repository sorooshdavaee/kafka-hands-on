using Order.Application;
using Order.Application.Abstractions;
using Order.Application.Orders.Commands.PlaceOrder;
using Order.Application.Orders.Queries.GetOrderById;
using Order.Infrastructure;
using Order.Infrastructure.Persistence;
using Microsoft.EntityFrameworkCore;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddApplication();
builder.Services.AddInfrastructure(builder.Configuration);
builder.Services.AddOpenApi();

var app = builder.Build();

using (var scope = app.Services.CreateScope())
{
    var db = scope.ServiceProvider.GetRequiredService<AppDbContext>();
    await db.Database.EnsureCreatedAsync();
}

if (app.Environment.IsDevelopment())
    app.MapOpenApi();

app.MapGet("/", () => Results.Ok(new
{
    service = "Order.Api",
    mode = "Transactional Outbox → Debezium CDC → Kafka",
    endpoints = new[] { "POST /orders", "GET /orders/{id}", "GET /health" }
}));

app.MapGet("/health", () => Results.Ok(new { status = "healthy" }));

app.MapPost("/orders", async (PlaceOrderRequest request, IDispatcher dispatcher, CancellationToken ct) =>
{
    try
    {
        var result = await dispatcher.SendAsync<PlaceOrderCommand, PlaceOrderResult>(
            new PlaceOrderCommand(
                request.CustomerId,
                request.CustomerName,
                request.ProductId,
                request.Quantity,
                request.Amount,
                request.Discount),
            ct);

        return Results.Accepted($"/orders/{result.OrderId}", result);
    }
    catch (ArgumentException ex)
    {
        return Results.BadRequest(new { error = ex.Message });
    }
});

app.MapGet("/orders/{id:guid}", async (Guid id, IDispatcher dispatcher, CancellationToken ct) =>
{
    var dto = await dispatcher.QueryAsync<GetOrderByIdQuery, OrderDto?>(new GetOrderByIdQuery(id), ct);
    return dto is null ? Results.NotFound() : Results.Ok(dto);
});

app.Run();

public sealed record PlaceOrderRequest(
    string CustomerId,
    string? CustomerName,
    string ProductId,
    int Quantity = 1,
    decimal Amount = 0,
    decimal Discount = 0);
