namespace Order.Application.Orders.Commands.PlaceOrder;

public sealed record PlaceOrderCommand(
    string CustomerId,
    string? CustomerName,
    string ProductId,
    int Quantity,
    decimal Amount,
    decimal Discount);

public sealed record PlaceOrderResult(
    Guid OrderId,
    string CustomerId,
    decimal TotalAmount,
    DateTimeOffset CreatedAt);
