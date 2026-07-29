namespace Order.Application.Orders.Queries.GetOrderById;

public sealed record GetOrderByIdQuery(Guid OrderId);

public sealed record OrderDto(
    Guid OrderId,
    string CustomerId,
    string CustomerName,
    string Status,
    string ProductId,
    int Quantity,
    decimal TotalAmount,
    decimal Discount,
    DateTimeOffset CreatedAt);
