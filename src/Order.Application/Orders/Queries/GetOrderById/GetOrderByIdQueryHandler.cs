using Order.Application.Abstractions;

namespace Order.Application.Orders.Queries.GetOrderById;

public sealed class GetOrderByIdQueryHandler(IOrderRepository orders)
    : IQueryHandler<GetOrderByIdQuery, OrderDto?>
{
    public async Task<OrderDto?> HandleAsync(GetOrderByIdQuery query, CancellationToken ct = default)
    {
        var order = await orders.GetByIdAsync(query.OrderId, ct);
        if (order is null) return null;

        var line = order.Lines.FirstOrDefault();
        return new OrderDto(
            order.Id,
            order.CustomerId,
            order.CustomerName,
            order.Status.ToString(),
            line?.ProductId ?? string.Empty,
            line?.Quantity ?? 0,
            order.TotalAmount,
            order.Discount,
            order.CreatedAt);
    }
}
