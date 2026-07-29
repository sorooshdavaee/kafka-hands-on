using Order.Domain.Common;
using Order.Domain.Orders.Events;

namespace Order.Domain.Orders;

public sealed class Order : AggregateRoot
{
    public string CustomerId { get; private set; } = string.Empty;
    public string CustomerName { get; private set; } = string.Empty;
    public OrderStatus Status { get; private set; }
    public decimal Discount { get; private set; }
    public DateTimeOffset CreatedAt { get; private set; }

    private readonly List<OrderLine> _lines = [];
    public IReadOnlyCollection<OrderLine> Lines => _lines.AsReadOnly();

    public decimal TotalAmount => Math.Max(0, _lines.Sum(l => l.LineTotal) - Discount);

    private Order() { }

    public static Order Place(
        string customerId,
        string customerName,
        string productId,
        int quantity,
        decimal amount,
        decimal discount = 0)
    {
        if (string.IsNullOrWhiteSpace(customerId))
            throw new ArgumentException("CustomerId is required (Kafka partition key).", nameof(customerId));

        var order = new Order
        {
            Id = Guid.NewGuid(),
            CustomerId = customerId.Trim(),
            CustomerName = string.IsNullOrWhiteSpace(customerName) ? customerId.Trim() : customerName.Trim(),
            Status = OrderStatus.Placed,
            Discount = Math.Max(0, discount),
            CreatedAt = DateTimeOffset.UtcNow
        };

        order._lines.Add(new OrderLine(productId, quantity <= 0 ? 1 : quantity, amount));
        order.AddDomainEvent(new OrderPlacedDomainEvent(
            order.Id,
            order.CustomerId,
            productId.Trim(),
            order._lines[0].Quantity,
            order.TotalAmount,
            order.Discount,
            order.CreatedAt));

        return order;
    }

    public void Cancel(string reason)
    {
        if (Status == OrderStatus.Cancelled) return;
        Status = OrderStatus.Cancelled;
        AddDomainEvent(new OrderCancelledDomainEvent(Id, CustomerId, reason, DateTimeOffset.UtcNow));
    }
}
