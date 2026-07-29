using Order.Domain.Common;

namespace Order.Domain.Orders.Events;

public sealed record OrderPlacedDomainEvent(
    Guid OrderId,
    string CustomerId,
    string ProductId,
    int Quantity,
    decimal Amount,
    decimal Discount,
    DateTimeOffset CreatedAt) : IDomainEvent
{
    public Guid EventId { get; } = Guid.NewGuid();
    public DateTimeOffset OccurredOn { get; } = DateTimeOffset.UtcNow;
    public string EventType => "OrderPlaced";
}

public sealed record OrderCancelledDomainEvent(
    Guid OrderId,
    string CustomerId,
    string Reason,
    DateTimeOffset CancelledAt) : IDomainEvent
{
    public Guid EventId { get; } = Guid.NewGuid();
    public DateTimeOffset OccurredOn { get; } = DateTimeOffset.UtcNow;
    public string EventType => "OrderCancelled";
}
