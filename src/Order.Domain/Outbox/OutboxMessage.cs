using System.Text.Json;
using Order.Domain.Common;

namespace Order.Domain.Outbox;

/// <summary>
/// Same DbContext / same transaction as Order — core of the Outbox Pattern.
/// Column names align with Debezium Outbox Event Router.
/// </summary>
public sealed class OutboxMessage
{
    public Guid Id { get; private set; }
    public string AggregateType { get; private set; } = string.Empty;
    public string AggregateId { get; private set; } = string.Empty;
    public string Type { get; private set; } = string.Empty;
    public string Payload { get; private set; } = string.Empty;
    public DateTimeOffset OccurredOn { get; private set; }

    private OutboxMessage() { }

    public static OutboxMessage FromDomainEvent(IDomainEvent domainEvent, string aggregateType, string aggregateId)
    {
        // Payload shape matches Shared.Models.OrderCreatedEvent so Phase-1 consumers keep working
        // after Debezium EventRouter unwraps the outbox row onto the `orders` topic.
        object payloadBody = domainEvent switch
        {
            Orders.Events.OrderPlacedDomainEvent e => new
            {
                orderId = e.OrderId,
                customerId = e.CustomerId,
                productId = e.ProductId,
                quantity = e.Quantity,
                amount = e.Amount,
                discount = e.Discount,
                createdAt = e.CreatedAt
            },
            Orders.Events.OrderCancelledDomainEvent e => new
            {
                orderId = e.OrderId,
                customerId = e.CustomerId,
                reason = e.Reason,
                cancelledAt = e.CancelledAt
            },
            _ => domainEvent
        };

        return new OutboxMessage
        {
            Id = domainEvent.EventId,
            AggregateType = aggregateType,
            AggregateId = aggregateId,
            Type = domainEvent.EventType,
            Payload = JsonSerializer.Serialize(payloadBody),
            OccurredOn = domainEvent.OccurredOn
        };
    }
}
