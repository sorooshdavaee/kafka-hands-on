namespace Shared.Models;

/// <summary>
/// Order event published to the <c>orders</c> topic.
/// Partition key = CustomerId (per-customer ordering, not global).
/// Discount is optional for Step 6 schema-evolution demos (default = 0).
/// </summary>
public sealed class OrderCreatedEvent
{
    public Guid OrderId { get; set; } = Guid.NewGuid();
    public string CustomerId { get; set; } = string.Empty;
    public string ProductId { get; set; } = string.Empty;
    public int Quantity { get; set; }
    public decimal Amount { get; set; }
    public decimal Discount { get; set; }
    public DateTimeOffset CreatedAt { get; set; } = DateTimeOffset.UtcNow;
}

public sealed class CreateOrderRequest
{
    public string CustomerId { get; set; } = string.Empty;
    public string ProductId { get; set; } = string.Empty;
    public int Quantity { get; set; } = 1;
    public decimal Amount { get; set; }
    public decimal Discount { get; set; }
}

/// <summary>Written by PaymentService transactional producer (Step 5).</summary>
public sealed class PaymentResultEvent
{
    public Guid OrderId { get; set; }
    public string CustomerId { get; set; } = string.Empty;
    public string Status { get; set; } = "Paid";
    public decimal Amount { get; set; }
    public DateTimeOffset ProcessedAt { get; set; } = DateTimeOffset.UtcNow;
}
