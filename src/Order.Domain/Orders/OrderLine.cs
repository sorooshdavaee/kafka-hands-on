namespace Order.Domain.Orders;

public sealed class OrderLine
{
    public string ProductId { get; private set; } = string.Empty;
    public int Quantity { get; private set; }
    public decimal UnitPrice { get; private set; }

    private OrderLine() { }

    public OrderLine(string productId, int quantity, decimal unitPrice)
    {
        if (string.IsNullOrWhiteSpace(productId)) throw new ArgumentException("ProductId required.", nameof(productId));
        if (quantity <= 0) throw new ArgumentOutOfRangeException(nameof(quantity));
        if (unitPrice < 0) throw new ArgumentOutOfRangeException(nameof(unitPrice));

        ProductId = productId.Trim();
        Quantity = quantity;
        UnitPrice = unitPrice;
    }

    public decimal LineTotal => Quantity * UnitPrice;
}
