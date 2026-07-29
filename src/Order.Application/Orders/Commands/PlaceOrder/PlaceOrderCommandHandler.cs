using Order.Application.Abstractions;
using Order.Domain.Orders;

namespace Order.Application.Orders.Commands.PlaceOrder;

public sealed class PlaceOrderCommandHandler(
    IOrderRepository orders,
    IUnitOfWork uow) : ICommandHandler<PlaceOrderCommand, PlaceOrderResult>
{
    public async Task<PlaceOrderResult> HandleAsync(PlaceOrderCommand command, CancellationToken ct = default)
    {
        if (string.IsNullOrWhiteSpace(command.CustomerId))
            throw new ArgumentException("CustomerId is required.");
        if (string.IsNullOrWhiteSpace(command.ProductId))
            throw new ArgumentException("ProductId is required.");

        var order = Order.Domain.Orders.Order.Place(
            command.CustomerId,
            command.CustomerName ?? command.CustomerId,
            command.ProductId,
            command.Quantity,
            command.Amount,
            command.Discount);

        await orders.AddAsync(order, ct);
        // Domain events → outbox_messages in the SAME SaveChanges transaction (interceptor)
        await uow.SaveChangesAsync(ct);

        return new PlaceOrderResult(order.Id, order.CustomerId, order.TotalAmount, order.CreatedAt);
    }
}
