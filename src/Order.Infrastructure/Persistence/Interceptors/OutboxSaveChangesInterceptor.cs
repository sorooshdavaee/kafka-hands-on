using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Order.Domain.Common;
using Order.Domain.Outbox;

namespace Order.Infrastructure.Persistence.Interceptors;

/// <summary>
/// Converts AggregateRoot domain events into outbox rows in the same SaveChanges transaction.
/// </summary>
public sealed class OutboxSaveChangesInterceptor : SaveChangesInterceptor
{
    public override InterceptionResult<int> SavingChanges(
        DbContextEventData eventData,
        InterceptionResult<int> result)
    {
        AppendOutbox(eventData.Context);
        return base.SavingChanges(eventData, result);
    }

    public override ValueTask<InterceptionResult<int>> SavingChangesAsync(
        DbContextEventData eventData,
        InterceptionResult<int> result,
        CancellationToken cancellationToken = default)
    {
        AppendOutbox(eventData.Context);
        return base.SavingChangesAsync(eventData, result, cancellationToken);
    }

    private static void AppendOutbox(DbContext? context)
    {
        if (context is null) return;

        var aggregates = context.ChangeTracker
            .Entries<AggregateRoot>()
            .Where(e => e.Entity.DomainEvents.Count > 0)
            .Select(e => e.Entity)
            .ToList();

        foreach (var aggregate in aggregates)
        {
            var aggregateId = aggregate is Domain.Orders.Order order
                ? order.CustomerId
                : aggregate.Id.ToString();

            foreach (var domainEvent in aggregate.DomainEvents)
            {
                context.Set<OutboxMessage>().Add(
                    OutboxMessage.FromDomainEvent(
                        domainEvent,
                        aggregateType: "Order",
                        aggregateId: aggregateId));
            }

            aggregate.ClearDomainEvents();
        }
    }
}
