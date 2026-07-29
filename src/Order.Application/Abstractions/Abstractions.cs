namespace Order.Application.Abstractions;

public interface ICommandHandler<in TCommand>
{
    Task HandleAsync(TCommand command, CancellationToken ct = default);
}

public interface ICommandHandler<in TCommand, TResult>
{
    Task<TResult> HandleAsync(TCommand command, CancellationToken ct = default);
}

public interface IQueryHandler<in TQuery, TResult>
{
    Task<TResult> HandleAsync(TQuery query, CancellationToken ct = default);
}

public interface IUnitOfWork
{
    Task<int> SaveChangesAsync(CancellationToken ct = default);
}

public interface IOrderRepository
{
    Task AddAsync(Domain.Orders.Order order, CancellationToken ct = default);
    Task<Domain.Orders.Order?> GetByIdAsync(Guid id, CancellationToken ct = default);
}

public interface IDateTimeProvider
{
    DateTimeOffset UtcNow { get; }
}

public interface IDispatcher
{
    Task SendAsync<TCommand>(TCommand command, CancellationToken ct = default);
    Task<TResult> SendAsync<TCommand, TResult>(TCommand command, CancellationToken ct = default);
    Task<TResult> QueryAsync<TQuery, TResult>(TQuery query, CancellationToken ct = default);
}
