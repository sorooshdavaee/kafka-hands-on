using Microsoft.Extensions.DependencyInjection;
using Order.Application.Abstractions;

namespace Order.Application;

public sealed class Dispatcher(IServiceProvider sp) : IDispatcher
{
    public Task SendAsync<TCommand>(TCommand command, CancellationToken ct = default)
    {
        var handler = sp.GetRequiredService<ICommandHandler<TCommand>>();
        return handler.HandleAsync(command, ct);
    }

    public Task<TResult> SendAsync<TCommand, TResult>(TCommand command, CancellationToken ct = default)
    {
        var handler = sp.GetRequiredService<ICommandHandler<TCommand, TResult>>();
        return handler.HandleAsync(command, ct);
    }

    public Task<TResult> QueryAsync<TQuery, TResult>(TQuery query, CancellationToken ct = default)
    {
        var handler = sp.GetRequiredService<IQueryHandler<TQuery, TResult>>();
        return handler.HandleAsync(query, ct);
    }
}
