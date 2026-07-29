using Microsoft.Extensions.DependencyInjection;
using Order.Application.Abstractions;
using Order.Application.Orders.Commands.PlaceOrder;
using Order.Application.Orders.Queries.GetOrderById;

namespace Order.Application;

public static class DependencyInjection
{
    public static IServiceCollection AddApplication(this IServiceCollection services)
    {
        services.AddScoped<IDispatcher, Dispatcher>();
        services.AddScoped<ICommandHandler<PlaceOrderCommand, PlaceOrderResult>, PlaceOrderCommandHandler>();
        services.AddScoped<IQueryHandler<GetOrderByIdQuery, OrderDto?>, GetOrderByIdQueryHandler>();
        return services;
    }
}
