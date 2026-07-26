using Shared;
using Shared.Kafka;

// Step 3: independent consumer group — notification-group
// Env: PROCESS_DELAY_MS, PARTITION_ASSIGNMENT, COMMIT_BEFORE_PROCESS

var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); };

await OrderConsumerHost.RunAsync(
    serviceName: "NotificationService",
    groupId: ConsumerGroups.Notification,
    onMessage: (order, cr, _) =>
    {
        Console.ForegroundColor = ConsoleColor.Magenta;
        Console.WriteLine(
            $"[NotificationService] Notify customer={order.CustomerId} order={order.OrderId} " +
            $"amount={order.Amount:C} p={cr.Partition.Value} off={cr.Offset.Value}");
        Console.ResetColor();
        return Task.CompletedTask;
    },
    cancellationToken: cts.Token);
