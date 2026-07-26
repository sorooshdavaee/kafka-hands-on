using Shared;
using Shared.Kafka;

// Step 3: independent consumer group — inventory-group
// Env: PROCESS_DELAY_MS, PARTITION_ASSIGNMENT, COMMIT_BEFORE_PROCESS

var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); };

await OrderConsumerHost.RunAsync(
    serviceName: "InventoryService",
    groupId: ConsumerGroups.Inventory,
    onMessage: (order, cr, _) =>
    {
        Console.ForegroundColor = ConsoleColor.Blue;
        Console.WriteLine(
            $"[InventoryService] Reserve product={order.ProductId} qty={order.Quantity} " +
            $"order={order.OrderId} p={cr.Partition.Value} off={cr.Offset.Value}");
        Console.ResetColor();
        return Task.CompletedTask;
    },
    cancellationToken: cts.Token);
