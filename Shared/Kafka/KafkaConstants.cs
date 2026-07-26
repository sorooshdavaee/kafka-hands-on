namespace Shared;

public static class KafkaTopics
{
    public const string Orders = "orders";
    public const string PaymentResults = "payment-results";
    public const string CustomerLatestStatus = "customer-latest-status";
}

public static class ConsumerGroups
{
    public const string Payment = "payment-group";
    public const string Notification = "notification-group";
    public const string Inventory = "inventory-group";
}

public static class KafkaDefaults
{
    public const string BootstrapServers = "localhost:29092,localhost:39092,localhost:49092";
    public const string SchemaRegistryUrl = "http://localhost:8081";
}
