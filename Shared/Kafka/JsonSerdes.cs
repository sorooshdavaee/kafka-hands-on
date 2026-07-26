using System.Text;
using System.Text.Json;
using Confluent.Kafka;

namespace Shared.Kafka;

public static class JsonKafka
{
    public static readonly JsonSerializerOptions Options = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        WriteIndented = false
    };

    public static byte[] Serialize<T>(T value) =>
        Encoding.UTF8.GetBytes(JsonSerializer.Serialize(value, Options));

    public static T? Deserialize<T>(byte[]? data)
    {
        if (data is null || data.Length == 0) return default;
        return JsonSerializer.Deserialize<T>(data, Options);
    }
}

public sealed class JsonSerializer<T> : ISerializer<T>
{
    public byte[] Serialize(T data, SerializationContext context) => JsonKafka.Serialize(data);
}

public sealed class JsonDeserializer<T> : IDeserializer<T>
{
    public T Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
    {
        if (isNull || data.IsEmpty) return default!;
        return JsonSerializer.Deserialize<T>(data, JsonKafka.Options)!;
    }
}
