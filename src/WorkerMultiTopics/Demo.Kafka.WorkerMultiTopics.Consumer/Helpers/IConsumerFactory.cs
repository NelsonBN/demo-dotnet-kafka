using Confluent.Kafka;

namespace Demo.Kafka.WorkerMultiTopics.Consumer.Helpers;

public interface IConsumerFactory
{
    IConsumer<TKey, TValue> Create<TKey, TValue>(string topic);
    IConsumer<Null, TValue> Create<TValue>(string topic);
}
