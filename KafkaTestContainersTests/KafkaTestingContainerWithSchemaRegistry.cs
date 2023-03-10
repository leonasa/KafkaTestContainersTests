using System.Globalization;
using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using Xunit.Abstractions;

namespace KafkaTestContainersTests;

public class KafkaTestingContainerWithSchemaRegistry
{
    private readonly ITestOutputHelper _testOutputHelper;

    public KafkaTestingContainerWithSchemaRegistry(ITestOutputHelper testOutputHelper)
    {
        _testOutputHelper = testOutputHelper;
    }

    private const string ZookeeperPort = "2181";

    [Fact]
    public async Task CanIRun_Kafka_InAGenericTestcontainer_WithSchemaRegistry()
    {
        var networkName = Guid.NewGuid().ToString();
        var testcontainersNetworkBuilder = new TestcontainersNetworkBuilder()
            .WithName(networkName);

        var network = testcontainersNetworkBuilder.Build();

        _testOutputHelper.WriteLine("Starting network");
        await network.CreateAsync();
        _testOutputHelper.WriteLine("Network started");

        var zookeeper = new TestcontainersBuilder<TestcontainersContainer>()
            .WithImage("confluentinc/cp-zookeeper:latest")
            .WithName("zookeeper")
            .WithExposedPort(2181)
            .WithNetworkAliases("zookeeper")
            .WithNetwork(network)
            .WithEnvironment("ZOOKEEPER_CLIENT_PORT", ZookeeperPort.ToString(CultureInfo.InvariantCulture))
            .Build();

        await zookeeper.StartAsync();

        var kafka = new TestcontainersBuilder<TestcontainersContainer>()
            .WithImage("confluentinc/cp-kafka:latest")
            .WithName("broker2")
            .WithPortBinding(9092)
            .WithNetworkAliases("broker2")
            .WithNetwork(network)
            .WithEnvironment("KAFKA_BROKER_ID", 1.ToString(CultureInfo.InvariantCulture))
            .WithEnvironment("KAFKA_ZOOKEEPER_CONNECT", "zookeeper:2181")
            .WithEnvironment("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT")
            .WithEnvironment("KAFKA_INTER_BROKER_LISTENER_NAME", "PLAINTEXT")
            .WithEnvironment("KAFKA_ADVERTISED_LISTENERS", "PLAINTEXT://broker2:29092,PLAINTEXT_HOST://localhost:9092")
            .WithEnvironment("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "true")
            .WithEnvironment("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", 1.ToString(CultureInfo.InvariantCulture))
            .WithEnvironment("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", 1.ToString(CultureInfo.InvariantCulture))
            .WithEnvironment("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", 1.ToString(CultureInfo.InvariantCulture))
            .WithEnvironment("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", 100.ToString(CultureInfo.InvariantCulture))
            .Build();

        await kafka.StartAsync();

        var schemaRegistryContainer = new TestcontainersBuilder<TestcontainersContainer>()
            .WithImage("confluentinc/cp-schema-registry:latest")
            .WithName("schema-registry")
            .WithExposedPort(8081)
            .WithPortBinding(8081, true)
            .WithNetworkAliases("schema-registry")
            .WithWaitStrategy(Wait.ForUnixContainer().UntilPortIsAvailable(8081))
            .WithNetwork(network)
            .WithEnvironment("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "broker2:29092")
            .WithEnvironment("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
            .WithEnvironment("SCHEMA_REGISTRY_HOST_LISTENERS", "http://0.0.0.0:8081")
            .Build();

       
        await schemaRegistryContainer.StartAsync();

        _testOutputHelper.WriteLine("schemaRegistry started");

        SchemaRegistryConfig schemaRegistryConfig = new SchemaRegistryConfig
        {
            Url = $"{schemaRegistryContainer.Hostname}:{schemaRegistryContainer.GetMappedPublicPort(8081)}",
        };
        
        using var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig);
        ProducerConfig producerConfig = new ProducerConfig
        {
            BootstrapServers = $"{kafka.Hostname}:{kafka.GetMappedPublicPort(9092)}",
        };
        
        using var producer = new ProducerBuilder<string, UserTest>(producerConfig)
            .SetValueSerializer(new JsonSerializer<UserTest>(schemaRegistry).AsSyncOverAsync())
            .Build();

        _testOutputHelper.WriteLine("producer built");
        
        var deliveryResult = await producer.ProduceAsync("test", new Message<string, UserTest>
        {
            Key = "test", Value = new UserTest {Name = "test", Id = 1 }
        });
        producer.Flush(TimeSpan.FromSeconds(1));

        Assert.Equal(0, deliveryResult.Offset);
        _testOutputHelper.WriteLine($"deliveryResult.Offset: {deliveryResult.Offset}");
    }
}

public class UserTest
{
    public int Id { get; set; }
    public string Name { get; set; }
}