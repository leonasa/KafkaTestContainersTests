using System.Globalization;
using System.Net;
using System.Net.Sockets;
using System.Text;
using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Configurations;
using DotNet.Testcontainers.Containers;
using Xunit.Abstractions;


namespace KafkaTestContainersTests;
//
// public class tes<TDockerContainer> : TestcontainersBuilder<TDockerContainer>
//     where TDockerContainer : ITestcontainersContainer
// {
//     public ITestcontainersBuilder<TDockerContainer> WithPortBinding(int port, bool assignRandomHostPort = false, Action<>)
//     {
//         return this.WithPortBinding(port.ToString(CultureInfo.InvariantCulture), assignRandomHostPort);
//     }
// }
public class KafkaTestContainerTest
{
    private readonly ITestOutputHelper _testOutputHelper;

    public KafkaTestContainerTest(ITestOutputHelper testOutputHelper)
    {
        _testOutputHelper = testOutputHelper;
    }
    
    static int FreeTcpPort()
    {
        TcpListener l = new TcpListener(IPAddress.Loopback, 0);
        l.Start();
        int port = ((IPEndPoint)l.LocalEndpoint).Port;
        l.Stop();
        return port;
    }

    [Fact]
    public async Task CanIRun_KafkaTestcontainer_WithSchemaRegistry()
    {
        var networkName = Guid.NewGuid().ToString();
        var testcontainersNetworkBuilder = new TestcontainersNetworkBuilder()
            .WithName(networkName);

        var network = testcontainersNetworkBuilder.Build();

        _testOutputHelper.WriteLine("Starting network");
        await network.CreateAsync();
        _testOutputHelper.WriteLine("Network started");

        var kafkaContainerNetworkAliases = Guid.NewGuid().ToString();
        var kafkaTestcontainerConfiguration = new KafkaTestcontainerConfigurationNew(kafkaContainerNetworkAliases);
        
        var kafka = new TestcontainersBuilder<KafkaTestcontainer>()
            .WithNetwork(network)
            .WithPortBinding(9092)
            .WithNetworkAliases(kafkaContainerNetworkAliases)
            .WithKafka(kafkaTestcontainerConfiguration)
            .Build();
        
        await kafka.StartAsync();

        _testOutputHelper.WriteLine("kafka started");

        var schemaRegistryContainer = new TestcontainersBuilder<TestcontainersContainer>()
            .WithImage("confluentinc/cp-schema-registry:latest")
            .WithPortBinding(8081, true)
            .WithNetwork(network)
            .WithWaitStrategy(Wait.ForUnixContainer().UntilPortIsAvailable(8081))
            .WithEnvironment("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", kafkaTestcontainerConfiguration.KafkaBrokerInternalUrl)
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
            BootstrapServers = kafka.BootstrapServers
        };
        
        using var producer = new ProducerBuilder<string, User>(producerConfig)
            .SetValueSerializer(new JsonSerializer<User>(schemaRegistry).AsSyncOverAsync())
            .Build();

        _testOutputHelper.WriteLine($"producer built with config {producerConfig.BootstrapServers}");
        
        var deliveryResult = await producer.ProduceAsync("test", new Message<string, User>());
        producer.Flush(TimeSpan.FromSeconds(1));

        Assert.Equal(0, deliveryResult.Offset);
        _testOutputHelper.WriteLine($"deliveryResult.Offset: {deliveryResult.Offset}");
    }
}

public record User(string name, int id);

public class KafkaTestcontainerConfigurationNew : KafkaTestcontainerConfiguration
{
    private readonly string _networkAliases;
    private static ushort _mappedPublicPort;
    private const int ZookeeperPort = 2181;
    private const int BrokerPort = 9093;
    private const string StartupScriptPath = "/testcontainers_start.sh";

    public string KafkaBrokerInternalUrl { get; }
    
    public KafkaTestcontainerConfigurationNew(string networkAliases)
        : base("confluentinc/cp-kafka:latest")
    {
        _networkAliases = networkAliases;
        KafkaBrokerInternalUrl = $"{_networkAliases}:{BrokerPort}";
    }
    
    public override Func<IRunningDockerContainer, CancellationToken, Task> StartupCallback
        => (container, ct) =>
        {
            _mappedPublicPort = container.GetMappedPublicPort(9092);

            const char lf = '\n';
            var startupScript = new StringBuilder();
            startupScript.Append("#!/bin/sh");
            startupScript.Append(lf);
            startupScript.Append($"echo 'clientPort={ZookeeperPort}' > zookeeper.properties");
            startupScript.Append(lf);
            startupScript.Append("echo 'dataDir=/var/lib/zookeeper/data' >> zookeeper.properties");
            startupScript.Append(lf);
            startupScript.Append("echo 'dataLogDir=/var/lib/zookeeper/log' >> zookeeper.properties");
            startupScript.Append(lf);
            startupScript.Append("zookeeper-server-start zookeeper.properties &");
            startupScript.Append(lf);                                 
            startupScript.Append($"export KAFKA_ADVERTISED_LISTENERS='PLAINTEXT://localhost:{_mappedPublicPort},BROKER://{KafkaBrokerInternalUrl}'");
            startupScript.Append(lf);
            startupScript.Append(". /etc/confluent/docker/bash-config");
            startupScript.Append(lf);
            startupScript.Append("/etc/confluent/docker/run");
            return container.CopyFileAsync(StartupScriptPath, Encoding.Default.GetBytes(startupScript.ToString()), 0x1ff, ct: ct);
        };
}