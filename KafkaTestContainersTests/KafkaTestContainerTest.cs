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

        var kafkaPort = FreeTcpPort();
        var kafkaTestcontainerConfiguration = new KafkaTestcontainerConfigurationNew(kafkaPort);
        
        var kafka = new TestcontainersBuilder<KafkaTestcontainer>()
            .WithNetwork(network)
            .WithPortBinding(kafkaPort, 9092)
            .WithName("broker")
            .WithNetworkAliases("broker")
            .WithKafka(kafkaTestcontainerConfiguration)
            .Build();
        

        _testOutputHelper.WriteLine($"kafka starting on external port {kafkaPort}");

        await kafka.StartAsync();

        _testOutputHelper.WriteLine("kafka started");

        var schemaRegistryContainer = new TestcontainersBuilder<TestcontainersContainer>()
            .WithImage("confluentinc/cp-schema-registry:latest")
            .WithName("sc-test")
            .WithPortBinding(8081, true)
            .WithNetworkAliases("sc-test")
            .WithNetwork(network)
            .WithWaitStrategy(Wait.ForUnixContainer().UntilPortIsAvailable(8081))
            .WithEnvironment("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "broker:29092")
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

        _testOutputHelper.WriteLine("producer built");
        
        var deliveryResult = await producer.ProduceAsync("test", new Message<string, User>());
        producer.Flush(TimeSpan.FromSeconds(1));

        Assert.Equal(0, deliveryResult.Offset);
        _testOutputHelper.WriteLine($"deliveryResult.Offset: {deliveryResult.Offset}");
    }
}

public record User(string name, int id);

public class KafkaTestcontainerConfigurationNew : KafkaTestcontainerConfiguration
{
    private const int KafkaPort = 9092;
    private const int BrokerPort = 29092;
    private const int ZookeeperPort = 2181;

    private const string StartupScriptPath = "/testcontainers_start.sh";

    
    public KafkaTestcontainerConfigurationNew(int kafkaPort)
    {
        Environments["KAFKA_LISTENER_SECURITY_PROTOCOL_MAP"] = "PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT";
        Environments["KAFKA_ADVERTISED_LISTENERS"] = $"PLAINTEXT://broker:29092,PLAINTEXT_HOST://localhost:{kafkaPort}";
        Environments["KAFKA_INTER_BROKER_LISTENER_NAME"] = "PLAINTEXT";
        Environments.Remove("KAFKA_LISTENERS");
    }

    public override Func<IRunningDockerContainer, CancellationToken, Task> StartupCallback
        => (container, ct) =>
        {
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
            startupScript.Append(". /etc/confluent/docker/bash-config");
            startupScript.Append(lf);
            startupScript.Append("/etc/confluent/docker/run");
            return container.CopyFileAsync(StartupScriptPath, Encoding.Default.GetBytes(startupScript.ToString()),
                0x1ff, ct: ct);
           
        };
}