using System.Globalization;
using System.Text;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Configurations;
using DotNet.Testcontainers.Containers;
using Xunit.Abstractions;

namespace KafkaTestContainersTests;

public class KafkaTestContainerTest
{
    private readonly ITestOutputHelper _testOutputHelper;

    public KafkaTestContainerTest(ITestOutputHelper testOutputHelper)
    {
        _testOutputHelper = testOutputHelper;
    }

    [Fact]
    public async Task Test1()
    {
        var networkName = Guid.NewGuid().ToString();
        var testcontainersNetworkBuilder = new TestcontainersNetworkBuilder()
            // .WithDriver(NetworkDriver.Host)
            .WithName(networkName);
        
        var network = testcontainersNetworkBuilder.Build();

        _testOutputHelper.WriteLine("Starting network");
        // var publicPort = 29092;
        // var BrokerPort = 9092;
        //
        //     const char lf = '\n';
        //     var startupScript = new StringBuilder();
        //     startupScript.Append("#!/bin/sh");
        //     startupScript.Append(lf);
        //     startupScript.Append($"echo 'clientPort={ZookeeperPort}' > zookeeper.properties");
        //     startupScript.Append(lf);
        //     startupScript.Append("echo 'dataDir=/var/lib/zookeeper/data' >> zookeeper.properties");
        //     startupScript.Append(lf);
        //     startupScript.Append("echo 'dataLogDir=/var/lib/zookeeper/log' >> zookeeper.properties");
        //     startupScript.Append(lf);
        //     startupScript.Append("zookeeper-server-start zookeeper.properties &");
        //     startupScript.Append(lf);
        //     startupScript.Append($"export KAFKA_ADVERTISED_LISTENERS='PLAINTEXT://{"kafka"}:{publicPort},PLAINTEXT_HOST://localhost:{BrokerPort}'");
        //     startupScript.Append(lf);
        //     startupScript.Append(". /etc/confluent/docker/bash-config");
        //     startupScript.Append(lf);
        //     startupScript.Append("/etc/confluent/docker/configure");
        //     startupScript.Append(lf);
        //     // startupScript.Append("/etc/confluent/docker/launch");
        //     startupScript.Append("/etc/confluent/docker/run");
        //     var startupScriptString = startupScript.ToString();
        //     var startupScriptBytes = Encoding.UTF8.GetBytes(startupScriptString);
        await network.CreateAsync();
        //
        // var kafka = new TestcontainersBuilder<TestcontainersContainer>()
        //     .WithImage("confluentinc/cp-kafka:latest")
        //     .WithNetwork(network)
        //     .WithPortBinding(9092)
        //     .WithEnvironment("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "BROKER:PLAINTEXT,PLAINTEXT:PLAINTEXT")
        //     .WithEnvironment("KAFKA_ADVERTISED_LISTENERS", "PLAINTEXT://broker:29092,PLAINTEXT_HOST://localhost:9092")
        //   .WithEnvironment("KAFKA_LISTENERS", $"PLAINTEXT://0.0.0.0:29092,BROKER://0.0.0.0:9092")
        //   .WithEnvironment("KAFKA_INTER_BROKER_LISTENER_NAME", "BROKER")
        //   .WithEnvironment("KAFKA_BROKER_ID", "1")
        //   .WithEnvironment("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
        //   .WithEnvironment("KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS", "1")
        //   .WithEnvironment("KAFKA_LOG_FLUSH_INTERVAL_MESSAGES", long.MaxValue.ToString(CultureInfo.InvariantCulture))
        //   .WithEnvironment("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0")
        //   .WithEnvironment("KAFKA_ZOOKEEPER_CONNECT", $"localhost:{ZookeeperPort}")
        //     .WithCommand(startupScriptString)
        //     .Build();


        var kafkaTestcontainerConfiguration = new KafkaTestcontainerConfiguration("confluentinc/cp-kafka:latest");
        kafkaTestcontainerConfiguration.Environments["KAFKA_LISTENERS"] = "PLAINTEXT://127.0.0.1:9092,BROKER://127.0.0.1:9093";
        kafkaTestcontainerConfiguration.Environments.Add("KAFKA_ADVERTISED_LISTENERS", "PLAINTEXT://broker:9092,BROKER://localhost:9093");
        // kafkaTestcontainerConfiguration.Environments["KAFKA_LISTENER_SECURITY_PROTOCOL_MAP"] = "PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT";
        
        var kafka2 = new TestcontainersBuilder<KafkaTestcontainer>()
            .WithNetwork(network)
            .WithName("broker")
            .WithNetworkAliases("broker")
            .WithKafka(kafkaTestcontainerConfiguration)
            .Build();
        
        //export KAFKA_ADVERTISED_LISTENERS='PLAINTEXT://127.0.0.1:49159,BROKER://localhost:9093'
        await kafka2.StartAsync();

        // var testCOntainer = new TestcontainersBuilder<TestcontainersContainer>()
        //     .WithImage("edenhill/kcat:1.7.1")
        //     .WithNetwork(network)
        //     .WithName("kcat")
        //     // .WithEntrypoint("kafkacat")
        //     .WithCommand("kcat -b broker:9092 -L")
        //     .Build();
        
        // await testCOntainer.StartAsync();
            
        
        var schemaRegistry = new TestcontainersBuilder<TestcontainersContainer>()
            .WithImage("confluentinc/cp-schema-registry:latest")
            .WithName("sc-test")
            .WithNetwork(network)
            .WithEnvironment("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", $"127.0.0.1:{kafka2.Port}")
            // .WithEnvironment("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", kafka.BootstrapServers)
            // .WithEnvironment("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "kafka:9093")
            // .WithEnvironment("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "BROKER://kafka:9093")
            // .WithEnvironment("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "BROKER://localhost:9093")
            // .WithEnvironment("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "PLAINTEXT://localhost:9092")
            .WithEnvironment("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
            .WithEnvironment("SCHEMA_REGISTRY_HOST_LISTENERS", "http://0.0.0.0:8081")
            .Build();
        
        await schemaRegistry.StartAsync();
    }

    public string ZookeeperPort => "2181";
}
