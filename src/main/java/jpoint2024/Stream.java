package jpoint2024;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

public class Stream {
    private final static Logger logger = LoggerFactory.getLogger("main");

    private final AtomicReference<ReadOnlyKeyValueStore<String, String>> topicOneStoreRef;
    private final AtomicReference<ReadOnlyKeyValueStore<String, String>> topicTwoStoreRef;
    private final AtomicReference<ReadOnlyKeyValueStore<String, String>> topicThreeStoreRef;

    private final AtomicReference<KafkaStreams> assignedPartitions;

    private final String topicStoreNameOne = "topic-store-1";
    private final String topicStoreNameTwo = "topic-store-2";
    private final String topicStoreNameThree = "topic-store-3";

    private KafkaStreams kafkaStreams;

    public Stream() {
        this.topicOneStoreRef = new AtomicReference<>();
        this.topicTwoStoreRef = new AtomicReference<>();
        this.topicThreeStoreRef = new AtomicReference<>();
        this.assignedPartitions = new AtomicReference<>();
    }

    public void configure() {
        var builder = new StreamsBuilder();

        var properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "jpoint2024");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);

        var topicOneKeySerde = Serdes.String();
        var topicTwoKeySerde = Serdes.String();
        var topicThreeKeySerde = Serdes.String();

        var topicOneStoreSupplier = Stores.inMemoryKeyValueStore(topicStoreNameOne);
        var topicTwoStoreSupplier = Stores.inMemoryKeyValueStore(topicStoreNameTwo);
        var topicThreeStoreSupplier = Stores.inMemoryKeyValueStore(topicStoreNameThree);

        var topicOneStoreBuilder = Stores.keyValueStoreBuilder(topicOneStoreSupplier, topicOneKeySerde, Serdes.String()).withLoggingEnabled(Map.of()).withCachingDisabled();
        var topicTwoStoreBuilder = Stores.keyValueStoreBuilder(topicTwoStoreSupplier, topicTwoKeySerde, Serdes.String()).withLoggingEnabled(Map.of()).withCachingDisabled();
        var topicThreeStoreBuilder = Stores.keyValueStoreBuilder(topicThreeStoreSupplier, topicThreeKeySerde, Serdes.String()).withLoggingEnabled(Map.of()).withCachingDisabled();

        builder.addStateStore(topicOneStoreBuilder);
        builder.addStateStore(topicTwoStoreBuilder);
        builder.addStateStore(topicThreeStoreBuilder);

        var topicOneKTable = builder.table("topic1", Consumed.with(topicOneKeySerde, Serdes.String()));
        var topicTwoKTable = builder.table("topic2", Consumed.with(topicTwoKeySerde, Serdes.String()));
        var topicThreeKTable = builder.table("topic3", Consumed.with(topicThreeKeySerde, Serdes.String()));

        topicOneKTable.transformValues(() -> createStoreProducer(topicStoreNameOne), topicStoreNameOne).toStream().foreach((key, value) -> {});
        topicTwoKTable.transformValues(() -> createStoreProducer(topicStoreNameTwo), topicStoreNameTwo).toStream().foreach((key, value) -> {});
        topicThreeKTable.transformValues(() -> createStoreProducer(topicStoreNameThree), topicStoreNameThree).toStream().foreach((key, value) -> {});

        var topology = builder.build(properties);
        kafkaStreams = new KafkaStreams(topology, new CustomStreamsConfig(properties));
    }

    private static <K, V> ValueTransformerWithKey<K, V, V> createStoreProducer(String storageName) {
        return new ValueTransformerWithKey<>() {
            private KeyValueStore<K, V> storage;
            private ProcessorContext context;

            @Override
            public void init(ProcessorContext context) {
                this.storage = (KeyValueStore<K, V>) context.getStateStore(storageName);
                this.context = context;
            }

            @Override
            public V transform(K readOnlyKey, V value) {
                storage.put(readOnlyKey, value);

                return value;
            }

            @Override
            public void close() {
            }
        };
    }

    public void start() {
        kafkaStreams.setUncaughtExceptionHandler((t, e) -> logger.error("Error: {}", e.getLocalizedMessage()));

        kafkaStreams.setStateListener((newState, oldState) -> {
            logger.info("Kafka streams has changed state from {} to {}", oldState.name(), newState.name());

            if (newState == KafkaStreams.State.RUNNING) {
                ReadOnlyKeyValueStore<String, String> topicOneStore = kafkaStreams.store(StoreQueryParameters.fromNameAndType(topicStoreNameOne, QueryableStoreTypes.keyValueStore()));
                ReadOnlyKeyValueStore<String, String> topicTwoStore = kafkaStreams.store(StoreQueryParameters.fromNameAndType(topicStoreNameTwo, QueryableStoreTypes.keyValueStore()));
                ReadOnlyKeyValueStore<String, String> topicThreeStore = kafkaStreams.store(StoreQueryParameters.fromNameAndType(topicStoreNameThree, QueryableStoreTypes.keyValueStore()));

                topicOneStoreRef.set(topicOneStore);
                topicTwoStoreRef.set(topicTwoStore);
                topicThreeStoreRef.set(topicThreeStore);

                assignedPartitions.set(kafkaStreams);
            } else {
                topicOneStoreRef.set(null);
                topicTwoStoreRef.set(null);
                topicThreeStoreRef.set(null);
                assignedPartitions.set(null);
            }
        });


        kafkaStreams.start();
    }

    public void logAssignedPartitions() {
        var kafkaStreamsInstance = assignedPartitions.get();

        if (kafkaStreamsInstance == null) {
            return;
        }

        var assignedPartitions = new HashSet<TopicPartition>();

        var metadata = kafkaStreamsInstance.localThreadsMetadata();
        metadata.forEach(meta -> meta.activeTasks().forEach(task -> assignedPartitions.addAll(task.topicPartitions())));

        logger.info("Assigned partitions: {}", assignedPartitions);
    }
}
