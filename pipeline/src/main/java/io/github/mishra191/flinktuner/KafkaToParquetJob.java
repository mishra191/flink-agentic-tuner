package io.github.mishra191.flinktuner;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

/**
 * Reads trade events from Kafka and writes them to Parquet with exactly-once guarantees.
 * All tuning knobs come from {@link JobConfig}, so the agent can restart it with new settings.
 */
public class KafkaToParquetJob {

    public static void main(String[] args) throws Exception {
        final JobConfig config = JobConfig.load(args);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(config.parallelism);
        // fixed max parallelism keeps keyed state compatible when the agent rescales
        env.setMaxParallelism(config.maxParallelism);
        env.enableCheckpointing(config.checkpointIntervalMs);
        // exposes the effective config at GET /jobs/{id}/config for the agent
        env.getConfig().setGlobalJobParameters(config.params);

        KafkaSource<TradeEvent> source = KafkaSource.<TradeEvent>builder()
                .setBootstrapServers(config.kafkaBootstrap)
                .setTopics(config.kafkaTopic)
                .setGroupId(config.kafkaGroupId)
                // resume from committed offsets on a fresh start; savepoint offsets win on restore
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
                .setValueOnlyDeserializer(new TradeEventDeserializationSchema())
                .build();

        // stable uids let the job restart from a savepoint after reconfiguration
        DataStream<TradeEvent> stream = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-source")
                .uid("kafka-source");

        // drop unparseable records and invalid prices
        DataStream<TradeEvent> cleaned = stream
                .filter(e -> e != null && e.price > 0)
                .uid("clean-filter")
                .name("clean-filter");

        FileSink<TradeEvent> sink = ParquetSinkBuilder.build(config);

        // partition by symbol so each symbol's bucket is written by a single subtask
        cleaned.keyBy(e -> e.symbol == null ? "unknown" : e.symbol)
                .sinkTo(sink)
                .uid("parquet-sink")
                .name("parquet-sink");

        env.execute("Kafka -> Parquet (" + config.kafkaTopic + ")");
    }
}
