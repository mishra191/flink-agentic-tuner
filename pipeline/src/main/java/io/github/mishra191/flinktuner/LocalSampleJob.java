package io.github.mishra191.flinktuner;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;

/**
 * Writes a few sample trades through the same Parquet sink, without Kafka.
 * A quick local check that the sink, bucketing and rolling policy produce committed files.
 */
public class LocalSampleJob {

    public static void main(String[] args) throws Exception {
        final JobConfig config = JobConfig.load(args);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // Parquet part files are only committed on checkpoints; without this they stay .inprogress forever
        env.enableCheckpointing(config.checkpointIntervalMs);

        long now = System.currentTimeMillis();
        List<TradeEvent> events = List.of(
                new TradeEvent("BTC/USD", 44000.5, now),
                new TradeEvent("ETH/USD", 3100.2, now),
                new TradeEvent("DOGE/USDT", 0.091, now)
        );

        env.fromCollection(events)
                .sinkTo(ParquetSinkBuilder.build(
                        config.params.get("local.output.path", "output-parquet"),
                        config.bucketStrategy,
                        config.rollingMaxPartBytes,
                        config.rollingIntervalMs));

        env.execute("Local sample -> Parquet");
    }
}
