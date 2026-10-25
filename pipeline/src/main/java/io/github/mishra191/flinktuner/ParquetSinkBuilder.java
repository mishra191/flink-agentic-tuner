package io.github.mishra191.flinktuner;

import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;

import java.time.ZoneOffset;

public class ParquetSinkBuilder {

    // How often the sink checks the time-based rolling condition
    private static final long BUCKET_CHECK_INTERVAL_MS = 30_000;

    public static FileSink<TradeEvent> build(JobConfig config) {
        return build(config.outputPath, config.bucketStrategy, config.rollingMaxPartBytes, config.rollingIntervalMs);
    }

    public static FileSink<TradeEvent> build(String outputPath,
                                             JobConfig.BucketStrategy bucketStrategy,
                                             long rollingMaxPartBytes,
                                             long rollingIntervalMs) {

        return FileSink
                .forBulkFormat(
                        new Path(outputPath),
                        ParquetAvroWriters.forReflectRecord(TradeEvent.class)
                )
                .withBucketAssigner(bucketAssigner(bucketStrategy))
                .withRollingPolicy(new SizeTimeRollingPolicy(rollingMaxPartBytes, rollingIntervalMs))
                .withBucketCheckInterval(BUCKET_CHECK_INTERVAL_MS)
                .withOutputFileConfig(OutputFileConfig.builder().withPartSuffix(".parquet").build())
                .build();
    }

    private static BucketAssigner<TradeEvent, String> bucketAssigner(JobConfig.BucketStrategy strategy) {
        switch (strategy) {
            case HOUR:
                return new DateTimeBucketAssigner<>("yyyy-MM-dd--HH", ZoneOffset.UTC);
            case SYMBOL_HOUR:
                return new SymbolBucketAssigner(true);
            case SYMBOL:
            default:
                return new SymbolBucketAssigner();
        }
    }
}
