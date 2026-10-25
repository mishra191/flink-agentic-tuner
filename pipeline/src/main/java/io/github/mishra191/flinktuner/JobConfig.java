package io.github.mishra191.flinktuner;

import org.apache.flink.api.java.utils.ParameterTool;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;

/**
 * Job settings: application.properties defaults, overridden by --key value args.
 * Restarting the job with different args is how the tuning agent reconfigures it.
 */
public class JobConfig implements Serializable {

    public enum BucketStrategy { SYMBOL, HOUR, SYMBOL_HOUR }

    public final ParameterTool params;

    public final String kafkaBootstrap;
    public final String kafkaTopic;
    public final String kafkaGroupId;

    public final int parallelism;
    public final int maxParallelism;
    public final long checkpointIntervalMs;

    public final String outputPath;
    public final BucketStrategy bucketStrategy;
    public final long rollingMaxPartBytes;
    public final long rollingIntervalMs;

    private JobConfig(ParameterTool p) {
        this.params = p;
        this.kafkaBootstrap = p.get("kafka.bootstrap.servers");
        this.kafkaTopic = p.get("kafka.topic");
        this.kafkaGroupId = p.get("kafka.group.id", "trade-parquet-pipeline");
        this.parallelism = p.getInt("flink.parallelism", 4);
        this.maxParallelism = p.getInt("flink.max.parallelism", 128);
        this.checkpointIntervalMs = p.getLong("flink.checkpoint.interval.ms", 10_000);
        this.outputPath = p.get("output.path");
        this.bucketStrategy = BucketStrategy.valueOf(p.get("sink.bucket.strategy", "symbol").toUpperCase());
        this.rollingMaxPartBytes = p.getLong("sink.rolling.max.part.mb", 128) * 1024 * 1024;
        this.rollingIntervalMs = p.getLong("sink.rolling.interval.min", 5) * 60 * 1000;
    }

    public static JobConfig load(String[] args) throws IOException {
        ParameterTool defaults;
        try (InputStream in = JobConfig.class.getClassLoader().getResourceAsStream("application.properties")) {
            defaults = in == null ? ParameterTool.fromArgs(new String[0]) : ParameterTool.fromPropertiesFile(in);
        }
        return new JobConfig(defaults.mergeWith(ParameterTool.fromArgs(args)));
    }
}
