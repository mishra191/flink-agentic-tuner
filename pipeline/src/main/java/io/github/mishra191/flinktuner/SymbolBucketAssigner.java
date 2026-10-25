package io.github.mishra191.flinktuner;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * Buckets by symbol, optionally sub-partitioned by event hour (UTC):
 * symbol=btc_usd or symbol=btc_usd/hour=2025-11-17--16
 */
public class SymbolBucketAssigner implements BucketAssigner<TradeEvent, String> {

    private static final DateTimeFormatter HOUR_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd--HH").withZone(ZoneOffset.UTC);

    private final boolean includeHour;

    public SymbolBucketAssigner() {
        this(false);
    }

    public SymbolBucketAssigner(boolean includeHour) {
        this.includeHour = includeHour;
    }

    @Override
    public String getBucketId(TradeEvent element, Context context) {
        String bucket = "symbol=" + sanitize(element == null ? null : element.symbol);
        if (!includeHour) {
            return bucket;
        }

        // Fall back to processing time when the event carries no timestamp
        long ts = element != null && element.timestamp > 0 ? element.timestamp : context.currentProcessingTime();
        return bucket + "/hour=" + HOUR_FORMAT.format(Instant.ofEpochMilli(ts));
    }

    private static String sanitize(String symbol) {
        if (symbol == null || symbol.isEmpty()) {
            return "unknown";
        }

        return symbol
                .replace("/", "_")
                .replace(" ", "_")
                .replace(":", "_")
                .toLowerCase();
    }

    @Override
    public SimpleVersionedSerializer<String> getSerializer() {
        return SimpleVersionedStringSerializer.INSTANCE;
    }
}
