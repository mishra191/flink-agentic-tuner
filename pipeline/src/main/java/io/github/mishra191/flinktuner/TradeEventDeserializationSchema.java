package io.github.mishra191.flinktuner;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Parses JSON trade events. Malformed records are logged and skipped (returned as null)
 * rather than failing the job, so one bad message cannot cause a restart loop.
 */
public class TradeEventDeserializationSchema implements DeserializationSchema<TradeEvent> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(TradeEventDeserializationSchema.class);

    private transient ObjectMapper mapper;

    @Override
    public void open(InitializationContext context) {
        mapper = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    @Override
    public TradeEvent deserialize(byte[] message) {
        if (message == null || message.length == 0) {
            return null;
        }
        try {
            return mapper.readValue(message, TradeEvent.class);
        } catch (Exception e) {
            LOG.warn("Skipping malformed trade event: {}", e.getMessage());
            return null;
        }
    }

    @Override
    public boolean isEndOfStream(TradeEvent nextElement) {
        return false;
    }

    @Override
    public TypeInformation<TradeEvent> getProducedType() {
        return Types.POJO(TradeEvent.class);
    }
}
