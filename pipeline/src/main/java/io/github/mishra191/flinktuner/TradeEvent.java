package io.github.mishra191.flinktuner;

import java.io.Serializable;

/** One trade: symbol, price and event time in epoch milliseconds. */
public class TradeEvent implements Serializable {

    public String symbol;
    public double price;
    public long timestamp;

    public TradeEvent() {}

    public TradeEvent(String symbol, double price, long timestamp) {
        this.symbol = symbol;
        this.price = price;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "TradeEvent{symbol='" + symbol + "', price=" + price + ", timestamp=" + timestamp + '}';
    }
}
