"""Synthetic trade traffic for the experiments.

Profiles are rate functions of elapsed seconds, so every run of a scenario sees the same load.
Symbols follow a skewed (Zipf-like) distribution: a few symbols get most of the trades.
"""

import json
import random
import threading
import time

from confluent_kafka import Producer

SYMBOLS = [f"SYM{i:02d}/USD" for i in range(20)]
WEIGHTS = [1 / (i + 1) for i in range(len(SYMBOLS))]


def steady(rate: float):
    return lambda t: rate


def bursty(base: float, peak: float, period_s: float, burst_s: float):
    """`base` events/s, rising to `peak` for `burst_s` seconds at the start of every `period_s`."""
    return lambda t: peak if (t % period_s) < burst_s else base


class LoadGenerator(threading.Thread):
    def __init__(self, bootstrap: str, topic: str, rate_fn, seed: int = 42):
        super().__init__(daemon=True)
        self.producer = Producer({"bootstrap.servers": bootstrap, "linger.ms": 20})
        self.topic = topic
        self.rate_fn = rate_fn
        self.rng = random.Random(seed)
        self.prices = {s: 100.0 * (i + 1) for i, s in enumerate(SYMBOLS)}
        self.sent = 0
        self._halt = threading.Event()

    def stop(self):
        self._halt.set()

    def run(self):
        start = time.time()
        step = 0.1
        owed = 0.0
        while not self._halt.is_set():
            tick = time.time()
            owed += self.rate_fn(tick - start) * step
            n, owed = int(owed), owed - int(owed)
            for _ in range(n):
                symbol = self.rng.choices(SYMBOLS, WEIGHTS)[0]
                # random walk keeps prices positive and plausible
                self.prices[symbol] = max(0.01, self.prices[symbol] * (1 + self.rng.gauss(0, 0.001)))
                event = {"symbol": symbol, "price": round(self.prices[symbol], 4), "timestamp": int(time.time() * 1000)}
                self.producer.produce(self.topic, json.dumps(event).encode())
            self.sent += n
            self.producer.poll(0)
            time.sleep(max(0.0, step - (time.time() - tick)))
        self.producer.flush(10)
