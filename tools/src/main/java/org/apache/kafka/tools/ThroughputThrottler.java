/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.tools;

import java.util.Arrays;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * This class helps producers throttle throughput.
 *
 * If targetThroughput >= 0, the resulting average throughput will be approximately
 * min(targetThroughput, maximumPossibleThroughput). If targetThroughput < 0,
 * no throttling will occur.
 *
 * To use, do this between successive send attempts:
 * <pre>
 *     {@code
 *      if (throttler.shouldThrottle(...)) {
 *          throttler.throttle();
 *      }
 *     }
 * </pre>
 *
 * Note that this can be used to throttle message throughput or data throughput.
 */
public class ThroughputThrottler {

    private static final long NS_PER_MS = 1000000L;
    private static final long NS_PER_SEC = 1000 * NS_PER_MS;
    private static final long MIN_SLEEP_NS = 2 * NS_PER_MS;

    private final long startMs;
    private final long sleepTimeNs;
    private final long targetThroughput;

    private long sleepDeficitNs = 0;
    private static class Stats {
        private long start;
        private long windowStart;
        private int[] latencies;
        private int sampling;
        private int iteration;
        private int index;
        private long count;
        private long bytes;
        private int maxLatency;
        private long totalLatency;
        private long windowCount;
        private int windowMaxLatency;
        private long windowTotalLatency;
        private long windowBytes;
        private long reportingInterval;

        public Stats(long numRecords, int reportingInterval) {
            this.start = System.currentTimeMillis();
            this.windowStart = System.currentTimeMillis();
            this.iteration = 0;
            this.sampling = (int) (numRecords / Math.min(numRecords, 500000));
            this.latencies = new int[(int) (numRecords / this.sampling) + 1];
            this.index = 0;
            this.maxLatency = 0;
            this.totalLatency = 0;
            this.windowCount = 0;
            this.windowMaxLatency = 0;
            this.windowTotalLatency = 0;
            this.windowBytes = 0;
            this.totalLatency = 0;
            this.reportingInterval = reportingInterval;
        }

        public void record(int iter, int latency, int bytes, long time) {
            this.count++;
            this.bytes += bytes;
            this.totalLatency += latency;
            this.maxLatency = Math.max(this.maxLatency, latency);
            this.windowCount++;
            this.windowBytes += bytes;
            this.windowTotalLatency += latency;
            this.windowMaxLatency = Math.max(windowMaxLatency, latency);
            if (iter % this.sampling == 0) {
                this.latencies[index] = latency;
                this.index++;
            }
            /* maybe report the recent perf */
            if (time - windowStart >= reportingInterval) {
                printWindow();
                newWindow();
            }
        }

        public Callback nextCompletion(long start, int bytes, Stats stats) {
            Callback cb = new PerfCallback(this.iteration, start, bytes, stats);
            this.iteration++;
            return cb;
        }

        public void printWindow() {
            long ellapsed = System.currentTimeMillis() - windowStart;
            double recsPerSec = 1000.0 * windowCount / (double) ellapsed;
            double mbPerSec = 1000.0 * this.windowBytes / (double) ellapsed / (1024.0 * 1024.0);
            System.out.printf("%d records sent, %.1f records/sec (%.2f MB/sec), %.1f ms avg latency, %.1f ms max latency.%n",
                              windowCount,
                              recsPerSec,
                              mbPerSec,
                              windowTotalLatency / (double) windowCount,
                              (double) windowMaxLatency);
        }

        public void newWindow() {
            this.windowStart = System.currentTimeMillis();
            this.windowCount = 0;
            this.windowMaxLatency = 0;
            this.windowTotalLatency = 0;
            this.windowBytes = 0;
        }

        public void printTotal() {
            long elapsed = System.currentTimeMillis() - start;
            double recsPerSec = 1000.0 * count / (double) elapsed;
            double mbPerSec = 1000.0 * this.bytes / (double) elapsed / (1024.0 * 1024.0);
            int[] percs = percentiles(this.latencies, index, 0.5, 0.95, 0.99, 0.999);
            System.out.printf("%d records sent, %f records/sec (%.2f MB/sec), %.2f ms avg latency, %.2f ms max latency, %d ms 50th, %d ms 95th, %d ms 99th, %d ms 99.9th.%n",
                              count,
                              recsPerSec,
                              mbPerSec,
                              totalLatency / (double) count,
                              (double) maxLatency,
                              percs[0],
                              percs[1],
                              percs[2],
                              percs[3]);
        }

        private static int[] percentiles(int[] latencies, int count, double... percentiles) {
            int size = Math.min(count, latencies.length);
            Arrays.sort(latencies, 0, size);
            int[] values = new int[percentiles.length];
            for (int i = 0; i < percentiles.length; i++) {
                int index = (int) (percentiles[i] * size);
                values[i] = latencies[index];
            }
            return values;
        }
    }

    private static final class PerfCallback implements Callback {
        private final long start;
        private final int iteration;
        private final int bytes;
        private final Stats stats;

        public PerfCallback(int iter, long start, int bytes, Stats stats) {
            this.start = start;
            this.stats = stats;
            this.iteration = iter;
            this.bytes = bytes;
        }

        public void onCompletion(RecordMetadata metadata, Exception exception) {
            long now = System.currentTimeMillis();
            int latency = (int) (now - start);
            this.stats.record(iteration, latency, bytes, now);
            if (exception != null)
                exception.printStackTrace();
        }
    }boolean wakeup = false;

    /**
     * @param targetThroughput Can be messages/sec or bytes/sec
     * @param startMs          When the very first message is sent
     */
    public ThroughputThrottler(long targetThroughput, long startMs) {
        this.startMs = startMs;
        this.targetThroughput = targetThroughput;
        this.sleepTimeNs = targetThroughput > 0 ?
                           NS_PER_SEC / targetThroughput :
                           Long.MAX_VALUE;
    }

    /**
     * @param amountSoFar bytes produced so far if you want to throttle data throughput, or
     *                    messages produced so far if you want to throttle message throughput.
     * @param sendStartMs timestamp of the most recently sent message
     * @return
     */
    public boolean shouldThrottle(long amountSoFar, long sendStartMs) {
        if (this.targetThroughput < 0) {
            // No throttling in this case
            return false;
        }

        float elapsedSec = (sendStartMs - startMs) / 1000.f;
        return elapsedSec > 0 && (amountSoFar / elapsedSec) > this.targetThroughput;
    }

    /**
     * Occasionally blocks for small amounts of time to achieve targetThroughput.
     *
     * Note that if targetThroughput is 0, this will block extremely aggressively.
     */
    public void throttle() {
        if (targetThroughput == 0) {
            try {
                synchronized (this) {
                    while (!wakeup) {
                        this.wait();
                    }
                }
            } catch (InterruptedException e) {
                // do nothing
            }
            return;
        }

        // throttle throughput by sleeping, on average,
        // (1 / this.throughput) seconds between "things sent"
        sleepDeficitNs += sleepTimeNs;

        // If enough sleep deficit has accumulated, sleep a little
        if (sleepDeficitNs >= MIN_SLEEP_NS) {
            long sleepStartNs = System.nanoTime();
            try {
                synchronized (this) {
                    long remaining = sleepDeficitNs;
                    while (!wakeup && remaining > 0) {
                        long sleepMs = remaining / 1000000;
                        long sleepNs = remaining - sleepMs * 1000000;
                        this.wait(sleepMs, (int) sleepNs);
                        long elapsed = System.nanoTime() - sleepStartNs;
                        remaining = sleepDeficitNs - elapsed;
                    }
                    wakeup = false;
                }
                sleepDeficitNs = 0;
            } catch (InterruptedException e) {
                // If sleep is cut short, reduce deficit by the amount of
                // time we actually spent sleeping
                long sleepElapsedNs = System.nanoTime() - sleepStartNs;
                if (sleepElapsedNs <= sleepDeficitNs) {
                    sleepDeficitNs -= sleepElapsedNs;
                }
            }
        }
    }

    /**
     * Wakeup the throttler if its sleeping.
     */
    public void wakeup() {
        synchronized (this) {
            wakeup = true;
            this.notifyAll();
        }
    }
}

