package com.akelius.codechallengeloadhandler;

import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;
import java.util.stream.*;


public class Exercise {

    private static final int MAX_PRICE_UPDATES_PER_SECOND = 100;
    private static final double TOLERANCE = 0.20;
    private static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    public static void main(final String[] args) {
        try {

            final StockPriceConsumer consumer = new StockPriceConsumer();
            final LoadHandler loadHandler = new LoadHandler(consumer);
            final StockPriceProducer producer = new StockPriceProducer(loadHandler);

            // start the producer
            producer.start();

            // interrupt the producer after 10 seconds
            scheduler.schedule(producer::interrupt, 10, TimeUnit.SECONDS);

            // print out consumer stats every second
            ScheduledFuture<?> fut = scheduler.scheduleAtFixedRate(consumer::printStats, 1, 1, TimeUnit.SECONDS);

            // wait for the producer to stop
            producer.join();

            // cancel the consumer stats printout
            fut.cancel(false);

            StockPriceStats stats = consumer.getStats();

            // check the consumer rate, fail if it's too high (with tolerance)
            if (stats.rate > (MAX_PRICE_UPDATES_PER_SECOND * (1 + TOLERANCE))) {
                throw new RuntimeException("Too many updates per second");
            }

            // check the consumer rate per stock, fail if it's too high (with tolerance)
            for (StockPriceCompanyStats stockStats : stats.companyStats) {
                if (stockStats.rate > (MAX_PRICE_UPDATES_PER_SECOND * (1 + TOLERANCE) / stats.companyStats.size())) {
                    throw new RuntimeException("Too many updates per second for " + stockStats.companyName);
                }
            }

            // check the consumer collected any warnings
            if (!stats.warnings.isEmpty()) {
                throw new RuntimeException("Consumer warnings: " + stats.warnings);
            }

            System.out.println("Nice job!");

        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            scheduler.shutdown();
        }
    }

    public static class StockPriceConsumer implements Consumer<StockPrice> {

        private long firstReceivedAt;
        private long lastReceivedAt;
        private final Map<String, Integer> counters = new HashMap<>();
        private final Map<String, Instant> lastStockPriceTimestamps = new HashMap<>();
        private final Set<String> warnings = new HashSet<>();

        @Override
        public synchronized void accept(StockPrice stockPrice) {
            counters.merge(stockPrice.companyName, 1, Integer::sum);
            Instant lastStockPriceTimestamp = lastStockPriceTimestamps.put(stockPrice.companyName, stockPrice.timestamp);
            if (lastStockPriceTimestamp != null && stockPrice.timestamp.isBefore(lastStockPriceTimestamp)) {
                warnings.add("Obsolete stock price for " + stockPrice.companyName);
            }
            long now = System.currentTimeMillis();
            if (firstReceivedAt == 0) {
                firstReceivedAt = now;
            }
            lastReceivedAt = now;
        }

        private double calcRate(Integer count) {
            long div = lastReceivedAt - firstReceivedAt;
            return div == 0 ? 0.0 : 1000.0 * count / div;
        }

        public synchronized StockPriceStats getStats() {
            double rate = calcRate(counters.values().stream().mapToInt(Integer::intValue).sum());
            List<StockPriceCompanyStats> stockStats = counters.keySet().stream()
                    .map(s -> new StockPriceCompanyStats(s, this.calcRate(counters.getOrDefault(s, 0))))
                    .collect(Collectors.toList());
            return new StockPriceStats(rate, stockStats, warnings);
        }

        public void printStats() {
            System.out.println(this.getStats());
        }
    }

    public record StockPriceCompanyStats(String companyName, double rate) {
    }

    public record StockPriceStats(double rate, List<StockPriceCompanyStats> companyStats, Set<String> warnings) {
        @Override
        public String toString() {
            String distribution = companyStats.stream()
                    .sorted(Comparator.comparing(StockPriceCompanyStats::companyName))
                    .map(s -> s.companyName + "=" + String.format("%.2f", 100.0 * s.rate / rate) + "%")
                    .collect(Collectors.joining(", "));
            return "Rate (updates/second): " + String.format("%.2f", rate) + " - Distribution (" + distribution + ")" + (warnings.isEmpty() ? "" : (" - Warnings: " + warnings));
        }
    }

    public static class LoadHandler implements Consumer<StockPrice> {

        private static final int INTERVAL_MILLIS = 100;
        private final Map<String, ConcurrentLinkedDeque<StockPrice>> stockPriceBuffer = new ConcurrentHashMap<>();
        private final Consumer<StockPrice> delegate;

        public LoadHandler(final Consumer<StockPrice> consumer) {
            this.delegate = consumer;
            scheduler.scheduleAtFixedRate(this::flushBuffer, 0, INTERVAL_MILLIS, TimeUnit.MILLISECONDS);
        }

        @Override
        public synchronized void accept(StockPrice stockPrice) {
            stockPriceBuffer
                    .computeIfAbsent(stockPrice.companyName(), k -> new ConcurrentLinkedDeque<>())
                    .add(stockPrice);
        }

        private long lastRun;

        private void flushBuffer() {
            long now = System.currentTimeMillis();
            if (lastRun == 0) {
                lastRun = now;
                // skip the first run because we cannot know the rate yet
                return;
            }
            long sinceLastRun = now - lastRun;
            lastRun = now;

            // how many updates we can send out in this interval
            long batchSize = Math.floorDiv(MAX_PRICE_UPDATES_PER_SECOND * sinceLastRun, 1000);

            // how many updates we have in the buffer
            int bufferSize = stockPriceBuffer.values().stream().mapToInt(ConcurrentLinkedDeque::size).sum();

            List<StockPrice> stockPricesToPush = new ArrayList<>();
            if (bufferSize <= batchSize) {
                stockPriceBuffer.values().forEach(stockPricesToPush::addAll);
            } else {
                Set<String> companies = stockPriceBuffer.keySet();
                synchronized (stockPriceBuffer) {
                    while (stockPricesToPush.size() < batchSize) {
                        for (String company : companies) {
                            ConcurrentLinkedDeque<StockPrice> deque = stockPriceBuffer.get(company);
                            try {
                                stockPricesToPush.add(deque.pop());
                            } catch (Exception e) {
                                System.out.println("Buffer for " + company + " is empty");
                                companies.remove(company);
                            }
                        }
                    }
                    stockPriceBuffer.clear();
                }
            }

            for (StockPrice stockPrice : stockPricesToPush) {
                delegate.accept(stockPrice);
            }
        }
    }

    public record StockPrice(String companyName, double price, Instant timestamp) {

        public StockPrice(String companyName, double price) {
            this(companyName, price, Instant.now());
        }
    }

    public static class StockPriceProducer extends Thread {

        private final Consumer<StockPrice> consumer;

        public StockPriceProducer(final Consumer<StockPrice> consumer) {
            this.consumer = consumer;
        }

        @Override
        public void run() {
            Random rand = new Random();
            // The price updates are unbalanced on purpose! This makes the life more difficult for the LoadHandler
            // which should send out a similar number of updates for each company.
            String[] companies = {"Google", "Google", "Google", "Google", "Google", "Apple", "Facebook"};
            while (!isInterrupted()) {
                consumer.accept(new StockPrice(companies[rand.nextInt(companies.length)], rand.nextDouble() * 1_000));

                // add some delay if your cpu starts to burn
                /*
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                */
            }
        }
    }
}
