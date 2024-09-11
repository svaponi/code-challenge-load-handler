package com.akelius.codechallengeloadhandler;

import java.util.*;

public class Exercise {

    private static final int MAX_PRICE_UPDATES_PER_SECOND = 100;

    public static void main(final String[] args) {
        final Consumer consumer = new Consumer();
        final LoadHandler loadHandler = new LoadHandler(consumer);
        final Producer producer = new Producer(loadHandler);

        producer.start();

        final double rate = consumer.getRate();
        System.out.println("Rate (updates/second): " + rate);

        if (rate > (MAX_PRICE_UPDATES_PER_SECOND * 1.25)) {
            throw new RuntimeException("Too many updates per second");
        }
    }

    public static class Consumer {

        private long firstReceivedAt;
        private long lastReceivedAt;
        private long counter;

        public synchronized void send(final List<PriceUpdate> priceUpdates) {
            priceUpdates.forEach(System.out::println);
            counter += priceUpdates.size();
            if (firstReceivedAt == 0) {
                firstReceivedAt = System.currentTimeMillis();
            }
            lastReceivedAt = System.currentTimeMillis();
        }

        public double getRate() {
            return (double) counter / (lastReceivedAt - firstReceivedAt) * 1000;
        }
    }

    public static class LoadHandler {

        private final Consumer consumer;

        public LoadHandler(final Consumer consumer) {
            this.consumer = consumer;
        }

        public void receive(final PriceUpdate priceUpdate) {

            consumer.send(Collections.singletonList(priceUpdate));
        }

    }

    public record PriceUpdate(String companyName, double price) {
    }

    public static class Producer extends Thread {

        private final LoadHandler loadHandler;

        public Producer(final LoadHandler loadHandler) {
            this.loadHandler = loadHandler;
        }

        @Override
        public void run() {
            generateUpdates();
        }

        public void generateUpdates() {
            for (int i = 1; i < 10000; i++) {
                loadHandler.receive(new PriceUpdate("Apple", 97.85));
                loadHandler.receive(new PriceUpdate("Google", 160.71));
                loadHandler.receive(new PriceUpdate("Facebook", 91.66));
                loadHandler.receive(new PriceUpdate("Google", 160.73));
                loadHandler.receive(new PriceUpdate("Facebook", 91.71));
                loadHandler.receive(new PriceUpdate("Google", 160.76));
                loadHandler.receive(new PriceUpdate("Apple", 97.85));
                loadHandler.receive(new PriceUpdate("Google", 160.71));
                loadHandler.receive(new PriceUpdate("Facebook", 91.63));
            }
        }
    }
}
