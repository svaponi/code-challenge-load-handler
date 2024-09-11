package com.akelius.codechallengeloadhandler;

import java.util.*;
import java.util.concurrent.*;

public class Exercise {

    private static final int MAX_PRICE_UPDATES_PER_SECOND = 100;
    private static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    public static void main(final String[] args) {
        try {

            final Consumer consumer = new Consumer();
            final LoadHandler loadHandler = new LoadHandler(consumer);
            final Producer producer = new Producer(loadHandler);

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

            // check the consumer rate, fail if it's too high (tolerance 25%)
            if (consumer.getRate() > (MAX_PRICE_UPDATES_PER_SECOND * 1.25)) {
                throw new RuntimeException("Too many updates per second");
            }

        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            scheduler.shutdown();
        }

    }

    public static class Consumer {

        private long firstReceivedAt;
        private long lastReceivedAt;
        private long counter;

        public synchronized void send(final List<PriceUpdate> priceUpdates) {
            // priceUpdates.forEach(System.out::println);
            counter += priceUpdates.size();
            if (firstReceivedAt == 0) {
                firstReceivedAt = System.currentTimeMillis();
            }
            lastReceivedAt = System.currentTimeMillis();
        }

        public double getRate() {
            return (double) counter / (lastReceivedAt - firstReceivedAt) * 1000;
        }

        public void printStats() {
            System.out.println("Rate (updates/second): " + this.getRate());
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
            while (!isInterrupted()) {
                loadHandler.receive(new PriceUpdate("Apple", 97.85));
                loadHandler.receive(new PriceUpdate("Google", 160.71));
                loadHandler.receive(new PriceUpdate("Facebook", 91.66));
                loadHandler.receive(new PriceUpdate("Google", 160.73));
                loadHandler.receive(new PriceUpdate("Facebook", 91.71));
                loadHandler.receive(new PriceUpdate("Google", 160.76));
                loadHandler.receive(new PriceUpdate("Apple", 97.85));
                loadHandler.receive(new PriceUpdate("Google", 160.71));
                loadHandler.receive(new PriceUpdate("Facebook", 91.63));

                // you may want to add some delay if your cpu starts to burn
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
