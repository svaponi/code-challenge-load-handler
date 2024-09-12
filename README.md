# Code Challenge Load Handler

## Scenario

The system consists of three components:

- **Producer** (`com.akelius.codechallengeloadhandler.Exercise.StockPriceProducer`): This component generates continuous price
  updates for stocks. It should operate without blocking, ensuring that each price update is consumed as quickly as
  possible.

- **Consumer** (`com.akelius.codechallengeloadhandler.Exercise.StockPriceConsumer`): This component processes the price updates.
  **It represents a legacy system that can handle only a limited number of price updates per second**.

- **Load Handler** (`com.akelius.codechallengeloadhandler.Exercise.LoadHandler`): This component manages the flow of
  traffic between the producer and the consumer. Currently, it merely forwards the updates from the producer to the
  consumer. This behavior needs to be modified.

## Task

Your task is to update the `LoadHandler` to enforce a rate limit on the number of updates sent to the consumer,
restricting it to a specified maximum (`MAX_PRICE_UPDATES` per second).

When sending updates to the consumer, ensure that only the most recent updates are considered. It is permissible to drop
price updates if necessary.

### Extra Bonus Point

As an extra bonus point, implement a mechanism that:

1. Ensures the distribution of updates among different stocks is as equal as possible, even when older updates are
   included.
2. If the update rate permits, sends older updates to maximize the detail of price variations.

This means balancing the number of updates per stock to maintain a fair representation of price variations while also
allowing older updates to provide more detailed price information if the system’s update rate allows it.
