# code-challenge-load-handler

## Scenario

There are 3 components:

- a producer (`com.akelius.codechallengeloadhandler.Exercise.Producer`) of price updates for stocks. This component
  generates constant price updates. The producer should not block, every price update has to consumed as quickly as
  possible.

- a consumer (`com.akelius.codechallengeloadhandler.Exercise.Consumer`) of price updates for stocks. This component
  processes the price updates. **The consumer is representing a legacy system that cannot consumer more than a certain
  number of price updates per second**.

- a load handler (`com.akelius.codechallengeloadhandler.Exercise.LoadHandler`) to handle the traffic. This component
  receives the price updates from the producer and pass them to the consumer. In the current implementation the load
  handler is just passing on the update to a consumer. This should be changed.

## Task

The task of this exercise is to update the `LoadHandler` to limit the updates per second to the consumer to a certain
given number (`MAX_PRICE_UPDATES`).

If a price update will be sent to the consumer, it has to be the most recent price.

It is allowed to drop price updates.

