# sharesAllocation: Smart Order Router Backtest

A smart order router (SOR) simulator. Given a buy order (for example 5,000 shares) and the same stock quoted at different prices and sizes on several trading venues, it decides how many shares to send to each venue to minimize total cost. It replays one recorded trading day of quotes through Kafka, runs the router on each snapshot, and compares it against three simpler buying strategies.

The data is **simulated, not live**. Quotes come from a CSV and are replayed at their original pace.

## The problem

Each venue shows its own best ask price and the number of shares available at that price. Sending the whole order to the cheapest venue is not always possible, because that venue may not have enough shares. The router has to split the order across venues so that it fills completely at the lowest total cost, including per-share fees.

## How it works

```
l1_day.csv  ->  kafka_producer.py  ->  Kafka topic  ->  backtest.py (consumer)  ->  allocator.py  ->  results
                (replay in order)      mock_11_stream    (group into snapshots)    (split the order)
```

1. **Producer** (`kafka_producer.py`): reads the CSV, keeps quotes inside a fixed time window (13:36:32 to 13:45:14), sorts them by timestamp, and sends each one to Kafka as JSON. It sleeps between messages to match the original gaps, so replay runs in real time (about 9 minutes).
2. **Consumer** (`backtest.py`): reads the topic from the beginning and groups messages that share a timestamp into a snapshot of `{venue: quote}`.
3. **Allocator** (`allocator.py`): for each snapshot, splits the remaining shares across venues in 100-share steps using dynamic programming keyed on `(venue index, shares remaining)`. Cost is the ask price plus fee for executed shares, with penalty terms for over- and under-execution based on the Cont-Kukanov model of order placement.
4. **Backtest**: runs the router with a small grid of penalty parameters, keeps the best configuration, and reports cost against three baselines.

### Strategies compared

| Strategy | Behavior |
|---|---|
| SOR (this project) | Calls the allocator on each snapshot for the remaining shares |
| Best-ask | On each snapshot, buys from the cheapest venue first until the order is filled |
| TWAP | Splits the order into roughly 1-minute intervals and buys an equal chunk in each |
| Size-weighted ("VWAP") | Buys from each venue in proportion to its displayed size. This is a simplified proxy, not a historical-volume VWAP |

The output reports average fill price per strategy and savings in basis points versus each baseline.

## Repository layout

| File | Purpose |
|---|---|
| `allocator.py` | `allocate()` dynamic program and `compute_cost()` cost model |
| `allocator_pseudocode.txt` | Brute-force reference version of the same model |
| `kafka_producer.py` | Replays the CSV into Kafka |
| `backtest.py` | Consumes snapshots, runs all strategies, prints results |
| `docker-compose.yml` | Local Kafka and Zookeeper |

## Running it

**Requirements:** Docker, Python 3.12, and a CSV named `l1_day.csv` in the repo root with at least these columns: `ts_event`, `publisher_id`, `ask_px_00`, `ask_sz_00`. The data file is not included in this repository.

```bash
pip install kafka-python python-dateutil
docker compose up -d        # starts Kafka on localhost:9092
python backtest.py          # runs the producer, then the consumer and simulations
```

The producer replays in real time, so a full run takes about 9 minutes.

**Between runs, reset Kafka.** The producer appends to the same topic and the consumer reads from the earliest offset, so a second run would see every quote twice. Because the compose file has no volumes, this clears the topic:

```bash
docker compose down && docker compose up -d
```

The project was run on an AWS EC2 instance with Docker. Kafka advertises `localhost:9092`, so the producer and consumer must run on the same machine as the broker.

## Parameters

| Name | Where | Meaning |
|---|---|---|
| `ORDER_SIZE` | `backtest.py` | Shares to buy (5,000) |
| `FEE`, `REBATE` | `backtest.py` | Per-share fee and rebate |
| `lambda_over`, `lambda_under` | `PARAM_GRID` | Penalty per extra or missing share |
| `theta_queue` | `PARAM_GRID` | Linear penalty on the gap between executed and target shares |
| `step` | `allocate()` | Allocation granularity (100 shares) |

## Known limitations

This is an academic-style simulation, and I have documented where it falls short of a real router.

- **One short window.** Results come from a single 8-minute period with no held-out data.
- **No tests yet.**

## Planned improvements

1. Allow partial fills and a resting-order portion so the penalty terms take effect.
2. Report fill rate and implementation shortfall alongside average price, and run all strategies at the same decision times.
3. Carry forward the latest quote per venue when building snapshots.
4. Add a replay speed multiplier, run the consumer concurrently with the producer, and use a fresh topic per run.
5. Add unit tests (invariants, edge cases, brute-force comparison) and run them in CI.
6. Evaluate across multiple days with a train and test split.

## Reference

Cont, R. and Kukanov, A., "Optimal order placement in limit order markets" (2017). This is the model the cost function is based on.
