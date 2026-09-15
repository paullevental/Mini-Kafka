# Mini Kafka

A simplified version of kafka consisting of a single message broker written from scratch. Producers append messages to an on-disk log over a custom binary protocol, and consumers read them back by offset.

The main objective of this project is to never lose a message the broker acknowledged as durable, even across a crash. Records are checksummed so a write cut short by a crash can be detected and truncated on restart.

Project specifications:
Java: Broker, JUnit testing <br>
Python: clients, fault-injection harness, and benchmark script

## Running it

```bash
cd backend
mvn compile
java -cp target/classes com.minikafka.Broker
```

Then, in a second terminal:

```bash
python3 send.py hello
python3 read.py          # -> hello
```

## What's built

- **TCP server** — blocking accept loop, one virtual thread per connection.
- **Binary framing** — 4-byte length prefix, 8-byte request header, dispatch on API key. Produce and Fetch.
- **Flat-file log** — appends payloads, returns the whole file on read.
- **Record format** — length, CRC32, offset, timestamp, key, value, with checksum validation on read.
- **Python clients** — `send.py` and `read.py`, exercising the path end to end.

## What's planned

- **Segmented log** — size-capped `.log` files named for their first offset, with a sparse `.index` mapping offsets to byte positions.
- **Real offsets** — topics, partitions, and broker-assigned offsets that survive restart.
- **Durability modes** — `acks=0/1/all`, where `all` returns only after `fsync`.
- **Crash recovery** — scan the active segment on startup, truncate at the first record whose checksum fails.
- **Named consumers** — commit a read position by name and resume from it after a restart.
- **Chaos harness and benchmarks** — `SIGKILL` the broker mid-write and verify no acknowledged message is lost.

## Out of scope

Replication, consumer groups and rebalancing, exactly-once semantics, log compaction, retention, TLS, and auth. This is one honest node rather than a half-built distributed system, and each exclusion is a deliberate trade rather than an oversight.

## Design

- **Blocking I/O, one virtual thread per connection.** No NIO and no selectors — virtual threads make thread-per-connection cheap enough that the complexity buys nothing.
- **Custom binary protocol** over raw TCP. A 4-byte length prefix frames each request; see `protocol.md`.
- **Records carry a CRC32**, which is what makes crash recovery possible: on restart, the first record whose checksum fails is where the process died.
