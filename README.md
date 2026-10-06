# Mini Kafka

A simplified version of kafka consisting of a single message broker written from scratch. Producers append messages to an on-disk log over a custom binary protocol, and consumers read them back by offset.

The main objective of this project is to never lose a message the broker acknowledged as durable, even across a crash. Records are checksummed so a write cut short by a crash can be detected and truncated on restart.

Project specifications:
Java: Broker, JUnit testing <br>
Python: clients, fault-injection harness, and benchmark script

Zero runtime dependencies — no Spring, no Netty, no NIO. Java 25, JUnit 5 test-scoped only.

## Running it

```bash
cd backend
mvn compile
java -cp target/classes com.minikafka.Broker   # listens on 8080
```

Then, in a second terminal, from `backend/`:

```bash
python3 send.py hello
python3 read.py                  # -> hello
python3 bench_connections.py 10000   # how many open connections the thread model holds
```

## What's built

### Storage

- **Record format** — length, CRC32, offset, timestamp, key, value. The checksum is verified *before* any field is trusted, which is what makes crash recovery possible later. Immutable.
- **Sparse offset index** (`OffsetIndex`) — memory-mapped, fixed 8-byte entries of `(offset delta from base, byte position in the log)`. An entry is written only once every 4 KB of log, so the index stays small enough to keep in memory; `lookup` is a floor binary search that returns a position at or before the target. On close the file is forced and truncated to its real entry count.
- **Log segment** (`LogSegment`) — one `.log` + `.index` pair, both named `%020d` after the segment's first offset. The segment owns its index. `append` checks the size cap before assigning an offset, so a rejected record burns no offset, and returns `-1` when the segment is full. `read` range-checks, jumps to the nearest indexed position, then scans forward discarding records below the requested offset.
- **Partition** (`Partition`) — the layer above segments. `append(List<Record>)` validates the whole batch first, so a batch containing one oversized record is rejected whole with nothing written, then appends record by record and rolls a new segment when the active one fills. Returns the batch's base offset. Every segment it touches is flushed before it returns. `earliestOffset()` / `latestOffset()` bound the readable range as the half-open interval `[earliest, latest)`.

Measured on 1,000,000 records of 132 bytes each, single-threaded, no `fsync`: **~497,000 records/sec**, 132 MB of log, index trimmed from 4,194,296 to 249,992 bytes on close.

### Network

- **TCP server** — blocking accept loop, one virtual thread per connection. Held 10,000 concurrent idle connections under `bench_connections.py`. This is the reason there is no NIO and no selector loop.
- **Binary framing** — 4-byte length prefix, 8-byte request header, dispatch on API key. See `protocol.md`.
- **Python clients** — `send.py` and `read.py`, exercising Produce and Fetch end to end.

## In progress

- **`Partition.read`** — returning a `FetchResult(List<Record> records, long endOffset)`. `endOffset` is the one number a consumer cannot work out for itself: it is the partition's log end offset at the moment of the read, so `endOffset` minus where the consumer got to is its lag. Kafka's equivalent is the high watermark.
- **Wiring real storage into the broker** — `Server` still routes Produce and Fetch through the Day 1 flat file (`Log`). Replacing that with `Partition` retires `Log.java`.

## What's planned

- **Restart safety** — the partition constructor currently always opens a fresh segment at offset 0, so reopening a directory that already holds data would restart offsets from zero. Needs a directory scan: list the `.log` files, parse their base offsets, open them in order.
- **Crash recovery** — `recoverAndTruncate()` is a stub. The real version scans the newest segment on startup and truncates at the first record whose checksum fails, which is where the process died.
- **Durability modes** — `acks=0/1/all`, where `all` returns only after `fsync`.
- **Named consumers** — commit a read position by name and resume from it after a restart. This replaces consumer groups.
- **Chaos harness and benchmarks** — `SIGKILL` the broker mid-write and verify no acknowledged message is lost.
- **Response format in `protocol.md`** — the request header and record format are specified; responses, the API key table, and error codes are not yet written down.

## Out of scope

Replication, consumer groups and rebalancing, exactly-once semantics, log compaction, retention, TLS, and auth. This is one honest node rather than a half-built distributed system, and each exclusion is a deliberate trade rather than an oversight.

## Design

- **Blocking I/O, one virtual thread per connection.** No NIO and no selectors — virtual threads make thread-per-connection cheap enough that the complexity buys nothing. Java 25 specifically, because JDK 24 removed virtual-thread pinning on `synchronized`, and the broker holds a monitor across `fsync`.
- **Custom binary protocol** over raw TCP. A 4-byte length prefix frames each request, because TCP is a byte stream with no message boundaries of its own; see `protocol.md`.
- **Records carry a CRC32**, which is what makes crash recovery possible: on restart, the first record whose checksum fails is where the process died.
- **Two-level offset lookup.** Finding offset N is a floor search over segment base offsets to pick the file, then a floor binary search in that segment's sparse index to pick a byte position near N, then a short forward scan. Neither level needs to hold every offset in memory.
- **Errors are return codes on the wire**, never exceptions. Inside a single JVM, ordinary Java exceptions are fine; the handler layer is what maps them to codes.

Notes on why each piece exists are kept in `learning.md`; the build schedule is `mini-kafka-7day.md`.
