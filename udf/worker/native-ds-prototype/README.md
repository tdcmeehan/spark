# native-ds-prototype (prototype, step 1)

A UDF worker that loads native data source libraries (`spark_datasource.h`, version 1) and serves
every call that Spark makes for a batch read, the driver's planning calls included, over the
existing `UdfWorker/Execute` stream. The protocol is unchanged: the calls are encoded in its opaque
fields. Writes and streaming are not implemented.

## How the calls map onto Execute

| C call | Execute stream |
|---|---|
| a driver call (`create`, `schema`, `create_reader`, `push_limit`, `prune_columns`, `partitions`, `serialize_reader`, `close_reader`, `close`) | `Init` with the `Request` in `udf.payload` → `InitResponse.data` with the `Reply` → `Finish` → `FinishResponse` |
| `read` of a partition | `Init` with the reader state and the partition → `InitResponse.data` with the schema of the stream → `Finish` → one `DataResponse` with the Arrow IPC messages of each batch → `FinishResponse.data` with the library's logs |
| `spark_ds_init` | `Load`, a driver call, once per library path in each worker |

- The encoding is `sql/core/src/main/protobuf/.../ffi/native_datasource_prototype.proto`, with the
  format tag `spark.native_datasource.prototype.v0`.
- A `Reply` carries what the C function returned, a failure included (`status` and `last_error`),
  so Spark reports it as it does in process. An `ExecutionError` is a failure of the worker or of a
  callback of a read's stream.
- Libraries and handles are IDs unique to the worker process. A call that reaches another worker,
  or one that restarted, fails with "The worker has no ..." rather than using another object.
- The worker calls `get_next` only once the engine sends `Finish`, because `GrpcWorkerSession`
  cancels a session on an `ErrorResponse` that comes before `process()`, and the error is lost.
- The driver decides where a scan's library runs when it plans the scan, and sends that to the
  executors with the reader state: a task only gets the SQL configuration of its session within a
  SQL execution, so a job such as `df.rdd.pipe(...)` would otherwise read in process.
- The JVM waits for the worker on threads that Spark does not interrupt. `GrpcWorkerSession`
  cancels its stream when its thread is interrupted, as `PipedRDD` does to its writer thread when
  its task completes, while Spark cannot interrupt a library in process, which only learns that its
  task was killed from `is_interrupted`.

The calls are mapped to messages in one place on each side, so a new version of the interface, such
as apache/spark#59931, changes those places only: `RemoteNativeLibrary.java` in Spark, and
`calls.rs` and `read.rs` here.

## Run

```
cargo build --release      # crates through the internal proxy
build/sbt "sql/testOnly *NativeDataSourceRemoteWorkerSuite"
target/release/native-ds-worker --listen 127.0.0.1:0   # prints LISTENING <address>
build/sbt "sql/Test/runMain org.apache.spark.sql.execution.datasources.v2.ffi.NativeDataSourceRemoteWorkerBenchmark <address>"
```

Spark uses a worker when `spark.sql.dataSource.native.prototype.remoteWorker` is `host:port`, through
`FixedAddressGrpcDispatcher` and the `GrpcWorkerSession` of `udf/worker/grpc`. The suite starts one
worker, which every caller uses.

## Shortcuts that step 2 replaces

- One worker at a fixed address, shared by every session and JVM, over plaintext TCP, with no
  lifecycle management: the test starts it.
- The worker loads a library at the path where Spark found it, so it must share the file system of
  every JVM. The JVM caches the worker's library ID for its lifetime, keyed by address, and never
  reloads it after a restart of the worker.
- Nothing routes the calls on a handle to the worker that holds it: there is only one.
- A read is decoded from Arrow IPC into Arrow Java vectors, then exported again as an Arrow C stream,
  so that `NativePartitionReader` is unchanged. Spark still needs the C data interface of Arrow Java.
- The checks that Spark makes on the arrays of a library run in the worker too (`checks.rs`), since
  Spark only sees the re-encoded batches.
- The library's logs are only returned for the thread of a call, at its end, and not at all for a
  read that is cancelled or fails: `CancelResponse` has no data.
- `is_interrupted` is answered from a `Cancel`, which the JVM sends once it sees the task killed,
  polling every 50 ms.
- The client's timeouts apply: 30 s for an `InitResponse`, so for any driver call, and 30 s between
  the batches of a read. The worker sends no heartbeat.
- Spark finds the structured error of a session by reflection, since `sql/core` does not depend on
  `udf-worker-grpc`.
