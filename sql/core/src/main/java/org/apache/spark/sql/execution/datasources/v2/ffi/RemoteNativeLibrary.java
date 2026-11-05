/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.datasources.v2.ffi;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;

import com.google.protobuf.ByteString;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;

import org.apache.spark.sql.execution.datasources.v2.ffi.prototype.Close;
import org.apache.spark.sql.execution.datasources.v2.ffi.prototype.CloseReader;
import org.apache.spark.sql.execution.datasources.v2.ffi.prototype.Create;
import org.apache.spark.sql.execution.datasources.v2.ffi.prototype.CreateReader;
import org.apache.spark.sql.execution.datasources.v2.ffi.prototype.Library;
import org.apache.spark.sql.execution.datasources.v2.ffi.prototype.Load;
import org.apache.spark.sql.execution.datasources.v2.ffi.prototype.Loaded;
import org.apache.spark.sql.execution.datasources.v2.ffi.prototype.Partitions;
import org.apache.spark.sql.execution.datasources.v2.ffi.prototype.PruneColumns;
import org.apache.spark.sql.execution.datasources.v2.ffi.prototype.PushLimit;
import org.apache.spark.sql.execution.datasources.v2.ffi.prototype.Read;
import org.apache.spark.sql.execution.datasources.v2.ffi.prototype.Reply;
import org.apache.spark.sql.execution.datasources.v2.ffi.prototype.Request;
import org.apache.spark.sql.execution.datasources.v2.ffi.prototype.SerializeReader;
import org.apache.spark.sql.execution.datasources.v2.ffi.prototype.TaskContext;
import org.apache.spark.sql.execution.datasources.v2.ffi.NativeLibrary.Function;
import org.apache.spark.sql.util.ArrowUtils;

/**
 * PROTOTYPE: a native data source library that a worker loads and calls, at a fixed
 * {@code host:port}, rather than the JVM. Each call is one UdfWorker/Execute stream, which
 * {@link RemoteNativeWorker} makes, with a message of {@code native_datasource_prototype.proto}
 * for each function of the table: this class maps the functions to the messages. The handles are
 * those of the worker, which keeps them across the streams. A failure of the library is reported
 * as in process: the worker returns its status and last_error.
 * <p>
 * Only batch reads are supported. Spark calls the other functions of a table that declares them,
 * which then fail.
 */
final class RemoteNativeLibrary extends NativeDataSourceLibrary {

  private final String address;
  // The path of the library, which the worker loads, for the logs of the library.
  private final String path;
  // The ID of the library in the worker.
  final long id;
  private final int abiVersion;
  private final EnumSet<Function> functions = EnumSet.noneOf(Function.class);

  private RemoteNativeLibrary(String address, String path, Library library) {
    this.address = address;
    this.path = path;
    this.id = library.getId();
    this.abiVersion = library.getAbiVersion();
    for (Function function : Function.values()) {
      if (library.getFunctionsList().contains(function.fieldName)) {
        functions.add(function);
      }
    }
  }

  /**
   * Has the worker load the library at the given path, and call {@code spark_ds_init}, as
   * {@link NativeLibrary#load} does in process.
   *
   * @throws NativeLibrary.LoadException if the library cannot be used
   */
  static RemoteNativeLibrary load(String address, String path, String sparkVersion) {
    Request request = Request.newBuilder()
      .setLoad(Load.newBuilder().setPath(path).setSparkVersion(sparkVersion))
      .build();
    Reply reply;
    try {
      reply = RemoteNativeWorker.call(address, path, request);
    } catch (RuntimeException e) {
      throw new NativeLibrary.LoadException(e.getMessage(), e);
    }
    Loaded loaded = reply.getLoaded();
    switch (loaded.getOutcomeCase()) {
      case LIBRARY -> {
        long size = loaded.getLibrary().getTableSize();
        if (size < 16) {
          throw new NativeLibrary.LoadException(
            "spark_ds_init returned a function table with a size of " + size + " bytes.", null,
            true);
        }
        return new RemoteNativeLibrary(address, path, loaded.getLibrary());
      }
      case CANNOT_OPEN -> throw new NativeLibrary.LoadException(loaded.getCannotOpen(), null);
      case NO_INIT -> throw new NativeLibrary.LoadException(
        "The library does not export the function spark_ds_init.", null, true);
      case INIT_FAILED -> throw new NativeLibrary.LoadException(
        "spark_ds_init failed with the error code " + reply.getStatus() + ".", null, true);
      case NO_TABLE -> throw new NativeLibrary.LoadException(
        "spark_ds_init returned no function table.", null, true);
      default -> throw new NativeLibrary.LoadException(
        "The native data source worker at " + address + " did not load the library.", null);
    }
  }

  private Reply call(Function function, Request request) {
    if (!functions.contains(function)) {
      throw new NativeLibrary.MissingFunctionException(function.fieldName);
    }
    Reply reply = RemoteNativeWorker.call(address, path, request);
    check(reply, function);
    return reply;
  }

  private static void check(Reply reply, Function function) {
    if (reply.getStatus() != 0) {
      String message = reply.hasLastError() ? reply.getLastError() : "";
      throw new RuntimeException(message.isEmpty()
        ? function.fieldName + " failed with the error code " + reply.getStatus() + "."
        : message);
    }
  }

  private static long handle(long handle, Function function) {
    if (handle == 0) {
      throw new IllegalStateException(function.fieldName + " returned a NULL handle.");
    }
    return handle;
  }

  private static byte[] single(List<ByteString> values, Function function) {
    if (values.size() > 1) {
      throw new IllegalStateException(
        function.fieldName + " returned " + values.size() + " values instead of one.");
    }
    return values.isEmpty() ? null : values.get(0).toByteArray();
  }

  private static UnsupportedOperationException unsupported(Function function) {
    return new UnsupportedOperationException("The prototype of native data sources in a worker " +
      "only supports batch reads, so it cannot call " + function.fieldName + ".");
  }

  @Override
  int abiVersion() {
    return abiVersion;
  }

  @Override
  boolean hasFunction(Function function) {
    return functions.contains(function);
  }

  @Override
  NativeLibrary.DataSourceHandle createDataSource(
      String name, String[] optionKeys, String[] optionValues) {
    Reply reply = call(Function.CREATE, Request.newBuilder().setCreate(Create.newBuilder()
      .setLibrary(id)
      .setName(name)
      .addAllOptionKeys(Arrays.asList(optionKeys))
      .addAllOptionValues(Arrays.asList(optionValues))).build());
    return new NativeLibrary.DataSourceHandle(
      handle(reply.getCreated().getSource(), Function.CREATE),
      reply.getCreated().getCapabilities());
  }

  @Override
  void schema(long dataSource, long schemaAddress) {
    Reply reply = call(Function.SCHEMA, Request.newBuilder().setSchema(
      org.apache.spark.sql.execution.datasources.v2.ffi.prototype.Schema.newBuilder()
        .setSource(dataSource)).build());
    Data.exportSchema(ArrowUtils.rootAllocator(), RemoteNativeWorker.schemaFrom(reply.getSchema()),
      null, ArrowSchema.wrap(schemaAddress));
  }

  @Override
  long createReader(long dataSource, long schemaAddress) {
    // Released by the import, as when a library moves the schema.
    ByteString schema = RemoteNativeWorker.schemaMessage(
      Data.importSchema(ArrowUtils.rootAllocator(), ArrowSchema.wrap(schemaAddress), null));
    Reply reply = call(Function.CREATE_READER, Request.newBuilder().setCreateReader(
      CreateReader.newBuilder().setSource(dataSource).setSchema(schema)).build());
    return handle(reply.getHandle(), Function.CREATE_READER);
  }

  @Override
  void closeDataSource(long dataSource) {
    call(Function.CLOSE,
      Request.newBuilder().setClose(Close.newBuilder().setSource(dataSource)).build());
  }

  @Override
  boolean pushLimit(long reader, int limit) {
    return call(Function.PUSH_LIMIT, Request.newBuilder().setPushLimit(
      PushLimit.newBuilder().setReader(reader).setLimit(limit)).build()).getFlag();
  }

  @Override
  boolean pruneColumns(long reader, String[] columnNames) {
    return call(Function.PRUNE_COLUMNS, Request.newBuilder().setPruneColumns(
      PruneColumns.newBuilder().setReader(reader).addAllColumns(Arrays.asList(columnNames)))
      .build()).getFlag();
  }

  @Override
  byte[][] partitions(long reader) {
    return call(Function.PARTITIONS, Request.newBuilder().setPartitions(
      Partitions.newBuilder().setReader(reader)).build())
      .getSunk().getValuesList().stream().map(ByteString::toByteArray).toArray(byte[][]::new);
  }

  @Override
  byte[] serializeReader(long reader) {
    return single(call(Function.SERIALIZE_READER, Request.newBuilder().setSerializeReader(
      SerializeReader.newBuilder().setReader(reader)).build()).getSunk().getValuesList(),
      Function.SERIALIZE_READER);
  }

  @Override
  void closeReader(long reader) {
    call(Function.CLOSE_READER,
      Request.newBuilder().setCloseReader(CloseReader.newBuilder().setReader(reader)).build());
  }

  /**
   * Starts the read on the worker, and initializes the stream at the given address with the
   * batches that it streams. The stream stays uninitialized if the library did not initialize its
   * own, for Spark to report it as in process.
   */
  @Override
  void read(NativeLibrary.Task task, byte[] readerState, byte[] partition, long streamAddress) {
    if (!functions.contains(Function.READ)) {
      throw new NativeLibrary.MissingFunctionException(Function.READ.fieldName);
    }
    Request request = Request.newBuilder().setRead(Read.newBuilder()
      .setLibrary(id)
      .setTask(TaskContext.newBuilder()
        .setStageId(task.stageId)
        .setStageAttemptNumber(task.stageAttemptNumber)
        .setPartitionId(task.partitionId)
        .setAttemptNumber(task.attemptNumber)
        .setTaskAttemptId(task.taskAttemptId))
      .setReaderState(ByteString.copyFrom(readerState))
      .setPartition(ByteString.copyFrom(partition)))
      .build();
    RemoteRead read = RemoteNativeWorker.openRead(address, path, request);
    try {
      check(read.reply(), Function.READ);
      if (!read.reply().getOpened().getInitialized()) {
        read.close();
        return;
      }
      read.exportStream(streamAddress, task.interrupted);
    } catch (Throwable e) {
      read.close();
      throw e;
    }
  }

  @Override
  long createStreamReader(long dataSource, long schemaAddress) {
    throw unsupported(Function.CREATE_STREAM_READER);
  }

  @Override
  long createWriter(long dataSource, long schemaAddress, int mode, String queryId) {
    throw unsupported(Function.CREATE_WRITER);
  }

  @Override
  String initialOffset(long streamReader) {
    throw unsupported(Function.INITIAL_OFFSET);
  }

  @Override
  String latestOffset(long streamReader) {
    throw unsupported(Function.LATEST_OFFSET);
  }

  @Override
  byte[][] streamPartitions(long streamReader, String start, String end) {
    throw unsupported(Function.STREAM_PARTITIONS);
  }

  @Override
  byte[] serializeStreamReader(long streamReader) {
    throw unsupported(Function.SERIALIZE_STREAM_READER);
  }

  @Override
  void commitOffset(long streamReader, String end) {
    throw unsupported(Function.COMMIT_OFFSET);
  }

  @Override
  void closeStreamReader(long streamReader) {
    throw unsupported(Function.CLOSE_STREAM_READER);
  }

  @Override
  byte[] serializeWriter(long writer) {
    throw unsupported(Function.SERIALIZE_WRITER);
  }

  @Override
  void commit(long writer, long epochId, byte[][] messages) {
    throw unsupported(Function.COMMIT);
  }

  @Override
  void abort(long writer, long epochId, byte[][] messages) {
    throw unsupported(Function.ABORT);
  }

  @Override
  void closeWriter(long writer) {
    throw unsupported(Function.CLOSE_WRITER);
  }

  @Override
  long createDataWriter(NativeLibrary.Task task, byte[] writerState, long epochId) {
    throw unsupported(Function.CREATE_DATA_WRITER);
  }

  @Override
  void write(long dataWriter, long arrayAddress, long schemaAddress) {
    throw unsupported(Function.WRITE);
  }

  @Override
  byte[] commitDataWriter(long dataWriter, Runnable committed) {
    throw unsupported(Function.COMMIT_DATA_WRITER);
  }

  @Override
  void abortDataWriter(long dataWriter) {
    throw unsupported(Function.ABORT_DATA_WRITER);
  }

  @Override
  void closeDataWriter(long dataWriter) {
    throw unsupported(Function.CLOSE_DATA_WRITER);
  }
}
