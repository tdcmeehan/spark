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

/**
 * The functions of the table {@code SparkDsLibrary} of a native data source library, as Spark
 * calls them: in process, with {@link NativeLibrary}, or in a worker, with the prototype
 * {@link RemoteNativeLibrary}. The pointers of the C interface are addresses in the JVM, and the
 * handles are opaque.
 */
abstract class NativeDataSourceLibrary {

  abstract int abiVersion();

  abstract boolean hasFunction(NativeLibrary.Function function);

  abstract NativeLibrary.DataSourceHandle createDataSource(
      String name, String[] optionKeys, String[] optionValues) throws Throwable;

  abstract void schema(long dataSource, long schemaAddress) throws Throwable;

  abstract long createReader(long dataSource, long schemaAddress) throws Throwable;

  abstract long createStreamReader(long dataSource, long schemaAddress) throws Throwable;

  abstract long createWriter(long dataSource, long schemaAddress, int mode, String queryId)
      throws Throwable;

  abstract void closeDataSource(long dataSource) throws Throwable;

  abstract boolean pushLimit(long reader, int limit) throws Throwable;

  abstract boolean pruneColumns(long reader, String[] columnNames) throws Throwable;

  abstract byte[][] partitions(long reader) throws Throwable;

  abstract byte[] serializeReader(long reader) throws Throwable;

  abstract void closeReader(long reader) throws Throwable;

  abstract String initialOffset(long streamReader) throws Throwable;

  abstract String latestOffset(long streamReader) throws Throwable;

  abstract byte[][] streamPartitions(long streamReader, String start, String end)
      throws Throwable;

  abstract byte[] serializeStreamReader(long streamReader) throws Throwable;

  abstract void commitOffset(long streamReader, String end) throws Throwable;

  abstract void closeStreamReader(long streamReader) throws Throwable;

  abstract void read(NativeLibrary.Task task, byte[] readerState, byte[] partition,
      long streamAddress) throws Throwable;

  abstract byte[] serializeWriter(long writer) throws Throwable;

  abstract void commit(long writer, long epochId, byte[][] messages) throws Throwable;

  abstract void abort(long writer, long epochId, byte[][] messages) throws Throwable;

  abstract void closeWriter(long writer) throws Throwable;

  abstract long createDataWriter(NativeLibrary.Task task, byte[] writerState, long epochId)
      throws Throwable;

  abstract void write(long dataWriter, long arrayAddress, long schemaAddress) throws Throwable;

  abstract byte[] commitDataWriter(long dataWriter, Runnable committed) throws Throwable;

  abstract void abortDataWriter(long dataWriter) throws Throwable;

  abstract void closeDataWriter(long dataWriter) throws Throwable;
}
