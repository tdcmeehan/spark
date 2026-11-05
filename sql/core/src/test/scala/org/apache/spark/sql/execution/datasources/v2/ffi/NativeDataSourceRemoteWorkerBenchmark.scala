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
package org.apache.spark.sql.execution.datasources.v2.ffi

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

import org.apache.spark.SPARK_VERSION
import org.apache.spark.sql.execution.datasources.v2.columnar.ColumnarPlannedBatch
import org.apache.spark.sql.execution.datasources.v2.ffi.prototype.{Load, Request}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * PROTOTYPE: measures the calls of the test library of the native data sources in process,
 * through FFM, and in a worker, over UdfWorker/Execute. Needs Java 22 or later, a C++ compiler,
 * and a worker started with `native-ds-worker --listen 127.0.0.1:0`, whose address it takes:
 * {{{
 *   build/sbt "sql/Test/runMain \
 *     org.apache.spark.sql.execution.datasources.v2.ffi.NativeDataSourceRemoteWorkerBenchmark \
 *     127.0.0.1:<port>"
 * }}}
 * Prints Markdown tables.
 */
// scalastyle:off println
object NativeDataSourceRemoteWorkerBenchmark {
  import NativeDataSourceTestUtils._

  private val Warmup = 200
  private val Iterations = 1000

  private type Samples = Map[String, ArrayBuffer[Long]]

  private val schema = StructType.fromDDL(
    "id BIGINT, mod INT, name STRING, value DOUBLE, flag BOOLEAN, day DATE, ts TIMESTAMP")

  def main(args: Array[String]): Unit = {
    val modes = Seq("FFM" -> None, "Worker" -> Some(args.head))
    val library = compile("test_native_datasource")
    val pkg = NativeDataSourcePackage.read(createPackage(library, dataSources("")))
    println(s"Java ${Runtime.version()}, ${Runtime.getRuntime.availableProcessors()} cores, " +
      s"worker at ${args.head}. Percentiles over $Iterations calls, after $Warmup.\n")
    driverCalls(pkg, modes, library.getPath)
    partitionReads(pkg, modes)
    batchReads(pkg, modes)
  }

  private def inMode[T](worker: Option[String])(f: => T): T = {
    val conf = new SQLConf
    worker.foreach(conf.setConfString(SQLConf.NATIVE_DATA_SOURCE_PROTOTYPE_REMOTE_WORKER.key, _))
    SQLConf.withExistingConf(conf)(f)
  }

  private def microseconds(nanos: Double): String = f"${nanos / 1000}%.1f"

  private def printPercentiles(title: String, rows: Seq[String], ffm: Samples, worker: Samples)
      : Unit = {
    def percentiles(samples: ArrayBuffer[Long]): String = if (samples.isEmpty) " | " else {
      val sorted = samples.toArray.sorted
      def at(p: Double) = sorted(math.min(sorted.length - 1, (p * sorted.length).toInt)).toDouble
      s"${microseconds(at(0.5))} | ${microseconds(at(0.95))}"
    }
    println(s"| $title | FFM p50 (us) | FFM p95 (us) | Worker p50 (us) | Worker p95 (us) |")
    println("|---|---:|---:|---:|---:|")
    rows.foreach { row =>
      println(s"| $row | ${percentiles(ffm(row))} | ${percentiles(worker(row))} |")
    }
    println()
  }

  /** Each call that plans a scan, through the library as the wrapper calls it, in its order. */
  private def driverCalls(
      pkg: NativeDataSourcePackage,
      modes: Seq[(String, Option[String])],
      path: String): Unit = {
    val noOp = "Load of a loaded library (a stream without a call)"
    val calls = Seq("create", "schema", "create_reader", "push_limit", "prune_columns",
      "partitions", "serialize_reader", "close_reader", "close", noOp)
    val load = Request.newBuilder()
      .setLoad(Load.newBuilder().setPath(path).setSparkVersion(SPARK_VERSION)).build()
    val samples = modes.map { case (mode, worker) =>
      val times = calls.map(_ -> ArrayBuffer.empty[Long]).toMap
      inMode(worker) {
        val library = NativeLibraries.get(pkg)
        (0 until Warmup + Iterations).foreach { i =>
          def measure[T](call: String)(f: => T): T = {
            val start = System.nanoTime()
            val result = f
            if (i >= Warmup) times(call) += System.nanoTime() - start
            result
          }
          val source = measure("create") {
            library.createDataSource("native_range", Array("end", "partitions"), Array("1000", "4"))
          }
          measure("schema")(NativeArrow.importSchema(library.schema(source.handle, _)))
          val reader = measure("create_reader") {
            NativeArrow.exportSchema(schema)(library.createReader(source.handle, _))
          }
          measure("push_limit")(library.pushLimit(reader, 10))
          measure("prune_columns")(library.pruneColumns(reader, Array("id", "name")))
          measure("partitions")(library.partitions(reader))
          measure("serialize_reader")(library.serializeReader(reader))
          measure("close_reader")(library.closeReader(reader))
          measure("close")(library.closeDataSource(source.handle))
          worker.foreach(address => measure(noOp)(RemoteNativeWorker.call(address, path, load)))
        }
      }
      mode -> times
    }.toMap
    printPercentiles("Driver call", calls, samples("FFM"), samples("Worker"))
  }

  private def plan(
      pkg: NativeDataSourcePackage,
      rows: Long,
      batchSize: Int): ColumnarPlannedBatch = {
    val options = new CaseInsensitiveStringMap(Map("end" -> rows.toString, "partitions" -> "1",
      "batch_size" -> batchSize.toString).asJava)
    new NativeDataSource(pkg, "native_range", options).batchPlanner(schema).plan()
  }

  /** Opens a partition of 1 row, reads it to its end and closes it, in each task. */
  private def partitionReads(
      pkg: NativeDataSourcePackage,
      modes: Seq[(String, Option[String])]): Unit = {
    val (open, rest) = ("open (read, get_schema)", "read 1 batch to the end, and close")
    val samples = modes.map { case (mode, worker) =>
      val times = Seq(open, rest).map(_ -> ArrayBuffer.empty[Long]).toMap
      inMode(worker) {
        val planned = plan(pkg, rows = 1, batchSize = 1)
        (0 until Warmup + Iterations).foreach { i =>
          val start = System.nanoTime()
          val reader = planned.readerFactory.createColumnarReader(planned.partitions.head)
          val opened = System.nanoTime()
          while (reader.next()) {}
          reader.close()
          if (i >= Warmup) {
            times(open) += opened - start
            times(rest) += System.nanoTime() - opened
          }
        }
      }
      mode -> times
    }.toMap
    printPercentiles("Read of a partition", Seq(open, rest), samples("FFM"), samples("Worker"))
  }

  /** Reads a partition in batches of each size, the best of 3 reads after 1. */
  private def batchReads(
      pkg: NativeDataSourcePackage,
      modes: Seq[(String, Option[String])]): Unit = {
    println("| Rows per batch | Batches | FFM us/batch | Worker us/batch | Overhead us/batch | " +
      "FFM ns/row | Worker ns/row |")
    println("|---:|---:|---:|---:|---:|---:|---:|")
    Seq(1 -> 20000, 1024 -> 2000, 65536 -> 100).foreach { case (batchSize, batches) =>
      val rows = batchSize.toLong * batches
      val best = modes.map { case (mode, worker) =>
        val times = inMode(worker) {
          val planned = plan(pkg, rows, batchSize)
          (0 until 4).map { _ =>
            val start = System.nanoTime()
            var read = 0
            val reader = planned.readerFactory.createColumnarReader(planned.partitions.head)
            try {
              while (reader.next()) read += 1
            } finally {
              reader.close()
            }
            assert(read == batches, s"read $read batches instead of $batches")
            System.nanoTime() - start
          }
        }
        mode -> times.drop(1).min.toDouble
      }.toMap
      val (ffm, worker) = (best("FFM"), best("Worker"))
      println(s"| $batchSize | $batches | ${microseconds(ffm / batches)} | " +
        s"${microseconds(worker / batches)} | ${microseconds((worker - ffm) / batches)} | " +
        f"${ffm / rows}%.1f | ${worker / rows}%.1f |")
    }
    println()
  }
}
// scalastyle:on println
