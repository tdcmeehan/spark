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

import java.io.{BufferedReader, File, InputStreamReader}
import java.nio.charset.StandardCharsets.UTF_8
import java.util.concurrent.TimeUnit

import scala.jdk.CollectionConverters._

import org.scalactic.source.Position
import org.scalatest.Tag

import org.apache.spark.{SPARK_VERSION, SparkConf}
import org.apache.spark.sql.execution.datasources.v2.ffi.prototype.{Create, Request}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.Utils

/**
 * PROTOTYPE: the tests of [[NativeDataSourceSuite]], with the libraries called by a worker over
 * UdfWorker/Execute instead of the JVM. The suite starts one worker, `native-ds-worker` of
 * udf/worker/native-ds-prototype, which it builds with cargo if needed, and every session of the
 * suite uses it. The tests of what the worker does not do yet are canceled, with the reason.
 */
class NativeDataSourceRemoteWorkerSuite extends NativeDataSourceSuite {
  import NativeDataSourceRemoteWorkerSuite._

  private lazy val worker: Option[Worker] = startWorker()

  override protected def sparkConf: SparkConf = {
    val conf = super.sparkConf
    worker.foreach(w => conf.set(SQLConf.NATIVE_DATA_SOURCE_PROTOTYPE_REMOTE_WORKER.key, w.address))
    conf
  }

  override protected def canLoadLibraries: Boolean =
    NativeDataSourceTestUtils.compiler.isDefined && worker.isDefined

  override protected def librariesRequirement: String =
    "A C++ compiler and the worker, which cargo builds, are required."

  override protected def test(testName: String, testTags: Tag*)(testFun: => Any)(
      implicit pos: Position): Unit = {
    super.test(testName, testTags: _*) {
      notRun.get(testName).foreach(reason => cancel(s"Not run with a worker: $reason"))
      testFun
    }
  }

  test("a worker rejects the IDs of another worker process") {
    assume(canLoadLibraries, librariesRequirement)
    val other = startWorker().get
    try {
      val path = NativeDataSourceTestUtils.compile("test_native_datasource").getPath
      val library = RemoteNativeLibrary.load(worker.get.address, path, SPARK_VERSION)
      val otherLibrary = RemoteNativeLibrary.load(other.address, path, SPARK_VERSION)
      val source = library.createDataSource("native_range", Array.empty, Array.empty)
      try {
        val e = intercept[RuntimeException](otherLibrary.closeDataSource(source.handle))
        assert(e.getMessage.contains("has no Source handle"), e.getMessage)
      } finally {
        library.closeDataSource(source.handle)
      }
      val create = Request.newBuilder()
        .setCreate(Create.newBuilder().setLibrary(library.id).setName("native_range")).build()
      val e = intercept[RuntimeException](RemoteNativeWorker.call(other.address, path, create))
      val id = java.lang.Long.toUnsignedString(library.id)
      assert(e.getMessage.contains(s"has no library $id"), e.getMessage)
    } finally {
      other.stop()
    }
  }

  test("read a planned scan with the worker that planned it") {
    assume(canLoadLibraries, librariesRequirement)
    import NativeDataSourceTestUtils._
    val pkg = NativeDataSourcePackage.read(
      createPackage(compile("test_native_datasource"), dataSources("")))
    val options = new CaseInsensitiveStringMap(Map("end" -> "3", "partitions" -> "1").asJava)
    val planned = new NativeDataSource(pkg, "native_range", options)
      .batchPlanner(StructType.fromDDL("id BIGINT")).plan()
    // A task only gets the configuration of the session in a SQL execution: here, no worker runs.
    val conf = new SQLConf
    conf.setConfString(SQLConf.NATIVE_DATA_SOURCE_PROTOTYPE_REMOTE_WORKER.key, "127.0.0.1:1")
    val rows = SQLConf.withExistingConf(conf) {
      val reader = planned.readerFactory.createColumnarReader(planned.partitions.head)
      try {
        Iterator.continually(reader.next()).takeWhile(identity).map(_ => reader.get().numRows()).sum
      } finally {
        reader.close()
      }
    }
    assert(rows == 3)
  }

  override def afterAll(): Unit = {
    try super.afterAll() finally worker.foreach(_.stop())
  }

  private def startWorker(): Option[Worker] = {
    val crate = getWorkspaceFilePath("udf", "worker", "native-ds-prototype").toFile
    val binary = new File(crate, "target/release/native-ds-worker")
    if (!binary.canExecute && Utils.checkCommandAvailable("cargo")) {
      val build = new ProcessBuilder("cargo", "build", "--release")
        .directory(crate).inheritIO().start()
      assert(build.waitFor() == 0, "Failed to build the worker with cargo.")
    }
    Option.when(binary.canExecute) {
      val process = new ProcessBuilder(
        binary.getPath, "--listen", "127.0.0.1:0", "--exit-on-stdin-eof")
        .redirectError(ProcessBuilder.Redirect.INHERIT)
        .start()
      val line = new BufferedReader(new InputStreamReader(process.getInputStream, UTF_8)).readLine()
      assert(line != null && line.startsWith("LISTENING "), s"The worker did not start: $line")
      new Worker(process, line.stripPrefix("LISTENING "))
    }
  }
}

object NativeDataSourceRemoteWorkerSuite {
  private class Worker(process: Process, val address: String) {
    def stop(): Unit = {
      process.getOutputStream.close()
      if (!process.waitFor(10, TimeUnit.SECONDS)) {
        process.destroyForcibly()
      }
    }
  }

  private val writes = "it writes, and the worker only runs batch reads"
  private val streams = "it reads or writes a stream, and the worker only runs batch reads"
  private val alsoWrites = "after reads that pass with the worker, it writes or reads a " +
    "stream, and the worker only runs batch reads"

  /** The tests of the suite that do not run with a worker, with the reason. */
  private val notRun: Map[String, String] = Map(
    "batch write" -> writes,
    "tables in a catalog" -> alsoWrites,
    "abort a failed write" -> writes,
    "create no writer when computing the data to write fails" -> writes,
    "abort a write whose commit failed" -> writes,
    "abort a data writer whose commit failed" -> writes,
    "do not abort a data writer that committed with an invalid message" -> writes,
    "write with a session that is not the active one" -> writes,
    "find installed libraries automatically" -> alsoWrites,
    "pass the task to reads and data writers" -> alsoWrites,
    "reject NULL handles" -> alsoWrites,
    "abort a micro-batch whose write failed" -> streams,
    "streaming read and write" -> streams,
    "reject offsets that are not JSON on a single line" -> streams,
    "pass the ID of a streaming query to its stream writers" -> streams,
    "native data sources require Java 22 or later" ->
      "it checks the error of a JVM that calls a library itself")
}
