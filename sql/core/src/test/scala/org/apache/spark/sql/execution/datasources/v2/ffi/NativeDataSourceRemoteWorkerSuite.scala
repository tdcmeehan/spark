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

import org.scalactic.source.Position
import org.scalatest.Tag

import org.apache.spark.SparkConf
import org.apache.spark.sql.internal.SQLConf
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
  private val jvmOnly = "it does not call a library, so it does not depend on where libraries run"
  private def alsoWrites(stops: String) =
    s"after its batch reads, which pass with the worker, it $stops, and the worker only runs " +
      "batch reads"

  /** The tests of the suite that do not run with a worker, with the reason. */
  private val notRun: Map[String, String] = Map(
    "batch write" -> writes,
    "tables in a catalog" -> alsoWrites("inserts into a table"),
    "abort a failed write" -> writes,
    "create no writer when computing the data to write fails" -> writes,
    "abort a write whose commit failed" -> writes,
    "abort a data writer whose commit failed" -> writes,
    "do not abort a data writer that committed with an invalid message" -> writes,
    "write with a session that is not the active one" -> writes,
    "find installed libraries automatically" -> alsoWrites("writes"),
    "pass the task to reads and data writers" -> alsoWrites("writes"),
    "reject NULL handles" -> alsoWrites("reads a stream and writes"),
    "abort a micro-batch whose write failed" -> streams,
    "streaming read and write" -> streams,
    "reject offsets that are not JSON on a single line" -> streams,
    "pass the ID of a streaming query to its stream writers" -> streams,
    "parse manifests" -> jvmOnly,
    "platform names" -> jvmOnly,
    "fail clearly where the C data interface of Arrow Java does not load" -> jvmOnly,
    "native library path" -> jvmOnly,
    "tell missing functions from link errors" -> jvmOnly,
    "close the previous stream writer of a streaming query" -> jvmOnly,
    "the bindings match the layout of spark_datasource.h" -> jvmOnly,
    "distribute a package without an active session" -> jvmOnly,
    "add any file with the extension when native data sources are disabled" -> jvmOnly,
    "add a read-only package as a writable artifact" -> jvmOnly,
    "disable native data sources" -> jvmOnly,
    "native data sources require Java 22 or later" ->
      "it checks the error of a JVM that calls a library itself")
}
