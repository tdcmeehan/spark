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

import java.io.{ByteArrayOutputStream, IOException, PrintWriter}
import java.nio.ByteBuffer
import java.nio.channels.{Channels, ReadableByteChannel}
import java.util.concurrent.{ConcurrentHashMap, ScheduledFuture, TimeUnit}
import java.util.function.BooleanSupplier

import scala.jdk.CollectionConverters._
import scala.util.Try
import scala.util.control.NonFatal

import com.google.protobuf.ByteString
import org.apache.arrow.c.{ArrowArrayStream, Data}
import org.apache.arrow.vector.ipc.{ArrowStreamReader, ReadChannel, WriteChannel}
import org.apache.arrow.vector.ipc.message.MessageSerializer
import org.apache.arrow.vector.types.pojo.Schema

import org.apache.spark.sql.execution.datasources.v2.ffi.prototype.{LogRecord, Opened, ReadEnd, Reply, Request}
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.udf.worker.{Cancel, ExecutionError, Init, InitResponse, UdfPayload, UDFWorkerDataFormat}
import org.apache.spark.udf.worker.core.{Termination, WorkerDispatcher, WorkerLogger, WorkerSession}
import org.apache.spark.util.{SparkClassUtils, SparkUDFWorkerLogger, ThreadUtils}

/**
 * PROTOTYPE: the Execute streams of a [[RemoteNativeLibrary]] to a worker that something else
 * started, at a fixed `host:port`. Every caller in the JVM shares one channel to it, through the
 * [[WorkerSession]] of the UDF worker framework, so that the handles of the worker are found by
 * the later calls. See `native_datasource_prototype.proto` for what a stream carries.
 */
private[ffi] object RemoteNativeWorker {
  val Format = "spark.native_datasource.prototype.v0"

  // In udf-worker-grpc, which the engine does not depend on, as SparkEnv's dispatcher factory.
  private val DispatcherClass = "org.apache.spark.udf.worker.grpc.FixedAddressGrpcDispatcher"

  private val dispatchers = new ConcurrentHashMap[String, WorkerDispatcher]()

  private[ffi] lazy val interruptions =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("native-ds-remote-interruptions")
  private[ffi] lazy val cancellations =
    ThreadUtils.newDaemonCachedThreadPool("native-ds-remote-cancel")

  private def dispatcher(address: String): WorkerDispatcher = {
    dispatchers.computeIfAbsent(address, _ => {
      SparkClassUtils.classForName[WorkerDispatcher](DispatcherClass)
        .getConstructor(classOf[String], classOf[WorkerLogger])
        .newInstance(address, new SparkUDFWorkerLogger)
    })
  }

  private def initMessage(request: Request): Init = {
    Init.newBuilder()
      .setProtocolVersion(1)
      .setDataFormat(UDFWorkerDataFormat.ARROW)
      .setUdf(UdfPayload.newBuilder()
        .setFormat(Format)
        .setPayload(request.toByteString)
        .setName(request.getCallCase.name.toLowerCase(java.util.Locale.ROOT)))
      .build()
  }

  private def initialize(session: WorkerSession, address: String, request: Request): Reply = {
    val response: InitResponse = try {
      session.init(initMessage(request))
    } catch {
      case NonFatal(e) => throw failure(address, e)
    }
    Reply.parseFrom(response.getData)
  }

  /**
   * Makes a call whose Reply completes it: Init, InitResponse, Finish, FinishResponse. Logs what
   * the library logged during the call.
   */
  def call(address: String, path: String, request: Request): Reply = {
    val session = dispatcher(address).createSession(None)
    try {
      val reply = initialize(session, address, request)
      log(path, reply.getLogsList)
      try {
        session.process(Iterator.empty).foreach { _ =>
          throw new IllegalStateException("The worker returned data for a call.")
        }
      } catch {
        case NonFatal(e) => throw failure(address, e)
      }
      reply
    } finally {
      session.close()
    }
  }

  /** Starts a read: the stream stays open for its batches until [[RemoteRead.close]]. */
  def openRead(address: String, path: String, request: Request): RemoteRead = {
    val session = dispatcher(address).createSession(None)
    val reply = try {
      initialize(session, address, request)
    } catch {
      case e: Throwable =>
        session.close()
        throw e
    }
    log(path, reply.getLogsList)
    new RemoteRead(address, path, session, reply)
  }

  /** A failure of the worker or of the stream, with the message of the error of the worker. */
  private[ffi] def failure(address: String, e: Throwable): RuntimeException = {
    new RuntimeException(message(address, e), e)
  }

  private[ffi] def message(address: String, e: Throwable): String = executionError(e) match {
    case Some(error) if error.hasUser => error.getUser.getMessage
    case Some(error) if error.hasWorker =>
      s"The native data source worker at $address failed: ${error.getWorker.getMessage}"
    case Some(error) if error.hasProtocol =>
      s"The native data source worker at $address reported a protocol error: " +
        error.getProtocol.getMessage
    case _ => s"The native data source worker at $address failed: ${Option(e.getMessage)
      .getOrElse(e.toString)}"
  }

  // The structured error of the stream, which the gRPC session keeps on its exception. The
  // engine cannot name that class: the session is only typed as a WorkerSession here.
  private def executionError(e: Throwable): Option[ExecutionError] = {
    Iterator.iterate(e)(_.getCause).takeWhile(_ != null).flatMap { t =>
      Try(t.getClass.getMethod("executionError").invoke(t)).toOption.collect {
        case error: ExecutionError => error
      }
    }.nextOption()
  }

  private[ffi] def log(path: String, logs: java.util.List[LogRecord]): Unit = {
    logs.asScala.foreach(record => NativeLibrary.log(path, record.getLevel, record.getMessage))
  }

  def schemaMessage(schema: Schema): ByteString = {
    val out = new ByteArrayOutputStream()
    MessageSerializer.serialize(new WriteChannel(Channels.newChannel(out)), schema)
    ByteString.copyFrom(out.toByteArray)
  }

  def schemaFrom(message: ByteString): Schema = {
    MessageSerializer.deserializeSchema(new ReadChannel(new MessageChannel(message)))
  }
}

/**
 * An open read. [[exportStream]] initializes an Arrow C stream at an address, as the read of a
 * library in process does, with the batches that the worker streams.
 */
private[ffi] class RemoteRead(
    address: String,
    path: String,
    session: WorkerSession,
    val reply: Reply) {
  import RemoteNativeWorker._
  @volatile private var watch: Option[ScheduledFuture[_]] = None

  def exportStream(streamAddress: Long, interrupted: BooleanSupplier): Unit = {
    val allocator =
      ArrowUtils.rootAllocator.newChildAllocator("remote native data source read", 0, Long.MaxValue)
    val channel = new RemoteReadChannel(reply.getOpened, this, allocator)
    Data.exportArrayStream(allocator, new ArrowStreamReader(channel, allocator),
      ArrowArrayStream.wrap(streamAddress))
    // A library learns that its task was killed from is_interrupted, which the worker answers
    // from the Cancel of the stream: Spark cannot interrupt the worker otherwise.
    watch = Some(interruptions.scheduleWithFixedDelay(() => {
      if (interrupted.getAsBoolean) {
        watch.foreach(_.cancel(false))
        cancellations.execute(() => cancel("the task was killed"))
      }
    }, 50, 50, TimeUnit.MILLISECONDS))
  }

  private[ffi] lazy val responses = session.process(Iterator.empty)

  private def cancel(reason: String): Termination =
    session.close(() => Cancel.newBuilder().setReason(reason).build())

  private[ffi] def nextBatch(): Option[ByteString] = {
    try {
      if (responses.hasNext) Some(responses.next().getData) else None
    } catch {
      case NonFatal(e) => throw new RemoteReadException(message(address, e), e)
    }
  }

  def close(): Unit = {
    watch.foreach(_.cancel(false))
    cancel("the read is closed") match {
      case Termination.Finished(response) if response.hasData =>
        log(path, ReadEnd.parseFrom(response.getData).getLogsList)
      case _ =>
    }
  }
}

/**
 * The bytes that Arrow reads as one IPC stream: the schema of the read, then the batches, one
 * DataResponse each. Not interruptible, unlike a channel over an InputStream, which would close
 * the stream on the thread that interrupts the task.
 */
private[ffi] class RemoteReadChannel(
    opened: Opened,
    read: RemoteRead,
    allocator: org.apache.arrow.memory.BufferAllocator)
  extends ReadableByteChannel {

  private var current: ByteBuffer = opened.getSchema.asReadOnlyByteBuffer()
  private var open = true

  override def read(dst: ByteBuffer): Int = {
    if (opened.hasSchemaError) {
      throw new RemoteReadException(opened.getSchemaError)
    }
    while (!current.hasRemaining) {
      read.nextBatch() match {
        case Some(messages) => current = messages.asReadOnlyByteBuffer()
        case None => return -1
      }
    }
    val n = math.min(dst.remaining, current.remaining)
    val chunk = current.slice()
    chunk.limit(n)
    dst.put(chunk)
    current.position(current.position() + n)
    n
  }

  override def isOpen: Boolean = open

  override def close(): Unit = {
    if (open) {
      open = false
      try read.close() finally allocator.close()
    }
  }
}

/**
 * A failure of a remote read. Arrow reports the failure of a stream that it exports with its
 * stack trace, so this one prints only its message: the error of the library, as Spark reports
 * the error of a stream in process.
 */
private[ffi] class RemoteReadException(message: String, cause: Throwable = null)
  extends IOException(message, cause) {
  override def printStackTrace(s: PrintWriter): Unit = s.print(message)
}

/** The bytes of a message, for Arrow to read. */
private class MessageChannel(message: ByteString) extends ReadableByteChannel {
  private val buffer = message.asReadOnlyByteBuffer()
  override def read(dst: ByteBuffer): Int = {
    if (!buffer.hasRemaining) {
      -1
    } else {
      val n = math.min(dst.remaining, buffer.remaining)
      val chunk = buffer.slice()
      chunk.limit(n)
      dst.put(chunk)
      buffer.position(buffer.position() + n)
      n
    }
  }
  override def isOpen: Boolean = true
  override def close(): Unit = ()
}
