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
package org.apache.spark.udf.worker.grpc

import java.util.concurrent.TimeUnit

import io.grpc.ManagedChannel
import io.grpc.netty.NettyChannelBuilder

import org.apache.spark.annotation.Experimental
import org.apache.spark.udf.worker.UDFWorkerSpecification
import org.apache.spark.udf.worker.core.{WorkerDispatcher, WorkerHandle, WorkerLogger, WorkerSecurityScope, WorkerSession}

/**
 * :: Experimental ::
 * PROTOTYPE: sessions on a single worker that something else started and keeps running, at a
 * fixed TCP address. All the sessions share the worker, which is what lets a worker keep state
 * across them. Spark loads it by name, as [[DirectDispatcherFactory]], so that the engine does not
 * depend on gRPC.
 *
 * @param address `host:port` of the worker
 */
@Experimental
class FixedAddressGrpcDispatcher(address: String, logger: WorkerLogger) extends WorkerDispatcher {

  private val channel: ManagedChannel = NettyChannelBuilder
    .forTarget(address)
    .maxInboundMessageSize(GrpcWorkerChannel.MAX_INBOUND_MESSAGE_SIZE)
    .usePlaintext()
    .build()

  // Nothing to release or recycle: the worker outlives the sessions and the dispatcher.
  private val handle = new WorkerHandle {
    override def id: String = address
    override def markInvalid(): Unit =
      logger.warn(s"A session left the shared worker at $address in an unknown state.")
    override def releaseSession(): Unit = ()
  }

  override def workerSpec: UDFWorkerSpecification = UDFWorkerSpecification.getDefaultInstance

  override def createSession(securityScope: Option[WorkerSecurityScope]): WorkerSession =
    new GrpcWorkerSession(handle, channel, logger)

  override def close(): Unit = {
    channel.shutdown()
    if (!channel.awaitTermination(5, TimeUnit.SECONDS)) {
      channel.shutdownNow()
    }
  }
}
