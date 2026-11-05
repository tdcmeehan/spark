// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! PROTOTYPE: a UDF worker that serves the native data sources of spark_datasource.h over
//! UdfWorker/Execute. See native_datasource_prototype.proto in sql/core for the encoding.
//!
//! Usage: native-ds-worker [--listen HOST:PORT] [--exit-on-stdin-eof]
//! Prints `LISTENING <address>` once it accepts connections.

mod abi;
mod calls;
mod checks;
mod host;
mod ipc;
mod read;
mod service;

pub mod proto {
    pub mod udf {
        tonic::include_proto!("org.apache.spark.udf.worker");
    }
    pub mod ds {
        tonic::include_proto!("spark.native_datasource.prototype.v0");
    }
}

use std::io::{Read as _, Write as _};
use std::sync::Arc;

use tokio::sync::Notify;
use tonic::transport::server::TcpIncoming;
use tonic::transport::Server;

use crate::proto::udf::udf_worker_server::UdfWorkerServer;

const MAX_MESSAGE_SIZE: usize = 128 * 1024 * 1024;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut listen = "127.0.0.1:0".to_string();
    let mut exit_on_stdin_eof = false;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--listen" => listen = args.next().ok_or("--listen needs HOST:PORT")?,
            "--exit-on-stdin-eof" => exit_on_stdin_eof = true,
            other => return Err(format!("Unknown argument {other}").into()),
        }
    }
    if exit_on_stdin_eof {
        // Lets the process that started the worker stop it by exiting, even if it crashes.
        std::thread::spawn(|| {
            let mut buffer = [0u8; 64];
            while matches!(std::io::stdin().read(&mut buffer), Ok(n) if n > 0) {}
            std::process::exit(0);
        });
    }
    let incoming = TcpIncoming::bind(listen.parse()?)?.with_nodelay(Some(true));
    println!("LISTENING {}", incoming.local_addr()?);
    std::io::stdout().flush()?;
    let shutdown = Arc::new(Notify::new());
    let service = UdfWorkerServer::new(service::Worker::new(shutdown.clone()))
        .max_decoding_message_size(MAX_MESSAGE_SIZE)
        .max_encoding_message_size(MAX_MESSAGE_SIZE);
    Server::builder()
        .add_service(service)
        .serve_with_incoming_shutdown(incoming, async move { shutdown.notified().await })
        .await?;
    Ok(())
}
