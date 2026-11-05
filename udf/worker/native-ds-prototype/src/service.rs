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

//! The UdfWorker service. Each Execute stream carries one call of a library: a call completes
//! with its InitResponse, and a read streams its batches after it.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use prost::Message;
use tokio::sync::{mpsc, oneshot, Notify};
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::Stream;
use tonic::{Request, Response, Status, Streaming};

use crate::calls;
use crate::host::State;
use crate::proto::ds;
use crate::proto::udf::{
    self, execution_error, udf_control_request, udf_control_response, udf_request, udf_response,
    worker_request, worker_response, CancelResponse, DataResponse, ErrorResponse, ExecutionError,
    FinishResponse, HeartbeatResponse, InitResponse, ProtocolError, ShutdownResponse,
    UdfControlResponse, UdfResponse, UdfWorkerDataFormat, WorkerResponse,
};
use crate::read::{self, worker_error, Event};

pub const FORMAT: &str = "spark.native_datasource.prototype.v0";

type Responses = mpsc::Sender<Result<UdfResponse, Status>>;

pub struct Worker {
    state: Arc<State>,
    shutdown: Arc<Notify>,
}

impl Worker {
    pub fn new(shutdown: Arc<Notify>) -> Self {
        Self { state: Arc::new(State::default()), shutdown }
    }
}

#[tonic::async_trait]
impl udf::udf_worker_server::UdfWorker for Worker {
    type ExecuteStream = Pin<Box<dyn Stream<Item = Result<UdfResponse, Status>> + Send>>;

    async fn execute(
        &self,
        request: Request<Streaming<udf::UdfRequest>>,
    ) -> Result<Response<Self::ExecuteStream>, Status> {
        let (responses, receiver) = mpsc::channel(4);
        tokio::spawn(session(self.state.clone(), request.into_inner(), responses));
        Ok(Response::new(Box::pin(ReceiverStream::new(receiver))))
    }

    async fn manage(
        &self,
        request: Request<udf::WorkerRequest>,
    ) -> Result<Response<WorkerResponse>, Status> {
        let manage = match request.into_inner().manage {
            Some(worker_request::Manage::Heartbeat(_)) => {
                let metrics = HashMap::from([
                    ("libraries".to_string(), self.state.libraries.count().to_string()),
                    ("handles".to_string(), self.state.handles.count().to_string()),
                ]);
                worker_response::Manage::Heartbeat(HeartbeatResponse { metrics })
            }
            Some(worker_request::Manage::Shutdown(_)) => {
                self.shutdown.notify_one();
                worker_response::Manage::Shutdown(ShutdownResponse { sessions_settled: false })
            }
            None => return Err(Status::invalid_argument("The WorkerRequest has no operation.")),
        };
        Ok(Response::new(WorkerResponse { manage: Some(manage) }))
    }
}

enum Inbound {
    Init(udf::Init),
    Finish,
    Cancel,
    Unexpected(&'static str),
    Closed,
}

async fn next(inbound: &mut Streaming<udf::UdfRequest>) -> Inbound {
    let request = match inbound.message().await {
        Ok(Some(request)) => request,
        Ok(None) | Err(_) => return Inbound::Closed,
    };
    match request.request {
        Some(udf_request::Request::Control(control)) => match control.control {
            Some(udf_control_request::Control::Init(init)) => Inbound::Init(init),
            Some(udf_control_request::Control::Finish(_)) => Inbound::Finish,
            Some(udf_control_request::Control::Cancel(_)) => Inbound::Cancel,
            Some(udf_control_request::Control::Payload(_)) => Inbound::Unexpected("PayloadChunk"),
            None => Inbound::Unexpected("an empty UdfControlRequest"),
        },
        Some(udf_request::Request::Data(_)) => Inbound::Unexpected("DataRequest"),
        None => Inbound::Unexpected("an empty UdfRequest"),
    }
}

fn control(control: udf_control_response::Control) -> UdfResponse {
    UdfResponse {
        response: Some(udf_response::Response::Control(UdfControlResponse { control: Some(control) })),
    }
}

fn init_response(data: Result<Vec<u8>, ExecutionError>) -> UdfResponse {
    let (data, error) = match data {
        Ok(data) => (Some(data), None),
        Err(error) => (None, Some(error)),
    };
    control(udf_control_response::Control::Init(InitResponse { data, error }))
}

fn finish_response(data: Option<Vec<u8>>) -> UdfResponse {
    control(udf_control_response::Control::Finish(FinishResponse {
        metrics: HashMap::new(),
        data,
        error: None,
        execution_metrics: None,
    }))
}

fn cancel_response() -> UdfResponse {
    control(udf_control_response::Control::Cancel(CancelResponse {
        metrics: HashMap::new(),
        error: None,
        execution_metrics: None,
    }))
}

fn error_response(error: ExecutionError) -> UdfResponse {
    control(udf_control_response::Control::Error(ErrorResponse { error: Some(error) }))
}

fn protocol_error(message: String) -> ExecutionError {
    ExecutionError { kind: Some(execution_error::Kind::Protocol(ProtocolError { message })) }
}

async fn send(out: &Responses, response: UdfResponse) -> bool {
    out.send(Ok(response)).await.is_ok()
}

/// After an error, the engine cancels the stream: answers its Cancel.
async fn await_cancel(inbound: &mut Streaming<udf::UdfRequest>, out: &Responses) {
    loop {
        match next(inbound).await {
            Inbound::Cancel => {
                send(out, cancel_response()).await;
                return;
            }
            Inbound::Closed => return,
            _ => {}
        }
    }
}

/// After a successful InitResponse, answers the Finish, or the Cancel, of the engine.
async fn await_terminator(
    inbound: &mut Streaming<udf::UdfRequest>,
    out: &Responses,
    finish_data: Option<Vec<u8>>,
) {
    match next(inbound).await {
        Inbound::Finish => {
            send(out, finish_response(finish_data)).await;
        }
        Inbound::Cancel => {
            send(out, cancel_response()).await;
        }
        Inbound::Init(_) => protocol_violation(inbound, out, "a second Init").await,
        Inbound::Unexpected(what) => protocol_violation(inbound, out, what).await,
        Inbound::Closed => {}
    }
}

async fn protocol_violation(inbound: &mut Streaming<udf::UdfRequest>, out: &Responses, what: &str) {
    let error = protocol_error(format!("The worker did not expect {what} on this stream."));
    if send(out, error_response(error)).await {
        await_cancel(inbound, out).await;
    }
}

fn decode(init: &udf::Init) -> Result<ds::request::Call, ExecutionError> {
    if init.data_format != UdfWorkerDataFormat::Arrow as i32 {
        return Err(protocol_error("The worker only supports the data format ARROW.".to_string()));
    }
    if init.is_chunking_payload == Some(true) {
        return Err(protocol_error("The worker does not support chunked payloads.".to_string()));
    }
    let payload = init.udf.as_ref().ok_or_else(|| protocol_error("Init has no udf.".to_string()))?;
    if payload.format != FORMAT {
        return Err(protocol_error(format!("The worker only supports the format {FORMAT}.")));
    }
    ds::Request::decode(payload.payload.as_slice())
        .map_err(|e| protocol_error(format!("Cannot decode the request: {e}")))?
        .call
        .ok_or_else(|| protocol_error("The request has no call.".to_string()))
}

async fn session(state: Arc<State>, mut inbound: Streaming<udf::UdfRequest>, out: Responses) {
    let init = match next(&mut inbound).await {
        Inbound::Init(init) => init,
        Inbound::Cancel => {
            send(&out, cancel_response()).await;
            return;
        }
        Inbound::Closed => return,
        Inbound::Finish => return protocol_violation(&mut inbound, &out, "Finish before Init").await,
        Inbound::Unexpected(what) => return protocol_violation(&mut inbound, &out, what).await,
    };
    match decode(&init) {
        Err(error) => {
            if send(&out, init_response(Err(error))).await {
                await_cancel(&mut inbound, &out).await;
            }
        }
        Ok(ds::request::Call::Read(read)) => read_session(state, read, inbound, out).await,
        Ok(call) => call_session(state, call, inbound, out).await,
    }
}

async fn call_session(
    state: Arc<State>,
    call: ds::request::Call,
    mut inbound: Streaming<udf::UdfRequest>,
    out: Responses,
) {
    let reply = match tokio::task::spawn_blocking(move || calls::call(&state, call)).await {
        Ok(Ok(reply)) => Ok(reply.encode_to_vec()),
        Ok(Err(message)) => Err(worker_error(message)),
        Err(e) => Err(worker_error(format!("The call failed in the worker: {e}"))),
    };
    let failed = reply.is_err();
    if send(&out, init_response(reply)).await {
        if failed {
            await_cancel(&mut inbound, &out).await;
        } else {
            await_terminator(&mut inbound, &out, None).await;
        }
    }
}

async fn read_session(
    state: Arc<State>,
    read: ds::Read,
    mut inbound: Streaming<udf::UdfRequest>,
    out: Responses,
) {
    let interrupted = Arc::new(AtomicBool::new(false));
    // One batch ahead of what the engine received, like a get_next that Spark has not asked for.
    let (events_sender, mut events) = mpsc::channel::<Event>(1);
    let (opened_sender, opened) = oneshot::channel::<Result<ds::Reply, String>>();
    let (done_sender, mut done) = oneshot::channel::<Vec<ds::LogRecord>>();
    // The read starts on Finish, when the engine asks for the data: GrpcWorkerSession drops an
    // ErrorResponse that arrives before its process() is called.
    let (start_sender, start) = oneshot::channel::<()>();
    let mut start_sender = Some(start_sender);
    let flag = interrupted.clone();
    let spawned = std::thread::Builder::new().name("native-ds-read".to_string()).spawn(move || {
        let opened = state.libraries.get(read.library).and_then(|l| read::open(l, &read, flag));
        let logs = match opened {
            Ok((reply, Some(open))) => {
                let _ = opened_sender.send(Ok(reply));
                match start.blocking_recv() {
                    Ok(()) => read::run(open, events_sender),
                    Err(_) => Vec::new(),
                }
            }
            Ok((reply, None)) => {
                let _ = opened_sender.send(Ok(reply));
                Vec::new()
            }
            Err(message) => {
                let _ = opened_sender.send(Err(message));
                Vec::new()
            }
        };
        let _ = done_sender.send(logs);
    });
    let reply = match spawned {
        Ok(_) => opened.await.unwrap_or_else(|_| Err("The read stopped before it opened.".into())),
        Err(e) => Err(format!("Cannot start the read: {e}")),
    };
    let failed = reply.is_err();
    let response = init_response(reply.map(|r| r.encode_to_vec()).map_err(worker_error));
    if !send(&out, response).await {
        interrupted.store(true, Ordering::SeqCst);
        return;
    }
    if failed {
        return await_cancel(&mut inbound, &out).await;
    }

    let read_end = |logs: Vec<ds::LogRecord>| Some(ds::ReadEnd { logs }.encode_to_vec());
    let mut finished = false;
    let mut ended = false;
    let mut errored = false;
    let mut pending: Option<Vec<u8>> = None;
    loop {
        tokio::select! {
            biased;
            message = next(&mut inbound) => match message {
                Inbound::Finish => {
                    finished = true;
                    if let Some(start) = start_sender.take() {
                        let _ = start.send(());
                    }
                    if ended {
                        let logs = (&mut done).await.unwrap_or_default();
                        send(&out, finish_response(read_end(logs))).await;
                        return;
                    }
                }
                Inbound::Cancel => {
                    interrupted.store(true, Ordering::SeqCst);
                    events.close();
                    start_sender.take();
                    let _ = (&mut done).await;
                    send(&out, cancel_response()).await;
                    return;
                }
                Inbound::Closed => {
                    interrupted.store(true, Ordering::SeqCst);
                    events.close();
                    start_sender.take();
                    return;
                }
                Inbound::Init(_) | Inbound::Unexpected(_) => {
                    interrupted.store(true, Ordering::SeqCst);
                    events.close();
                    start_sender.take();
                    let _ = (&mut done).await;
                    return protocol_violation(&mut inbound, &out, "a request other than Finish or Cancel").await;
                }
            },
            permit = out.reserve(), if pending.is_some() => match permit {
                Ok(permit) => {
                    let data = pending.take().unwrap();
                    permit.send(Ok(UdfResponse { response: Some(udf_response::Response::Data(DataResponse { data })) }));
                }
                Err(_) => {
                    interrupted.store(true, Ordering::SeqCst);
                    events.close();
                    return;
                }
            },
            event = events.recv(), if pending.is_none() && !ended && !errored => match event {
                Some(Event::Batch(messages)) => pending = Some(messages),
                Some(Event::End) | None => {
                    ended = true;
                    if finished {
                        let logs = (&mut done).await.unwrap_or_default();
                        send(&out, finish_response(read_end(logs))).await;
                        return;
                    }
                }
                Some(Event::Failed(error)) => {
                    errored = true;
                    if !send(&out, error_response(error)).await {
                        interrupted.store(true, Ordering::SeqCst);
                        return;
                    }
                }
            },
        }
    }
}
