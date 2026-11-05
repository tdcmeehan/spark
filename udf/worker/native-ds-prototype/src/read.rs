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

//! The read of a partition. One thread calls read and then the callbacks of its stream, and owns
//! the stream and the task context, which it releases only once no callback runs.

use std::ffi::CStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use arrow_array::ffi::from_ffi_and_data_type;
use arrow_data::ffi::FFI_ArrowArray;
use arrow_schema::ffi::FFI_ArrowSchema;
use arrow_schema::{DataType, Fields, Schema};
use tokio::sync::mpsc;

use crate::abi::{self, ArrowArrayStream, Function, RawArrowArray, ReadFn, SparkDsTaskContext};
use crate::checks::check_batch;
use crate::host::{capturing_logs, Library};
use crate::ipc::Encoder;
use crate::proto::ds::{self, reply, LogRecord, Opened, Reply};
use crate::proto::udf::{execution_error, ExecutionError, UserError, WorkerError};

// The SparkDsTaskContext of a read, with what its is_interrupted answers.
#[repr(C)]
struct Task {
    context: SparkDsTaskContext,
    interrupted: Arc<AtomicBool>,
}

unsafe extern "C" fn is_interrupted(task: *const SparkDsTaskContext) -> bool {
    task.is_null() || unsafe { &*(task as *const Task) }.interrupted.load(Ordering::SeqCst)
}

pub enum Event {
    Batch(Vec<u8>),
    End,
    Failed(ExecutionError),
}

pub struct OpenRead {
    stream: Box<ArrowArrayStream>,
    // Used by the library until the stream is released.
    task: Box<Task>,
    fields: Fields,
    encoder: Encoder,
}

impl Drop for OpenRead {
    fn drop(&mut self) {
        release(&mut self.stream);
    }
}

fn release(stream: &mut ArrowArrayStream) {
    if let Some(release) = stream.release {
        unsafe { release(stream) };
    }
}

fn stream_error(stream: &mut ArrowArrayStream, status: i32, callback: &str) -> String {
    let message = stream
        .get_last_error
        .map(|get_last_error| unsafe { get_last_error(stream) })
        .filter(|message| !message.is_null())
        .map(|message| unsafe { CStr::from_ptr(message) }.to_string_lossy().into_owned());
    message.unwrap_or_else(|| format!("{callback} failed with the error code {status}."))
}

/// Calls read, and get_schema on its stream. Returns the stream if the read can go on.
pub fn open(
    library: Arc<Library>,
    read: &ds::Read,
    interrupted: Arc<AtomicBool>,
) -> Result<(Reply, Option<OpenRead>), String> {
    let address = library.function(Function::Read).ok_or("The library does not implement read.")?;
    let read_fn: ReadFn = unsafe { std::mem::transmute(address) };
    let context = read.task.clone().unwrap_or_default();
    let task = Box::new(Task {
        context: SparkDsTaskContext {
            size: size_of::<SparkDsTaskContext>() as i64,
            stage_id: context.stage_id,
            stage_attempt_number: context.stage_attempt_number,
            partition_id: context.partition_id,
            attempt_number: context.attempt_number,
            task_attempt_id: context.task_attempt_id,
            is_interrupted: Some(is_interrupted),
        },
        interrupted,
    });
    let mut stream = Box::new(ArrowArrayStream::empty());
    let ((status, last_error), mut logs) = capturing_logs(|| {
        let status = unsafe {
            read_fn(
                &task.context,
                read.reader_state.as_ptr(),
                read.reader_state.len() as i64,
                read.partition.as_ptr(),
                read.partition.len() as i64,
                &mut *stream,
            )
        };
        (status, if status != 0 { library.last_error() } else { None })
    });
    let reply = |logs, value| Reply { status, last_error: last_error.clone(), logs, value };
    if status != 0 {
        release(&mut stream);
        return Ok((reply(logs, None), None));
    }
    let opened = |opened: Opened| Some(reply::Value::Opened(opened));
    if stream.release.is_none()
        || stream.get_schema.is_none()
        || stream.get_next.is_none()
        || stream.get_last_error.is_none()
    {
        release(&mut stream);
        return Ok((reply(logs, opened(Opened { initialized: false, ..Default::default() })), None));
    }
    let mut ffi_schema = FFI_ArrowSchema::empty();
    let get_schema = stream.get_schema.unwrap();
    let (schema_status, schema_logs) =
        capturing_logs(|| unsafe { get_schema(&mut *stream, &mut ffi_schema) });
    logs.extend(schema_logs);
    let schema = if schema_status != 0 {
        Err(stream_error(&mut stream, schema_status, "get_schema"))
    } else if !abi::is_initialized(&ffi_schema) {
        Err("get_schema did not initialize the schema.".to_string())
    } else {
        Schema::try_from(&ffi_schema).map_err(|e| e.to_string())
    };
    drop(ffi_schema);
    let encoded = schema.and_then(|schema| {
        let fields = schema.fields().clone();
        let mut encoder = Encoder::new(Arc::new(schema));
        let message = encoder.schema_message().map_err(|e| e.to_string())?;
        Ok((fields, encoder, message))
    });
    match encoded {
        Ok((fields, encoder, schema)) => {
            let value = opened(Opened { initialized: true, schema, schema_error: None });
            Ok((reply(logs, value), Some(OpenRead { stream, task, fields, encoder })))
        }
        Err(message) => {
            release(&mut stream);
            let value =
                opened(Opened { initialized: true, schema: Vec::new(), schema_error: Some(message) });
            Ok((reply(logs, value), None))
        }
    }
}

/// Reads the batches until the end of the stream, a failure, or until Spark cancels the read or
/// stops receiving, then releases the stream. Returns what the library logged meanwhile.
pub fn run(mut read: OpenRead, events: mpsc::Sender<Event>) -> Vec<LogRecord> {
    let ((), logs) = capturing_logs(|| {
        let get_next = read.stream.get_next.unwrap();
        while !read.task.interrupted.load(Ordering::SeqCst) {
            let mut array = FFI_ArrowArray::empty();
            let status = unsafe { get_next(&mut *read.stream, &mut array) };
            if status != 0 {
                let message = stream_error(&mut read.stream, status, "get_next");
                let _ = events.blocking_send(Event::Failed(user_error(message)));
                return;
            }
            if array.is_released() {
                let _ = events.blocking_send(Event::End);
                return;
            }
            let raw = unsafe { &*(&array as *const FFI_ArrowArray as *const RawArrowArray) };
            if let Err(message) = check_batch(raw, &read.fields) {
                drop(array);
                let _ = events.blocking_send(Event::Failed(user_error(message)));
                return;
            }
            let data_type = DataType::Struct(read.fields.clone());
            let encoded = unsafe { from_ffi_and_data_type(array, data_type) }
                .and_then(|data| read.encoder.batch_messages(data));
            let event = match encoded {
                Ok(messages) => Event::Batch(messages),
                Err(e) => Event::Failed(worker_error(format!("Cannot encode a batch: {e}"))),
            };
            let failed = matches!(event, Event::Failed(_));
            if events.blocking_send(event).is_err() || failed {
                return;
            }
        }
    });
    drop(read);
    logs
}

pub fn user_error(message: String) -> ExecutionError {
    ExecutionError {
        kind: Some(execution_error::Kind::User(UserError { message, traceback: None, error_class: None })),
    }
}

pub fn worker_error(message: String) -> ExecutionError {
    ExecutionError { kind: Some(execution_error::Kind::Worker(WorkerError { message, traceback: None })) }
}
