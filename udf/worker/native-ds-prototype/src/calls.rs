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

//! The calls of a Request that complete with their Reply, on the library. An error is a failure
//! of the worker, not of the library, whose failures are in the Reply.

use std::ffi::{c_char, c_void, CString};

use arrow_schema::ffi::FFI_ArrowSchema;
use arrow_schema::Schema;

use crate::abi::{
    self, CloseFn, CreateFn, CreateReaderFn, Function, PruneColumnsFn, PushLimitFn, SchemaFn,
    SinkFn, SparkDsCreateInfo,
};
use crate::host::{capturing_logs, Kind, Library, Loaded, State};
use crate::ipc;
use crate::proto::ds::{self, loaded, reply, request::Call, Created, Reply, Sunk};

pub fn call(state: &State, call: Call) -> Result<Reply, String> {
    match call {
        Call::Load(load) => Ok(load_library(state, &load)),
        Call::Create(create) => create_source(state, create),
        Call::Schema(schema) => source_schema(state, schema.source),
        Call::CreateReader(create) => create_reader(state, create),
        Call::Close(close) => close_handle(state, close.source, Kind::Source, Function::Close),
        Call::PushLimit(push) => push_limit(state, push),
        Call::PruneColumns(prune) => prune_columns(state, prune),
        Call::Partitions(p) => sink_call(state, p.reader, Function::Partitions),
        Call::SerializeReader(s) => sink_call(state, s.reader, Function::SerializeReader),
        Call::CloseReader(close) => {
            close_handle(state, close.reader, Kind::Reader, Function::CloseReader)
        }
        Call::Read(_) => Err("A read streams its batches, so it is not a call.".to_string()),
    }
}

/// The function of the library, typed. Spark checks that a library implements a function before
/// it calls it, so a missing one is a failure of Spark or of the worker.
unsafe fn function<F: Copy>(library: &Library, function: Function) -> Result<F, String> {
    let address = library
        .function(function)
        .ok_or_else(|| format!("The library does not implement {}.", function.field_name()))?;
    Ok(unsafe { std::mem::transmute_copy::<usize, F>(&address) })
}

/// Calls a function that returns an int, and gets last_error on the same thread if it failed.
fn invoke(library: &Library, f: impl FnOnce() -> i32) -> Reply {
    let ((status, last_error), logs) = capturing_logs(|| {
        let status = f();
        (status, if status != 0 { library.last_error() } else { None })
    });
    Reply { status, last_error, logs, value: None }
}

fn c_string(value: &str) -> Result<CString, String> {
    CString::new(value).map_err(|_| format!("The string {value:?} contains the character U+0000."))
}

fn load_library(state: &State, load: &ds::Load) -> Reply {
    let (loaded, logs) = state.libraries.load(&load.path, &load.spark_version);
    let (status, outcome) = match loaded {
        Loaded::Library(library) => (
            0,
            loaded::Outcome::Library(ds::Library {
                id: library.id,
                table_size: library.table_size(),
                abi_version: library.abi_version(),
                functions: library.present_functions(),
            }),
        ),
        Loaded::CannotOpen(reason) => (0, loaded::Outcome::CannotOpen(reason)),
        Loaded::NoInit => (0, loaded::Outcome::NoInit(true)),
        Loaded::InitFailed(status) => (status, loaded::Outcome::InitFailed(true)),
        Loaded::NoTable => (0, loaded::Outcome::NoTable(true)),
    };
    Reply {
        status,
        last_error: None,
        logs,
        value: Some(reply::Value::Loaded(ds::Loaded { outcome: Some(outcome) })),
    }
}

fn create_source(state: &State, create: ds::Create) -> Result<Reply, String> {
    let library = state.libraries.get(create.library)?;
    let create_fn: CreateFn = unsafe { function(&library, Function::Create)? };
    if create.option_keys.len() != create.option_values.len() {
        return Err("Create has as many option keys as values.".to_string());
    }
    let name = c_string(&create.name)?;
    let keys = create.option_keys.iter().map(|k| c_string(k)).collect::<Result<Vec<_>, _>>()?;
    let values = create.option_values.iter().map(|v| c_string(v)).collect::<Result<Vec<_>, _>>()?;
    let key_pointers: Vec<*const c_char> = keys.iter().map(|k| k.as_ptr()).collect();
    let value_pointers: Vec<*const c_char> = values.iter().map(|v| v.as_ptr()).collect();
    let info = SparkDsCreateInfo {
        size: size_of::<SparkDsCreateInfo>() as i64,
        name: name.as_ptr(),
        option_keys: key_pointers.as_ptr(),
        option_values: value_pointers.as_ptr(),
        num_options: keys.len() as i64,
    };
    let mut capabilities = 0u64;
    let mut source: *mut c_void = std::ptr::null_mut();
    let mut reply = invoke(&library, || unsafe { create_fn(&info, &mut capabilities, &mut source) });
    if reply.status == 0 {
        let source = if source.is_null() {
            0
        } else {
            state.handles.insert(Kind::Source, library.clone(), source)
        };
        reply.value = Some(reply::Value::Created(Created { capabilities, source }));
    }
    Ok(reply)
}

fn source_schema(state: &State, id: u64) -> Result<Reply, String> {
    let (library, source) = state.handles.get(id, Kind::Source)?;
    let schema_fn: SchemaFn = unsafe { function(&library, Function::Schema)? };
    let mut out = FFI_ArrowSchema::empty();
    let mut reply = invoke(&library, || unsafe { schema_fn(source, &mut out) });
    if reply.status == 0 {
        if !abi::is_initialized(&out) {
            return Err("schema returned without initializing the schema.".to_string());
        }
        let schema = Schema::try_from(&out).map_err(|e| e.to_string())?;
        let message = ipc::schema_message(&schema).map_err(|e| e.to_string())?;
        reply.value = Some(reply::Value::Schema(message));
    }
    Ok(reply)
}

fn create_reader(state: &State, create: ds::CreateReader) -> Result<Reply, String> {
    let (library, source) = state.handles.get(create.source, Kind::Source)?;
    let create_fn: CreateReaderFn = unsafe { function(&library, Function::CreateReader)? };
    let schema = ipc::schema_from_ipc(&create.schema).map_err(|e| e.to_string())?;
    let mut ffi_schema = FFI_ArrowSchema::try_from(&schema).map_err(|e| e.to_string())?;
    let mut reader: *mut c_void = std::ptr::null_mut();
    let mut reply = invoke(&library, || unsafe { create_fn(source, &mut ffi_schema, &mut reader) });
    // Released here unless the library moved it.
    drop(ffi_schema);
    if reply.status == 0 {
        let reader = if reader.is_null() {
            0
        } else {
            state.handles.insert(Kind::Reader, library.clone(), reader)
        };
        reply.value = Some(reply::Value::Handle(reader));
    }
    Ok(reply)
}

fn close_handle(state: &State, id: u64, kind: Kind, close: Function) -> Result<Reply, String> {
    let (library, handle) = state.handles.remove(id, kind)?;
    let close_fn: CloseFn = unsafe { function(&library, close)? };
    Ok(invoke(&library, || {
        unsafe { close_fn(handle) };
        0
    }))
}

fn push_limit(state: &State, push: ds::PushLimit) -> Result<Reply, String> {
    let (library, reader) = state.handles.get(push.reader, Kind::Reader)?;
    let push_fn: PushLimitFn = unsafe { function(&library, Function::PushLimit)? };
    let mut applied = false;
    let mut reply = invoke(&library, || unsafe { push_fn(reader, push.limit, &mut applied) });
    if reply.status == 0 {
        reply.value = Some(reply::Value::Flag(applied));
    }
    Ok(reply)
}

fn prune_columns(state: &State, prune: ds::PruneColumns) -> Result<Reply, String> {
    let (library, reader) = state.handles.get(prune.reader, Kind::Reader)?;
    let prune_fn: PruneColumnsFn = unsafe { function(&library, Function::PruneColumns)? };
    let columns = prune.columns.iter().map(|c| c_string(c)).collect::<Result<Vec<_>, _>>()?;
    let pointers: Vec<*const c_char> = columns.iter().map(|c| c.as_ptr()).collect();
    let mut pruned = false;
    let mut reply = invoke(&library, || unsafe {
        prune_fn(reader, pointers.as_ptr(), pointers.len() as i64, &mut pruned)
    });
    if reply.status == 0 {
        reply.value = Some(reply::Value::Flag(pruned));
    }
    Ok(reply)
}

#[derive(Default)]
struct Sink {
    values: Vec<Vec<u8>>,
    error: Option<String>,
}

unsafe extern "C" fn sink(sink: *mut c_void, data: *const u8, length: i64) {
    let sink = unsafe { &mut *(sink as *mut Sink) };
    if sink.error.is_some() {
        return;
    }
    if length < 0 || (data.is_null() && length > 0) {
        sink.error = Some(format!(
            "The library returned an invalid byte array, of {length} bytes at the address {}.",
            data as usize
        ));
        return;
    }
    let bytes = if length == 0 {
        Vec::new()
    } else {
        unsafe { std::slice::from_raw_parts(data, length as usize) }.to_vec()
    };
    sink.values.push(bytes);
}

fn sink_call(state: &State, id: u64, called: Function) -> Result<Reply, String> {
    let (library, reader) = state.handles.get(id, Kind::Reader)?;
    let sink_fn: SinkFn = unsafe { function(&library, called)? };
    let mut values = Sink::default();
    let mut reply =
        invoke(&library, || unsafe { sink_fn(reader, sink, &mut values as *mut Sink as *mut c_void) });
    if reply.status == 0 {
        if let Some(error) = values.error {
            return Err(error);
        }
        reply.value = Some(reply::Value::Sunk(Sunk { values: values.values }));
    }
    Ok(reply)
}
