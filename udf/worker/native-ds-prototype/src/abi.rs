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

//! `#[repr(C)]` mirrors of version 1 of
//! `sql/core/src/main/resources/org/apache/spark/sql/datasource/spark_datasource.h`.

use std::ffi::{c_char, c_int, c_void};
use std::mem::offset_of;

use arrow_data::ffi::FFI_ArrowArray;
use arrow_schema::ffi::FFI_ArrowSchema;

pub const ABI_VERSION: i32 = 1;

#[repr(C)]
pub struct SparkDsHost {
    pub size: i64,
    pub abi_version: i32,
    pub spark_version: *const c_char,
    pub log: Option<unsafe extern "C" fn(level: i32, message: *const c_char)>,
}

#[repr(C)]
pub struct SparkDsCreateInfo {
    pub size: i64,
    pub name: *const c_char,
    pub option_keys: *const *const c_char,
    pub option_values: *const *const c_char,
    pub num_options: i64,
}

#[repr(C)]
pub struct SparkDsTaskContext {
    pub size: i64,
    pub stage_id: i32,
    pub stage_attempt_number: i32,
    pub partition_id: i32,
    pub attempt_number: i32,
    pub task_attempt_id: i64,
    pub is_interrupted: Option<unsafe extern "C" fn(task: *const SparkDsTaskContext) -> bool>,
}

#[repr(C)]
pub struct ArrowArrayStream {
    pub get_schema: Option<unsafe extern "C" fn(*mut ArrowArrayStream, *mut FFI_ArrowSchema) -> c_int>,
    pub get_next: Option<unsafe extern "C" fn(*mut ArrowArrayStream, *mut FFI_ArrowArray) -> c_int>,
    pub get_last_error: Option<unsafe extern "C" fn(*mut ArrowArrayStream) -> *const c_char>,
    pub release: Option<unsafe extern "C" fn(*mut ArrowArrayStream)>,
    pub private_data: *mut c_void,
}

impl ArrowArrayStream {
    pub fn empty() -> Self {
        Self {
            get_schema: None,
            get_next: None,
            get_last_error: None,
            release: None,
            private_data: std::ptr::null_mut(),
        }
    }
}

/// `struct ArrowArray`, to read the fields that `FFI_ArrowArray` keeps private.
#[repr(C)]
pub struct RawArrowArray {
    pub length: i64,
    pub null_count: i64,
    pub offset: i64,
    pub n_buffers: i64,
    pub n_children: i64,
    pub buffers: *const *const c_void,
    pub children: *const *const RawArrowArray,
    pub dictionary: *const RawArrowArray,
    pub release: Option<unsafe extern "C" fn(*mut RawArrowArray)>,
    pub private_data: *mut c_void,
}

/// `struct ArrowSchema`, to check that a library initialized one before arrow-rs reads it.
#[repr(C)]
pub struct RawArrowSchema {
    pub format: *const c_char,
    pub name: *const c_char,
    pub metadata: *const c_char,
    pub flags: i64,
    pub n_children: i64,
    pub children: *const *const RawArrowSchema,
    pub dictionary: *const RawArrowSchema,
    pub release: Option<unsafe extern "C" fn(*mut RawArrowSchema)>,
    pub private_data: *mut c_void,
}

/// Whether a library initialized the schema: arrow-rs reads `format` without checking it.
pub fn is_initialized(schema: &FFI_ArrowSchema) -> bool {
    let raw = unsafe { &*(schema as *const FFI_ArrowSchema as *const RawArrowSchema) };
    raw.release.is_some() && !raw.format.is_null()
}

pub type SparkDsSink = unsafe extern "C" fn(sink: *mut c_void, data: *const u8, length: i64);

pub type InitFn =
    unsafe extern "C" fn(host: *const SparkDsHost, library: *mut *const c_void) -> c_int;
pub type LastErrorFn = unsafe extern "C" fn() -> *const c_char;
pub type CreateFn = unsafe extern "C" fn(
    info: *const SparkDsCreateInfo,
    capabilities: *mut u64,
    source: *mut *mut c_void,
) -> c_int;
pub type SchemaFn = unsafe extern "C" fn(source: *mut c_void, out: *mut FFI_ArrowSchema) -> c_int;
pub type CreateReaderFn = unsafe extern "C" fn(
    source: *mut c_void,
    schema: *mut FFI_ArrowSchema,
    reader: *mut *mut c_void,
) -> c_int;
pub type CloseFn = unsafe extern "C" fn(handle: *mut c_void);
pub type PushLimitFn = unsafe extern "C" fn(reader: *mut c_void, limit: i32, applied: *mut bool) -> c_int;
pub type PruneColumnsFn = unsafe extern "C" fn(
    reader: *mut c_void,
    columns: *const *const c_char,
    count: i64,
    pruned: *mut bool,
) -> c_int;
pub type SinkFn = unsafe extern "C" fn(handle: *mut c_void, sink: SparkDsSink, data: *mut c_void) -> c_int;
pub type ReadFn = unsafe extern "C" fn(
    task: *const SparkDsTaskContext,
    reader_state: *const u8,
    reader_state_length: i64,
    partition: *const u8,
    partition_length: i64,
    out: *mut ArrowArrayStream,
) -> c_int;

/// The offset of the first function of `SparkDsLibrary`, after its size and its version.
pub const TABLE_FUNCTIONS: usize = 16;

/// The functions of `SparkDsLibrary`, in their order in the table.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Function {
    LastError,
    Create,
    Schema,
    CreateReader,
    CreateStreamReader,
    CreateWriter,
    Close,
    PushLimit,
    PruneColumns,
    Partitions,
    SerializeReader,
    CloseReader,
    InitialOffset,
    LatestOffset,
    StreamPartitions,
    SerializeStreamReader,
    CommitOffset,
    CloseStreamReader,
    Read,
    SerializeWriter,
    Commit,
    Abort,
    CloseWriter,
    CreateDataWriter,
    Write,
    CommitDataWriter,
    AbortDataWriter,
    CloseDataWriter,
}

impl Function {
    pub const ALL: [Function; 28] = [
        Function::LastError,
        Function::Create,
        Function::Schema,
        Function::CreateReader,
        Function::CreateStreamReader,
        Function::CreateWriter,
        Function::Close,
        Function::PushLimit,
        Function::PruneColumns,
        Function::Partitions,
        Function::SerializeReader,
        Function::CloseReader,
        Function::InitialOffset,
        Function::LatestOffset,
        Function::StreamPartitions,
        Function::SerializeStreamReader,
        Function::CommitOffset,
        Function::CloseStreamReader,
        Function::Read,
        Function::SerializeWriter,
        Function::Commit,
        Function::Abort,
        Function::CloseWriter,
        Function::CreateDataWriter,
        Function::Write,
        Function::CommitDataWriter,
        Function::AbortDataWriter,
        Function::CloseDataWriter,
    ];

    pub fn field_name(self) -> &'static str {
        match self {
            Function::LastError => "last_error",
            Function::Create => "create",
            Function::Schema => "schema",
            Function::CreateReader => "create_reader",
            Function::CreateStreamReader => "create_stream_reader",
            Function::CreateWriter => "create_writer",
            Function::Close => "close",
            Function::PushLimit => "push_limit",
            Function::PruneColumns => "prune_columns",
            Function::Partitions => "partitions",
            Function::SerializeReader => "serialize_reader",
            Function::CloseReader => "close_reader",
            Function::InitialOffset => "initial_offset",
            Function::LatestOffset => "latest_offset",
            Function::StreamPartitions => "stream_partitions",
            Function::SerializeStreamReader => "serialize_stream_reader",
            Function::CommitOffset => "commit_offset",
            Function::CloseStreamReader => "close_stream_reader",
            Function::Read => "read",
            Function::SerializeWriter => "serialize_writer",
            Function::Commit => "commit",
            Function::Abort => "abort",
            Function::CloseWriter => "close_writer",
            Function::CreateDataWriter => "create_data_writer",
            Function::Write => "write",
            Function::CommitDataWriter => "commit_data_writer",
            Function::AbortDataWriter => "abort_data_writer",
            Function::CloseDataWriter => "close_data_writer",
        }
    }

    pub fn offset(self) -> usize {
        TABLE_FUNCTIONS + 8 * self as usize
    }
}

const _: () = assert!(offset_of!(SparkDsHost, log) == 24 && size_of::<SparkDsHost>() == 32);
const _: () = assert!(
    offset_of!(SparkDsCreateInfo, num_options) == 32 && size_of::<SparkDsCreateInfo>() == 40
);
const _: () = assert!(
    offset_of!(SparkDsTaskContext, task_attempt_id) == 24
        && offset_of!(SparkDsTaskContext, is_interrupted) == 32
        && size_of::<SparkDsTaskContext>() == 40
);
const _: () = assert!(size_of::<RawArrowArray>() == size_of::<FFI_ArrowArray>());
const _: () = assert!(size_of::<RawArrowSchema>() == size_of::<FFI_ArrowSchema>());
