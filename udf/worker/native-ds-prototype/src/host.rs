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

//! The host of the libraries: what Spark does in process before it calls a library, for all the
//! Execute streams of the worker. A library is loaded and initialized once for each path, and the
//! handles that it returns stay in the worker until Spark closes them.

use std::cell::RefCell;
use std::collections::hash_map::RandomState;
use std::collections::HashMap;
use std::ffi::{c_char, c_void, CStr, CString};
use std::hash::{BuildHasher, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use crate::abi::{Function, InitFn, LastErrorFn, SparkDsHost, ABI_VERSION, TABLE_FUNCTIONS};
use crate::proto::ds::LogRecord;

pub struct Library {
    pub id: u64,
    table_size: i64,
    abi_version: i32,
    // The address of each function of the table, or 0 if it is NULL or past the end of the table.
    functions: [usize; Function::ALL.len()],
    _library: libloading::Library,
}

impl Library {
    pub fn function(&self, function: Function) -> Option<usize> {
        Some(self.functions[function as usize]).filter(|&address| address != 0)
    }

    pub fn present_functions(&self) -> Vec<String> {
        Function::ALL
            .iter()
            .filter(|f| self.function(**f).is_some())
            .map(|f| f.field_name().to_string())
            .collect()
    }

    pub fn table_size(&self) -> i64 {
        self.table_size
    }

    pub fn abi_version(&self) -> i32 {
        self.abi_version
    }

    /// Calls last_error, which must run on the thread of the call that failed.
    pub fn last_error(&self) -> Option<String> {
        let address = self.function(Function::LastError)?;
        let last_error: LastErrorFn = unsafe { std::mem::transmute(address) };
        let message = unsafe { last_error() };
        (!message.is_null())
            .then(|| unsafe { CStr::from_ptr(message) }.to_string_lossy().into_owned())
    }
}

#[derive(Clone)]
pub enum Loaded {
    Library(Arc<Library>),
    CannotOpen(String),
    NoInit,
    InitFailed(i32),
    NoTable,
}

#[derive(Default)]
pub struct Libraries {
    // A library that spark_ds_init rejected stays loaded and keeps its outcome, as in Spark: it is
    // never initialized again. A library that could not be opened is tried again.
    by_path: Mutex<HashMap<String, Loaded>>,
    by_id: Mutex<HashMap<u64, Arc<Library>>>,
    // Rejected libraries, which must not be unloaded.
    rejected: Mutex<Vec<libloading::Library>>,
}

impl Libraries {
    /// Loads the library at `path` and initializes it, once. Returns the logs of spark_ds_init.
    pub fn load(&self, path: &str, spark_version: &str) -> (Loaded, Vec<LogRecord>) {
        let mut by_path = self.by_path.lock().unwrap();
        if let Some(loaded) = by_path.get(path) {
            return (loaded.clone(), Vec::new());
        }
        let (loaded, logs) = capturing_logs(|| self.initialize(path, spark_version));
        if !matches!(loaded, Loaded::CannotOpen(_)) {
            by_path.insert(path.to_string(), loaded.clone());
        }
        if let Loaded::Library(library) = &loaded {
            self.by_id.lock().unwrap().insert(library.id, library.clone());
        }
        (loaded, logs)
    }

    pub fn get(&self, id: u64) -> Result<Arc<Library>, String> {
        self.by_id.lock().unwrap().get(&id).cloned().ok_or_else(|| {
            format!("The worker has no library {id}: it may have restarted since Spark loaded it.")
        })
    }

    pub fn count(&self) -> usize {
        self.by_id.lock().unwrap().len()
    }

    fn initialize(&self, path: &str, spark_version: &str) -> Loaded {
        let library = match unsafe { libloading::Library::new(path) } {
            Ok(library) => library,
            Err(e) => return Loaded::CannotOpen(e.to_string()),
        };
        let init: InitFn = match unsafe { library.get::<InitFn>(b"spark_ds_init\0") } {
            Ok(symbol) => *symbol,
            Err(_) => {
                self.rejected.lock().unwrap().push(library);
                return Loaded::NoInit;
            }
        };
        let mut table: *const c_void = std::ptr::null();
        let status = unsafe { init(host(spark_version), &mut table) };
        if status != 0 || table.is_null() {
            self.rejected.lock().unwrap().push(library);
            return if status != 0 { Loaded::InitFailed(status) } else { Loaded::NoTable };
        }
        let base = table as *const u8;
        let table_size = unsafe { (base as *const i64).read_unaligned() };
        let abi_version = unsafe { (base.add(8) as *const i32).read_unaligned() };
        let mut functions = [0usize; Function::ALL.len()];
        // Only the functions that fit in the size of the table, of the version that the worker
        // knows: a library built against an earlier header has a shorter table.
        if abi_version == ABI_VERSION && table_size >= TABLE_FUNCTIONS as i64 {
            for function in Function::ALL {
                if (function.offset() + 8) as i64 <= table_size {
                    functions[function as usize] =
                        unsafe { (base.add(function.offset()) as *const usize).read_unaligned() };
                }
            }
        }
        Loaded::Library(Arc::new(Library {
            id: new_id(),
            table_size,
            abi_version,
            functions,
            _library: library,
        }))
    }
}

/// A SparkDsHost for one library, which the library may keep until the process exits.
fn host(spark_version: &str) -> *const SparkDsHost {
    let version = CString::new(spark_version).unwrap_or_default();
    Box::leak(Box::new(SparkDsHost {
        size: size_of::<SparkDsHost>() as i64,
        abi_version: ABI_VERSION,
        spark_version: version.into_raw(),
        log: Some(log),
    }))
}

thread_local! {
    static CAPTURED_LOGS: RefCell<Option<Vec<LogRecord>>> = const { RefCell::new(None) };
}

/// Runs `f`, and returns what the library logged on this thread meanwhile.
pub fn capturing_logs<T>(f: impl FnOnce() -> T) -> (T, Vec<LogRecord>) {
    let previous = CAPTURED_LOGS.with(|logs| logs.replace(Some(Vec::new())));
    let result = f();
    let logs = CAPTURED_LOGS.with(|logs| logs.replace(previous)).unwrap_or_default();
    (result, logs)
}

// SparkDsHost.log has no context argument, so a message is attributed to the call that runs on
// the thread that logs it. The others only go to the log of the worker.
unsafe extern "C" fn log(level: i32, message: *const c_char) {
    let message = if message.is_null() {
        String::new()
    } else {
        unsafe { CStr::from_ptr(message) }.to_string_lossy().into_owned()
    };
    let unclaimed = CAPTURED_LOGS
        .try_with(|logs| match logs.try_borrow_mut() {
            Ok(mut logs) => match logs.as_mut() {
                Some(logs) => {
                    logs.push(LogRecord { level, message: message.clone() });
                    false
                }
                None => true,
            },
            Err(_) => true,
        })
        .unwrap_or(true);
    if unclaimed {
        eprintln!("native-ds-worker: library log (level {level}) outside a call: {message}");
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    Source,
    Reader,
}

struct Entry {
    kind: Kind,
    library: Arc<Library>,
    pointer: usize,
}

/// The handles that the libraries returned, by an ID of this worker process.
#[derive(Default)]
pub struct Handles {
    entries: Mutex<HashMap<u64, Entry>>,
}

impl Handles {
    pub fn insert(&self, kind: Kind, library: Arc<Library>, pointer: *mut c_void) -> u64 {
        let id = new_id();
        self.entries.lock().unwrap().insert(id, Entry { kind, library, pointer: pointer as usize });
        id
    }

    pub fn get(&self, id: u64, kind: Kind) -> Result<(Arc<Library>, *mut c_void), String> {
        let entries = self.entries.lock().unwrap();
        let entry = entries.get(&id).ok_or_else(|| unknown(id, kind))?;
        if entry.kind != kind {
            return Err(format!("The handle {id} is a {:?} handle, not a {kind:?} handle.", entry.kind));
        }
        Ok((entry.library.clone(), entry.pointer as *mut c_void))
    }

    pub fn remove(&self, id: u64, kind: Kind) -> Result<(Arc<Library>, *mut c_void), String> {
        let found = self.get(id, kind)?;
        self.entries.lock().unwrap().remove(&id);
        Ok(found)
    }

    pub fn count(&self) -> usize {
        self.entries.lock().unwrap().len()
    }
}

fn unknown(id: u64, kind: Kind) -> String {
    format!(
        "The worker has no {kind:?} handle {id}: it was closed, or it belongs to another worker \
         process."
    )
}

/// A new ID for a library or a handle, unique to this worker process: an ID that another
/// process returned, as before a restart, is reported as unknown rather than used.
fn new_id() -> u64 {
    static SEED: OnceLock<u64> = OnceLock::new();
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let seed = *SEED.get_or_init(|| {
        let mut hasher = RandomState::new().build_hasher();
        hasher.write_u32(std::process::id());
        hasher.finish()
    });
    loop {
        let id = splitmix64(seed ^ COUNTER.fetch_add(1, Ordering::Relaxed));
        if id != 0 {
            return id;
        }
    }
}

// A bijection of u64, so that the IDs of a process never repeat.
fn splitmix64(x: u64) -> u64 {
    let mut z = x.wrapping_add(0x9e37_79b9_7f4a_7c15);
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

#[derive(Default)]
pub struct State {
    pub libraries: Libraries,
    pub handles: Handles,
}
