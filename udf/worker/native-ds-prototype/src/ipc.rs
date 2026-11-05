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

//! Schemas and batches as Arrow IPC messages, the format of Spark's side of the stream.

use std::io::Cursor;
use std::sync::Arc;

use arrow_array::{RecordBatch, RecordBatchOptions, StructArray};
use arrow_data::ArrayData;
use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::{write_message, DictionaryTracker, IpcDataGenerator, IpcWriteContext, IpcWriteOptions};
use arrow_schema::{ArrowError, Schema, SchemaRef};

pub fn schema_from_ipc(bytes: &[u8]) -> Result<Schema, ArrowError> {
    let reader = StreamReader::try_new(Cursor::new(bytes), None)?;
    Ok(reader.schema().as_ref().clone())
}

/// Encodes the schema of a stream, and the batches that follow it with the same tracker.
pub struct Encoder {
    schema: SchemaRef,
    // The top-level nullability is Spark's to check against the schema to read, as it does in
    // process, so the batches are built without it.
    batch_schema: SchemaRef,
    tracker: DictionaryTracker,
    options: IpcWriteOptions,
    context: IpcWriteContext,
}

impl Encoder {
    pub fn new(schema: SchemaRef) -> Self {
        let batch_schema = Arc::new(Schema::new_with_metadata(
            schema.fields().iter().map(|f| f.as_ref().clone().with_nullable(true)).collect::<Vec<_>>(),
            schema.metadata().clone(),
        ));
        Self {
            schema,
            batch_schema,
            tracker: DictionaryTracker::new(false),
            options: IpcWriteOptions::default(),
            context: IpcWriteContext::default(),
        }
    }

    pub fn schema_message(&mut self) -> Result<Vec<u8>, ArrowError> {
        let encoded = IpcDataGenerator::default().schema_to_bytes_with_dictionary_tracker(
            &self.schema,
            &mut self.tracker,
            &self.options,
        );
        let mut out = Vec::new();
        write_message(&mut out, encoded, &self.options)?;
        Ok(out)
    }

    /// The messages of one batch: the dictionary batches that it needs, then its record batch.
    pub fn batch_messages(&mut self, batch: ArrayData) -> Result<Vec<u8>, ArrowError> {
        let rows = batch.len();
        let (_, columns, _) = StructArray::from(batch).into_parts();
        let batch = RecordBatch::try_new_with_options(
            self.batch_schema.clone(),
            columns,
            &RecordBatchOptions::new().with_row_count(Some(rows)),
        )?;
        let (dictionaries, encoded) = IpcDataGenerator::default().encode(
            &batch,
            &mut self.tracker,
            &self.options,
            &mut self.context,
        )?;
        let mut out = Vec::new();
        for dictionary in dictionaries {
            write_message(&mut out, dictionary, &self.options)?;
        }
        write_message(&mut out, encoded, &self.options)?;
        Ok(out)
    }
}

pub fn schema_message(schema: &Schema) -> Result<Vec<u8>, ArrowError> {
    Encoder::new(Arc::new(schema.clone())).schema_message()
}
