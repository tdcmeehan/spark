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

//! The checks that Spark makes on each batch that it reads in process, before it imports it
//! (NativePartitionReader.checkArrays in NativeDataSource.scala), with the same messages. Spark
//! never sees the arrays of the library here, since the worker re-encodes them, and arrow-rs
//! would read some of them differently: it reads a sliced array correctly, and panics on a struct
//! with too few children.

use std::ffi::c_void;

use arrow_schema::{DataType, Field, Fields};

use crate::abi::RawArrowArray;

pub fn check_batch(batch: &RawArrowArray, fields: &Fields) -> Result<(), String> {
    if batch.offset != 0 {
        return Err(format!(
            "The batch has an offset of {}, but Spark only reads arrays with an offset of 0. Copy \
             sliced arrays before exporting them.",
            batch.offset
        ));
    }
    check_validity(batch, "The batch")?;
    let null_rows = match batch.null_count {
        -1 => count_nulls(batch),
        n if n < -1 => return Err(format!("The batch has an invalid null_count of {n}.")),
        n => n,
    };
    if null_rows != 0 {
        return Err(format!(
            "The struct of the batch marks {null_rows} of its rows as null, but Spark only reads \
             batches without null rows: mark the values of the columns as null instead."
        ));
    }
    let columns = fields.len();
    let arrays = children(batch, columns, "The batch", &format!("the schema has {columns} columns"))?;
    for (column, field) in arrays.into_iter().zip(fields.iter()) {
        if column.length < batch.length {
            return Err(format!(
                "The column {} has a length of {}, but the batch has a length of {}.",
                field.name(),
                column.length,
                batch.length
            ));
        }
        check_column(column, field, field.name())?;
    }
    Ok(())
}

fn check_column(array: &RawArrowArray, field: &Field, column: &str) -> Result<(), String> {
    if array.offset != 0 {
        return Err(format!(
            "The column {column} has an offset of {}, but Spark only reads arrays with an offset \
             of 0. Copy sliced arrays before exporting them.",
            array.offset
        ));
    }
    check_validity(array, &format!("The column {column}"))?;
    let length = referenced(array, field.data_type());
    let fields = child_fields(field.data_type());
    let owner = format!("An array of the column {column}");
    let arrays = children(array, fields.len(), &owner, &format!("its type has {}", fields.len()))?;
    for (child, child_field) in arrays.into_iter().zip(fields) {
        if child.length < length {
            return Err(format!(
                "The column {column} contains an array with a length of {}, but its parent refers \
                 to {length} values.",
                child.length
            ));
        }
        check_column(child, child_field, column)?;
    }
    Ok(())
}

// The fields that Arrow Java gives the children of a field of this type.
fn child_fields(data_type: &DataType) -> Vec<&Field> {
    match data_type {
        DataType::Struct(fields) => fields.iter().map(|f| f.as_ref()).collect(),
        DataType::List(f)
        | DataType::LargeList(f)
        | DataType::FixedSizeList(f, _)
        | DataType::ListView(f)
        | DataType::LargeListView(f)
        | DataType::Map(f, _) => vec![f.as_ref()],
        DataType::Union(fields, _) => fields.iter().map(|(_, f)| f.as_ref()).collect(),
        DataType::RunEndEncoded(run_ends, values) => vec![run_ends.as_ref(), values.as_ref()],
        _ => Vec::new(),
    }
}

fn children<'a>(
    array: &'a RawArrowArray,
    fields: usize,
    owner: &str,
    expected: &str,
) -> Result<Vec<&'a RawArrowArray>, String> {
    if array.n_children != fields as i64 || (fields > 0 && array.children.is_null()) {
        return Err(format!("{owner} has {} child arrays, but {expected}.", array.n_children));
    }
    (0..fields)
        .map(|i| {
            let child = unsafe { *array.children.add(i) };
            if child.is_null() {
                Err(format!("{owner} has a NULL child array."))
            } else {
                Ok(unsafe { &*child })
            }
        })
        .collect()
}

fn referenced(array: &RawArrowArray, data_type: &DataType) -> i64 {
    match data_type {
        DataType::Struct(_) => array.length,
        DataType::List(_) | DataType::Map(_, _) => last_offset(array),
        _ => 0,
    }
}

fn buffer(array: &RawArrowArray, i: i64) -> *const c_void {
    if array.n_buffers > i && !array.buffers.is_null() {
        unsafe { *array.buffers.add(i as usize) }
    } else {
        std::ptr::null()
    }
}

fn last_offset(array: &RawArrowArray) -> i64 {
    let offsets = if array.length > 0 { buffer(array, 1) } else { std::ptr::null() };
    if offsets.is_null() {
        0
    } else {
        unsafe { (offsets as *const i32).add(array.length as usize).read_unaligned() as i64 }
    }
}

fn check_validity(array: &RawArrowArray, owner: &str) -> Result<(), String> {
    if array.null_count != 0 && array.length > 0 && buffer(array, 0).is_null() {
        return Err(format!(
            "{owner} has a null_count of {} but no validity buffer, which the C data interface \
             only allows with a null_count of 0.",
            array.null_count
        ));
    }
    Ok(())
}

fn count_nulls(batch: &RawArrowArray) -> i64 {
    let bitmap = buffer(batch, 0) as *const u8;
    let mut nulls = 0i64;
    let mut i = 0i64;
    while !bitmap.is_null() && i < batch.length {
        let bits = (batch.length - i).min(8);
        let valid = unsafe { *bitmap.add((i / 8) as usize) } as u32 & ((1u32 << bits) - 1);
        nulls += bits - valid.count_ones() as i64;
        i += 8;
    }
    nulls
}
