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

// Compiles the protos where Spark keeps them, so that the worker and Spark share one copy.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let udf_worker = "../proto/src/main/protobuf";
    let sql_core = "../../../sql/core/src/main/protobuf";
    tonic_prost_build::configure().build_client(false).compile_protos(
        &[
            format!("{udf_worker}/org/apache/spark/udf/worker/udf_service.proto"),
            format!(
                "{sql_core}/org/apache/spark/sql/execution/datasources/v2/ffi/\
                 native_datasource_prototype.proto"
            ),
        ],
        &[udf_worker.to_string(), sql_core.to_string()],
    )?;
    Ok(())
}
