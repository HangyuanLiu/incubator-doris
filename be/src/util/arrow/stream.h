// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
#ifndef DORIS_STREAM_H
#define DORIS_STREAM_H

#include <memory>

#include "common/status.h"
#include "runtime/mem_pool.h"

namespace arrow {

class MemoryPool;
class RecordBatch;
class Schema;
}

namespace doris {

class MemTracker;
class ObjectPool;
class RowBatch;
class RowDescriptor;

Status convert_to_stream(const arrow::RecordBatch& batch,
        const RowDescriptor& row_desc,
        MemPool* tuple_pool,
        std::shared_ptr<RowBatch>* result);

}
#endif //DORIS_STREAM_H

