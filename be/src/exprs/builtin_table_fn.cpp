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
#include "builtin_table_fn.h"
#include "math_functions.h"

namespace doris {

class FunctionContextImpl;

void BuiltinTableFn::init() {

}

void BuiltinTableFn::generate_rand(doris_udf::FunctionContext *context, const doris_udf::IntVal &row, const doris_udf::IntVal &col) {
    doris_udf::RecordStore* store = context->impl()->record_store();
    std::cout << "BuiltinTableFn::generate_rand " << "row : " << row.val << "col : " << col.val <<std::endl;
    for (int i = 0; i < row.val; ++i) {
        doris_udf::Record *record = store->allocate_record();
        for (int j = 0; j < col.val; ++j) {
            // set index
            //record->set_int(j, (int) MathFunctions::rand(context).val);
            record->set_int(j, i + j);
        }
        store->append_record(record);
    }
}

}
