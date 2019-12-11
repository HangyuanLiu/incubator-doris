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

#include "util/arrow/row_batch.h"

#include <gtest/gtest.h>

#include <string>
#include <sstream>

#include "common/logging.h"

#define ARROW_UTIL_LOGGING_H
#include <arrow/json/api.h>
#include <arrow/json/test-common.h>
#include <arrow/buffer.h>
#include <arrow/pretty_print.h>

#include "common/object_pool.h"
#include "runtime/mem_tracker.h"
#include "runtime/row_batch.h"
#include "runtime/tuple_row.h"
#include "util/debug_util.h"
#include "util/arrow/stream.h"

namespace doris {

class ArrowStreamTest : public testing::Test {
public:
    ArrowStreamTest() {}
    virtual ~ArrowStreamTest() {
    }
};

std::string test_str() {
    return R"(
    { "c1": 1, "c2": 1.1 }
    { "c1": 2, "c2": 2.2 }
    { "c1": 3, "c2": 3.3 }
        )";
}

TEST_F(ArrowStreamTest, PrettyPrint) {
    auto json = test_str();
    std::shared_ptr<arrow::Buffer> buffer;
    arrow::json::MakeBuffer(test_str(), &buffer);
    arrow::json::ParseOptions parse_opts = arrow::json::ParseOptions::Defaults();
    parse_opts.explicit_schema = arrow::schema(
                {
                    arrow::field("c1", arrow::int32()),
                    //arrow::field("c2", arrow::float16()),
                    });

    std::shared_ptr<arrow::RecordBatch> record_batch;
    auto arrow_st = arrow::json::ParseOne(parse_opts, buffer, &record_batch);
    ASSERT_TRUE(arrow_st.ok());

    ObjectPool obj_pool;
    RowDescriptor *row_desc;
    auto doris_st = convert_to_row_desc(&obj_pool, *record_batch->schema(), &row_desc);
    ASSERT_TRUE(doris_st.ok());
    MemTracker tracker;
    MemPool pool(&tracker);
    std::shared_ptr<RowBatch> row_batch;
    doris_st = convert_to_stream(*record_batch, *row_desc, &pool, &row_batch);
    ASSERT_TRUE(doris_st.ok());
    std::cout << "debug string : " << row_batch->row_desc().debug_string() <<std::endl;
    std::cout << row_batch->to_string() << ":" <<record_batch->schema()->ToString() << std::endl;
    std::cout << Tuple::to_string(row_batch->get_row(0)->get_tuple(0), *row_desc->tuple_descriptors()[0]) << std::endl;
}

}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

