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

#include "exec/broker_scan_node.h"

#include <string>
#include <map>
#include <vector>

#include <gtest/gtest.h>
#include <time.h>
#include "common/object_pool.h"
#include "runtime/tuple.h"
#include "exec/local_file_reader.h"
#include "exprs/cast_functions.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "runtime/row_batch.h"
#include "runtime/user_function_cache.h"
#include "gen_cpp/Descriptors_types.h"
#include "gen_cpp/PlanNodes_types.h"

namespace doris {

class OrcScannerTest : public testing::Test {
public:
    OrcScannerTest() : _runtime_state(TQueryGlobals()) {
        _runtime_state._instance_mem_tracker.reset(new MemTracker());
    }

private:
    RuntimeState _runtime_state;
    ObjectPool _obj_pool;
    TBrokerScanRangeParams _params;
    DescriptorTbl* _desc_tbl;
    TPlanNode _tnode;
};

void OrcScannerTest::init_desc_table() {
    TDescriptorTable t_desc_table;

    // table descriptors
    TTableDescriptor t_table_desc;

    t_table_desc.id = 0;
    t_table_desc.tableType = TTableType::BROKER_TABLE;
    t_table_desc.numCols = 0;
    t_table_desc.numClusteringCols = 0;
    t_desc_table.tableDescriptors.push_back(t_table_desc);
    t_desc_table.__isset.tableDescriptors = true;

    int next_slot_id = 1;

    next_slot_id = create_dst_tuple(t_desc_table, next_slot_id);

    next_slot_id = create_src_tuple(t_desc_table, next_slot_id);

    DescriptorTbl::create(&_obj_pool, t_desc_table, &_desc_tbl);

    _runtime_state.set_desc_tbl(_desc_tbl);
}

void OrcScannerTest::create_expr_info() {

}

void OrcScannerTest::init() {
    create_expr_info();
    init_desc_table();

    // Node Id
    _tnode.node_id = 0;
    _tnode.node_type = TPlanNodeType::BROKER_SCAN_NODE;
    _tnode.num_children = 0;
    _tnode.limit = -1;
    _tnode.row_tuples.push_back(0);
    _tnode.nullable_tuples.push_back(false);
    _tnode.broker_scan_node.tuple_id = 0;
    _tnode.__isset.broker_scan_node = true;
}

TEST_F(OrcScannerTest, normal) {
    BrokerScanNode scan_node(&_obj_pool, _tnode, *_desc_tbl);
    auto status = scan_node.prepare(&_runtime_state);
    ASSERT_TRUE(status.ok());

    //set scan range
    std::vector<TScanRangeParams> scan_ranges;
    {
        TScanRangeParams scan_range_params;
        TBrokerScanRange broker_scan_range;
        broker_scan_range.params = _params;
        TBrokerRangeDesc rangeDesc;
        rangeDesc.start_offset = 0;
        rangeDesc.size = -1;
        rangeDesc.format_type = TFileFormatType::FORMAT_ORC;
        rangeDesc.splittable = false;
        rangeDesc.__set_num_of_columns_from_file(13);

        rangeDesc.path = "./be/test/exec/test_data/parquet_scanner/localfile.parquet";
        rangeDesc.file_type = TFileType::FILE_LOCAL;

        broker_scan_range.ranges.push_back(rangeDesc);
        scan_range_params.scan_range.__set_broker_scan_range(broker_scan_range);
        scan_ranges.push_back(scan_range_params);
    }

    scan_node.set_scan_ranges(scan_ranges);
    status = scan_node.open(&_runtime_state);
    ASSERT_TRUE(status.ok());

}

}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    doris::CpuInfo::init();
    return RUN_ALL_TESTS();
}