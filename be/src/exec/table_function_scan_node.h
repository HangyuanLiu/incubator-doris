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

#ifndef DORIS_TABLE_FUNCTION_SCAN_NODE_H
#define DORIS_TABLE_FUNCTION_SCAN_NODE_H

#include "exec/scan_node.h"

namespace doris {
class TableFunctionScanNode : public ScanNode {
public:
    TableFunctionScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& desc);
    ~TableFunctionScanNode();

    // Called after create this scan node
    virtual Status init(const TPlanNode& tnode, RuntimeState* state = nullptr);

    virtual Status prepare(RuntimeState* state);

    virtual Status open(RuntimeState* state);

    virtual Status get_next(RuntimeState* state, RowBatch* row_batch, bool* eos);

    virtual Status close(RuntimeState* state);

    // No use
    virtual Status set_scan_ranges(const std::vector<TScanRangeParams>& scan_ranges);

protected:
    virtual void debug_string(int indentatial_level, std::stringstream* out) const;

private:
    TupleId _tuple_id;
    RuntimeState* _runtime_state;
    TupleDescriptor* _tuple_desc;
    std::map<std::string, SlotDescriptor*> _slots_map;
    std::unique_ptr<MemPool> _mem_pool;

    TFunction _fn;
    FunctionContext* _context;
};

}


#endif //DORIS_TABLE_FUNCTION_SCAN_NODE_H
