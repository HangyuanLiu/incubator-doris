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

#include "exec/table_function_scan_node.h"

#include <algorithm>
#include <boost/foreach.hpp>
#include <sstream>
#include <iostream>
#include <utility>
#include <string>

#include "codegen/llvm_codegen.h"
#include "common/logging.h"
#include "exprs/expr.h"
#include "exprs/binary_predicate.h"
#include "exprs/in_predicate.h"
#include "gen_cpp/PlanNodes_types.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "runtime/row_batch.h"
#include "runtime/string_value.h"
#include "runtime/tuple_row.h"
#include "util/runtime_profile.h"
#include "util/thread_pool.hpp"
#include "util/debug_util.h"
#include "util/priority_thread_pool.hpp"
#include "agent/cgroups_mgr.h"
#include "common/resource_tls.h"
#include <boost/variant.hpp>
#include <exprs/builtin_table_fn.h>

namespace doris {

TableFunctionScanNode::TableFunctionScanNode(ObjectPool *pool, const TPlanNode &tnode,
        const DescriptorTbl &descs) :
    ScanNode(pool, tnode, descs),
    _tuple_id(tnode.table_func_scan_node.tuple_id),
    _fn(tnode.table_func_scan_node.fn) {
}

TableFunctionScanNode::~TableFunctionScanNode() {
}

Status TableFunctionScanNode::prepare(RuntimeState *state) {
    VLOG(1) << "TableFunctionScanNode prepare";

    if (NULL == state) {
        return Status::InternalError("input pointer is NULL.");
    }
    RETURN_IF_ERROR(ScanNode::prepare(state));
    // get tuple desc

    _tuple_desc = state->desc_tbl().get_tuple_descriptor(_tuple_id);
    if (_tuple_desc == nullptr) {
        std::stringstream ss;
        ss << "Failed to get tuple descriptor, _tuple_id=" << _tuple_id;
        return Status::InternalError(ss.str());
    }

    const FunctionTableDescriptor* function_table =
            static_cast<const FunctionTableDescriptor*>(_tuple_desc->table_desc());
    if (nullptr == function_table) {
        return Status::InternalError("function table pointer is null");
    }

    /*
    for (auto slot : _tuple_desc->slots()) {
        auto pair = _slots_map.emplace(slot->col_name(), slot);
        if (!pair.second) {
            std::stringstream ss;
            ss << "Failed to insert slot, col_name=" << slot->col_name();
            return Status::InternalError(ss.str());
        }
    }*/

    _mem_pool.reset(new(std::nothrow) MemPool(mem_tracker()));
    if (_mem_pool.get() == nullptr) {
        return Status::InternalError("new a mem pool failed.");
    }

    std::vector<doris_udf::FunctionContext::TypeDesc> return_type;
    std::vector<doris_udf::FunctionContext::TypeDesc> arg_types;
    for (int i = 0; i < 5; i++) {
        FunctionContext::TypeDesc typeDesc;
        typeDesc.type = FunctionContext::TYPE_INT;
        return_type.push_back(typeDesc);
        arg_types.push_back(typeDesc);
    }

    //TODO(lhy)
    _context = FunctionContextImpl::create_context(state, _mem_pool.get(), return_type, arg_types, 0, false);

    return Status::OK();
}

Status TableFunctionScanNode::open(RuntimeState* state) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(ExecNode::open(state));
    RETURN_IF_ERROR(exec_debug_action(TExecNodePhase::OPEN));
    RETURN_IF_CANCELLED(state);

    return Status::OK();
}

Status TableFunctionScanNode::get_next(RuntimeState* state, RowBatch* row_batch, bool* eos) {

    BuiltinTableFn::generate_rand(_context, 5, 5);
    RecordStore* store = _context->impl()->record_store();

    for (int i = 0; i < store->size(); ++i) {
        int row_idx = row_batch->add_row();
        TupleRow *row = row_batch->get_row(row_idx);
        row->set_tuple(0, reinterpret_cast<Tuple*> (store->get(i)));
    }
    *eos = true;
    return Status::OK();
}

Status TableFunctionScanNode::close(RuntimeState* state) {
    if (is_closed()) {
        return Status::OK();
    }
    RETURN_IF_ERROR(exec_debug_action(TExecNodePhase::CLOSE));

    return ScanNode::close(state);
}

void TableFunctionScanNode::debug_string(int indentation_level, stringstream* out) const {
    *out << string(indentation_level * 2, ' ');
    *out << "TableFunctionScanNode(tupleid=" << _tuple_id << " table=" << _fn.name.function_name;
    *out << ")" << std::endl;

    for (int i = 0; i < _children.size(); ++i) {
        _children[i]->debug_string(indentation_level + 1, out);
    }
}

Status TableFunctionScanNode::set_scan_ranges(const vector<TScanRangeParams>& scan_ranges) {
    return Status::OK();
}

} // namespace doris
