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

#include "stream.h"

#include <arrow/array.h>
#include <arrow/buffer.h>
#include <arrow/io/memory.h>
#include <arrow/memory_pool.h>
#include <arrow/type.h>
#include <arrow/visitor.h>
#include <arrow/visitor_inline.h>
#include <arrow/record_batch.h>

#include <memory>

#include "exprs/slot_ref.h"
#include "gutil/strings/substitute.h"
#include "runtime/row_batch.h"
#include "runtime/descriptors.h"
#include "runtime/descriptor_helper.h"
#include "util/arrow/utils.h"

namespace doris {


// Convert Arrow Array to String
class ToStringConverter : public arrow::ArrayVisitor {
public:
    using arrow::ArrayVisitor::Visit;

    ToStringConverter(const arrow::RecordBatch& batch,
                      const RowDescriptor& row_desc,
                      MemPool* tuple_pool)
                      : _batch(batch), _row_desc(row_desc), _tuple_pool(tuple_pool) {

    }

#define PRIMITIVE_VISIT(TYPE) \
    arrow::Status Visit(const arrow::TYPE& array) override { \
        return _visit(array); \
    }

    PRIMITIVE_VISIT(Int8Array);
    PRIMITIVE_VISIT(Int16Array);
    PRIMITIVE_VISIT(Int32Array);
    PRIMITIVE_VISIT(Int64Array);
    PRIMITIVE_VISIT(FloatArray);
    PRIMITIVE_VISIT(DoubleArray);

#undef PRIMITIVE_VISIT

    Status convert(std::shared_ptr<RowBatch>* result);

private:

    template<typename T>
    typename std::enable_if<std::is_base_of<arrow::PrimitiveCType, typename T::TypeClass>::value,
            arrow::Status>::type
    _visit(const T& array) {
        std::cout << "_visit : " << std::endl;
        for (size_t i = 0; i < array.length(); ++i) {
             std::cout << array.Value(i)  << "," ;
        }
        std::cout << std::endl;

        for (size_t i = 0; i < array.length(); ++i) {
            auto row = _output->get_row(i);
            auto tuple = _cur_slot_ref->get_tuple(row);
            if (array.IsValid(i)) {
                tuple->set_not_null(_cur_slot_ref->null_indicator_offset());
                StringValue* str_slot = reinterpret_cast<StringValue*>(_cur_slot_ref->get_slot(row));

                //int32_t len = sizeof(typename T::TypeClass::c_type);
                typename T::TypeClass::c_type value = array.Value(i);
                std::string value_str = std::to_string(value);

                str_slot->ptr = reinterpret_cast<char*>(_tuple_pool->allocate(value_str.length()));
                memcpy(str_slot->ptr, (uint8_t*)(value_str.c_str()), value_str.length());
                str_slot->len = value_str.length();
                
                std::cout << "value : " << std::string(str_slot->ptr, str_slot->len) << std::endl;
                //*(typename T::TypeClass::c_type*)slot = raw_values[i];
            } else {
                tuple->set_null(_cur_slot_ref->null_indicator_offset());
            }
        }
        
        return arrow::Status::OK();
    }

private:
    const arrow::RecordBatch& _batch;
    const RowDescriptor& _row_desc;
    MemPool* _tuple_pool;

    std::unique_ptr<SlotRef> _cur_slot_ref;
    std::shared_ptr<RowBatch> _output;
};

Status ToStringConverter:: convert(std::shared_ptr<RowBatch>* result) {
    std::vector<SlotDescriptor*> slot_descs;
    for (auto tuple_desc : _row_desc.tuple_descriptors()) {
        for (auto desc : tuple_desc->slots()) {
            slot_descs.push_back(desc);
        }
    }
    size_t num_fields = slot_descs.size();
    if (num_fields != _batch.schema()->num_fields()) {
        return Status::InvalidArgument("Schema not match");
    }
    // TODO(zc): check if field type match

    size_t num_rows = _batch.num_rows();
    _output.reset(new RowBatch(_row_desc, num_rows, _tuple_pool->mem_tracker()));
    _output->commit_rows(num_rows);
    auto pool = _output->tuple_data_pool();
    for (size_t row_id = 0; row_id < num_rows; ++row_id) {
        auto row = _output->get_row(row_id);
        for (int tuple_id = 0; tuple_id < _row_desc.tuple_descriptors().size(); ++tuple_id) {
            auto tuple_desc = _row_desc.tuple_descriptors()[tuple_id];
            auto tuple = pool->allocate(tuple_desc->byte_size());
            row->set_tuple(tuple_id, (Tuple*)tuple);
        }
    }
    for (size_t idx = 0; idx < num_fields; ++idx) {
        _cur_slot_ref.reset(new SlotRef(slot_descs[idx]));
        RETURN_IF_ERROR(_cur_slot_ref->prepare(slot_descs[idx], _row_desc));
        auto arrow_st = arrow::VisitArrayInline(*_batch.column(idx), this);
        if (!arrow_st.ok()) {
            return to_status(arrow_st);
        }
    }

    *result = std::move(_output);

    return Status::OK();
}

Status convert_to_stream(const arrow::RecordBatch& batch,
                                const RowDescriptor& row_desc,
                                MemPool* tuple_pool,
                                std::shared_ptr<RowBatch>* result) {
    ToStringConverter converter(batch, row_desc, tuple_pool);
    return converter.convert(result);
}

}

