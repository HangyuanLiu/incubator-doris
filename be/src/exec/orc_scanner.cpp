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

#include <util/arrow/stream.h>
#include "exec/orc_scanner.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/mem_tracker.h"
#include "runtime/raw_value.h"
#include "runtime/stream_load/load_stream_mgr.h"
#include "runtime/stream_load/stream_load_pipe.h"
#include "runtime/tuple.h"
#include "exec/parquet_reader.h"
#include "exprs/expr.h"
#include "exec/text_converter.h"
#include "exec/text_converter.hpp"
#include "exec/local_file_reader.h"
#include "exec/broker_reader.h"
#include "exec/decompressor.h"

namespace doris {

    ORCScanner::ORCScanner(RuntimeState* state,
                                   RuntimeProfile* profile,
                                   const TBrokerScanRangeParams& params,
                                   const std::vector<TBrokerRangeDesc>& ranges,
                                   const std::vector<TNetworkAddress>& broker_addresses,
                                   ScannerCounter* counter) : BaseScanner(state, profile, params, counter),
                                                              _ranges(ranges),
                                                              _broker_addresses(broker_addresses),
            // _splittable(params.splittable),
                                                              _cur_file_reader(nullptr),
                                                              _next_range(0),
                                                              _cur_file_eof(false),
                                                              _scanner_eof(false) {
    }

ORCScanner::~ORCScanner() {
    close();
}

Status ORCScanner::open() {
    return BaseScanner::open();
}

Status ORCScanner::get_next(Tuple* tuple, MemPool* tuple_pool, bool* eof) {
    SCOPED_TIMER(_read_timer);
    // Get one line
    while (!_scanner_eof) {
        if (_cur_file_reader == nullptr || _cur_file_eof) {
            RETURN_IF_ERROR(open_next_reader());
            // If there isn't any more reader, break this
            if (_scanner_eof) {
                continue;
            }
            _cur_file_eof = false;
       }


        std::cout<<_current_line_of_group << "," << _rows_of_group << "," << _current_group << "," << _total_groups << std::endl;

        //RETURN_IF_ERROR(_cur_file_reader->read(_src_tuple, _src_slot_descs, tuple_pool, &_cur_file_eof));
        if (_current_line_of_group >= _rows_of_group) { // read next row group
            // ++_current_group;
            if (_current_group > _total_groups) {
                //_parquet_column_ids.clear(); //TODO: need clear?
                *eof = true;
                return Status::OK();
            }

            _current_line_of_group = 0;
            //_rows_of_group =xxx

            ORC_UNIQUE_PTR<orc::RowReader> rowReader = _reader->createRowReader(_rowReaderOptions);
            std::cout << _reader->getType().toString() << std::endl;
            _batch = rowReader->createRowBatch(_reader->getNumberOfRows());
            _row_batch.reset(new RowBatch(*_row_desc.get(), _reader->getNumberOfRows(), tuple_pool->mem_tracker()));
            _row_batch->commit_rows(_reader->getNumberOfRows());

            rowReader->next(*_batch.get());
            std::cout << "batch : " << _batch->numElements << ",  numberOfRows : " << _reader->getNumberOfRows() << std::endl;

            std::vector<orc::ColumnVectorBatch*> _batch_vec = ((orc::StructVectorBatch*) _batch.get())->fields;


            std::vector<SlotDescriptor*> slot_descs;
            for (auto tuple_desc : _row_desc->tuple_descriptors()) {
                for (auto desc : tuple_desc->slots()) {
                    slot_descs.push_back(desc);
                }
            }
            size_t num_fields = slot_descs.size();

            for (int column_ipos = 0; column_ipos < _batch_vec.size() ; ++column_ipos ) {
                orc::ColumnVectorBatch* b = _batch_vec[column_ipos];
                auto slot_desc = _src_slot_descs[column_ipos];
                _cur_slot_ref.reset(new SlotRef(slot_descs[column_ipos]));
                RETURN_IF_ERROR(_cur_slot_ref->prepare(slot_descs[column_ipos], *_row_desc));

                for (uint64_t r = 0; r < b->numElements; ++r) {

                    auto row = _row_batch->get_row(r);
                    auto t = _cur_slot_ref->get_tuple(row);

                    int32_t wbytes = 0;
                    uint8_t tmp_buf[128] = {0};
                    switch (_reader->getType().getSubtype(r)->getKind()) {
                        case orc::INT: {
                            int32_t value = ((orc::LongVectorBatch *) b)->data[r];
                            wbytes = sprintf((char *) tmp_buf, "%d", value);

                            t->set_not_null(slot_desc->null_indicator_offset());
                            void *slot = t->get_slot(slot_desc->tuple_offset());
                            StringValue *str_slot = reinterpret_cast<StringValue *>(slot);
                            str_slot->ptr = reinterpret_cast<char *>(tuple_pool->allocate(wbytes));
                            memcpy(str_slot->ptr, value, wbytes);
                            str_slot->len = wbytes;
                            break;
                        }
                        case orc::LONG: {
                            int64_t value = ((orc::LongVectorBatch *) b)->data[r];
                            wbytes = sprintf((char *) tmp_buf, "%ld", value);

                            t->set_not_null(slot_desc->null_indicator_offset());
                            void *slot = t->get_slot(slot_desc->tuple_offset());
                            StringValue *str_slot = reinterpret_cast<StringValue *>(slot);
                            str_slot->ptr = reinterpret_cast<char *>(tuple_pool->allocate(wbytes));
                            memcpy(str_slot->ptr, value, wbytes);
                            str_slot->len = wbytes;
                            break;
                        }
                        default:
                            std::cout << "ORC ERROR" << std::endl;
                    }
                }
            }

            Tuple::to_string(_row_batch->get_row(0)->get_tuple(0), *(_row_desc->tuple_descriptors()[0]));
        } else {
            //_src_tuple = (*_row_batch)->get_row(_current_line_of_group)->get_tuple(0);
        }


        // range of current file
        const TBrokerRangeDesc &range = _ranges.at(_next_range - 1);
        if (range.__isset.num_of_columns_from_file) {
            fill_slots_of_columns_from_path(range.num_of_columns_from_file, range.columns_from_path);
            {
                COUNTER_UPDATE(_rows_read_counter, 1);
                SCOPED_TIMER(_materialize_timer);
                //TODO : why ?
                if (fill_dest_tuple(Slice(), tuple, tuple_pool)) {
                    break;// break if true
                }
            }
        }
    }
    if (_scanner_eof) {
        *eof = true;
    } else {
        *eof = false;
    }
    return Status::OK();
}

Status ORCScanner::open_next_reader() {
    // open_file_reader
    if (_cur_file_reader != nullptr) {
        if (_stream_load_pipe != nullptr) {
            _stream_load_pipe.reset();
            _cur_file_reader = nullptr;
        } else {
            delete _cur_file_reader;
            _cur_file_reader = nullptr;
        }
    }

    while (true) {
        if (_next_range >= _ranges.size()) {
            _scanner_eof = true;
            return Status::OK();
        }
        const TBrokerRangeDesc &range = _ranges[_next_range++];
        std::unique_ptr<FileReader> file_reader;
        switch (range.file_type) {
            case TFileType::FILE_LOCAL: {
                file_reader.reset(new LocalFileReader(range.path, range.start_offset));
                break;
            }
            case TFileType::FILE_BROKER: {
                int64_t file_size = 0;
                // for compatibility
                if (range.__isset.file_size) { file_size = range.file_size; }
                file_reader.reset(new BrokerReader(_state->exec_env(), _broker_addresses, _params.properties,
                                                   range.path, range.start_offset, file_size));
                break;
            }
#if 0
            case TFileType::FILE_STREAM:
    {
        _stream_load_pipe = _state->exec_env()->load_stream_mgr()->get(range.load_id);
        if (_stream_load_pipe == nullptr) {
            return Status::InternalError("unknown stream load id");
        }
        _cur_file_reader = _stream_load_pipe.get();
        break;
    }
#endif
            default: {
                std::stringstream ss;
                ss << "Unknown file type, type=" << range.file_type;
                return Status::InternalError(ss.str());
            }
        }
        RETURN_IF_ERROR(file_reader->open());
        if (file_reader->size() == 0) {
            file_reader->close();
            continue;
        }

        std::unique_ptr<orc::InputStream> inStream = std::unique_ptr<orc::InputStream>(new ORCFileStream(file_reader.release()));
        _reader = orc::createReader(std::move(inStream), _options);
        _total_groups = _reader->getNumberOfStripes();
        return Status::OK();
    }
}

    void ORCScanner::close() {
        if (_cur_file_reader != nullptr) {
            if (_stream_load_pipe != nullptr) {
                _stream_load_pipe.reset();
                _cur_file_reader = nullptr;
            } else {
                delete _cur_file_reader;
                _cur_file_reader = nullptr;
            }
        }
    }

}
