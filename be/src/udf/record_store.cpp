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

#include "udf.h"
#include "udf_internal.h"
#include "runtime/descriptors.h"

namespace doris_udf {

void Record::set_null(int idx) {
    auto *descriptor = *reinterpret_cast<doris::TupleDescriptor **>(this - sizeof(doris::TupleDescriptor *));
    doris::NullIndicatorOffset offset = descriptor->slots()[idx]->null_indicator_offset();
    char *null_indicator_byte = reinterpret_cast<char *>(this) + offset.byte_offset;
    *null_indicator_byte |= offset.bit_mask;
}

void Record::set_int(int idx, int val) {
    auto *descriptor = *reinterpret_cast<doris::TupleDescriptor **>(this - sizeof(doris::TupleDescriptor *));
    uint8_t *dst = (uint8_t *) this + descriptor->slots()[idx]->tuple_offset();
    memcpy(dst, reinterpret_cast<uint8_t *>(&val), sizeof(int));
}

void Record::set_string(int idx, const uint8_t *ptr, size_t len) {
    auto *descriptor = *reinterpret_cast<doris::TupleDescriptor **>(this - sizeof(doris::TupleDescriptor *));
    uint8_t *dst = (uint8_t *) this + descriptor->slots()[idx]->tuple_offset();
    memcpy(dst, &ptr, sizeof(char *));
    memcpy(dst + sizeof(char *), reinterpret_cast<uint8_t *>(&len), sizeof(size_t));
}

//RecordStore
RecordStore::RecordStore() : _impl(new doris::RecordStoreImpl()) {}

Record *RecordStore::allocate_record() {
    return _impl->allocate_record();
}

void RecordStore::append_record(Record *record) {
    _impl->append_record(record);
}

void RecordStore::free_record(Record *record) {
    _impl->free_record(record);
}

void *RecordStore::allocate(size_t size) {
    return (void *) _impl->allocate(size);
}

size_t RecordStore::size() {
    return _impl->size();
}

Record *RecordStore::get(int idx) {
    return _impl->get(idx);
}

RecordStore::~RecordStore() {
    delete _impl;
}

}