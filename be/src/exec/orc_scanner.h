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

#pragma once

#include <memory>
#include <vector>
#include <string>
#include <map>
#include <sstream>
#include <arrow/record_batch.h>
#include <arrow/adapters/orc/adapter.h>

#include "exec/base_scanner.h"
#include "exec/file_reader.h"
#include "common/status.h"
#include "gen_cpp/PlanNodes_types.h"
#include "gen_cpp/Types_types.h"
#include "runtime/mem_pool.h"
#include "runtime/row_batch.h"
#include "util/slice.h"
#include "util/runtime_profile.h"

#include "orc/OrcFile.hh"


namespace doris {

class FileReader;
class Tuple;
class SlotDescriptor;
class Slice;
class ParquetReaderWrap;
class RuntimeState;
class ExprContext;
class TupleDescriptor;
class TupleRow;
class RowDescriptor;
class MemTracker;
class RuntimeProfile;
class StreamLoadPipe;

class ORCFileStream : public orc::InputStream {
public:
    ORCFileStream(FileReader *file) : _file(file) {

    }
    ~ORCFileStream() {

    }
    /**
     * Get the total length of the file in bytes.
     */
    uint64_t getLength() const{
        return _file->size();
    }

    /**
     * Get the natural size for reads.
     * @return the number of bytes that should be read at once
     */
    uint64_t getNaturalReadSize() const{
        return 128 * 1024;
    }

    /**
     * Read length bytes from the file starting at offset into
     * the buffer starting at buf.
     * @param buf the starting position of a buffer.
     * @param length the number of bytes to read.
     * @param offset the position in the stream to read from.
     */
     void read(void* buf,
                      uint64_t length,
                      uint64_t offset) {
        int64_t reads = 0;
        while(length != 0) {
            Status result = _file->readat(offset, length, &reads, buf);
            if (!result.ok()) {
                //*bytes_read = 0;
                //return arrow::Status::IOError("Readat failed.");
                return ;
            }
            if (reads == 0) {
                break;
            }
            //*bytes_read += reads;// total read bytes
            length -= reads; // remained bytes
            offset += reads;
            buf = (char*)buf + reads;
        }
        //return arrow::Status::OK();
        return ;
    }

    /**
     * Get the name of the stream for error messages.
     */
    const std::string& getName() const{
        return _filename;
    }

private:
    FileReader* _file;
    std::string _filename = "fuck";
};



class ORCFile : public arrow::io::RandomAccessFile {
public:
    ORCFile(FileReader *file) : _file(file) {

    }

    virtual ~ORCFile() {
        Close();
    }

    arrow::Status Read(int64_t nbytes, int64_t *bytes_read, void *buffer) override {
        bool eof = false;
        size_t data_size = 0;
        do {
            data_size = nbytes;
            Status result = _file->read((uint8_t*)buffer, &data_size, &eof);
            if (!result.ok()) {
                return arrow::Status::IOError("Read failed.");
            }
            if (eof) {
                break;
            }
            *bytes_read += data_size; // total read bytes
            nbytes -= data_size; // remained bytes
            buffer = (uint8_t*)buffer + data_size;
        } while (nbytes != 0);
        return arrow::Status::OK();
    }

    arrow::Status ReadAt(int64_t position, int64_t nbytes, int64_t *bytes_read,
                         void *out) override {
        int64_t reads = 0;
        while(nbytes != 0) {
            Status result = _file->readat(position, nbytes, &reads, out);
            if (!result.ok()) {
                *bytes_read = 0;
                return arrow::Status::IOError("Readat failed.");
            }
            if (reads == 0) {
                break;
            }
            *bytes_read += reads;// total read bytes
            nbytes -= reads; // remained bytes
            position += reads;
            out = (char*)out + reads;
        }
        return arrow::Status::OK();
    }

    arrow::Status GetSize(int64_t *size) override {
        *size = _file->size();
        return arrow::Status::OK();
    }

    arrow::Status Seek(int64_t position) override {
        _file->seek(position);
        return arrow::Status::OK();
    }

    arrow::Status Read(int64_t nbytes, std::shared_ptr<arrow::Buffer> *out) override {
        return arrow::Status::NotImplemented("Not Supported.");
    }

    arrow::Status Tell(int64_t *position) const override {
        _file->tell(position);
        return arrow::Status::OK();
    }

    arrow::Status Close() override {
        if (_file) {
            _file->close();
            delete _file;
            _file = nullptr;
        }
        return arrow::Status::OK();
    }

    bool closed() const override {
        if (_file) {
            return _file->closed();
        } else {
            return true;
        }
    }

private:
    FileReader *_file;
};


// Broker scanner convert the data read from broker to doris's tuple.
class ORCScanner : public BaseScanner {
public:
    ORCScanner(RuntimeState* state,
    RuntimeProfile *profile,
    const TBrokerScanRangeParams &params,
    const std::vector<TBrokerRangeDesc> &ranges,
    const std::vector<TNetworkAddress> &broker_addresses,ScannerCounter* counter);

    ~ORCScanner();

    // Open this scanner, will initialize information need to
    virtual Status open();

    // Get next tuple
    virtual Status get_next(Tuple *tuple, MemPool *tuple_pool, bool *eof);

    // Close this scanner
    virtual void close();

private:
    // Read next buffer from reader
    Status open_next_reader();

private:
    //const TBrokerScanRangeParams& _params;
    const std::vector<TBrokerRangeDesc> &_ranges;
    const std::vector<TNetworkAddress> &_broker_addresses;

    // Reader
    ParquetReaderWrap *_cur_file_reader;
    int _next_range;
    bool _cur_file_eof; // is read over?
    bool _scanner_eof;

    // used to hold current StreamLoadPipe
    std::shared_ptr<StreamLoadPipe> _stream_load_pipe;

    // orc file reader object
    std::shared_ptr<::arrow::RecordBatchReader> _rb_batch;
    std::shared_ptr<arrow::RecordBatch> _batch;
    std::unique_ptr<arrow::adapters::orc::ORCFileReader> _reader;
    std::shared_ptr<ORCFile> _orc;
    std::shared_ptr<arrow::Schema> _schema;

    std::shared_ptr<RowBatch>* _row_batch;

    int _total_groups; // groups in a orc file
    int _current_group;
    int _rows_of_group; // rows in a group.
    int _current_line_of_group;
};




}
