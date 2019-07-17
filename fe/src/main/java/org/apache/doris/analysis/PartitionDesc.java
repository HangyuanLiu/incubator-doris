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

package org.apache.doris.analysis;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;

import org.apache.commons.lang.NotImplementedException;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class PartitionDesc implements Writable {

    protected PartitionType type;

    public PartitionDesc() {
    }

    public void analyze(List<ColumnDef> columnDefs, Map<String, String> otherProperties) throws AnalysisException {
        throw new NotImplementedException();
    }

    public PartitionType getType() {
        return type;
    }

    public String toSql() {
        throw new NotImplementedException();
    }

    public PartitionInfo toPartitionInfo(List<Column> columns, Map<String, Long> partitionNameToId)
            throws DdlException {
        throw new NotImplementedException();
    }

    public static PartitionDesc read(DataInput in) throws IOException {
        PartitionType type = PartitionType.valueOf(Text.readString(in));
        if (type == PartitionType.RANGE) {
            PartitionDesc desc = new RangePartitionDesc();
            desc.readFields(in);
            return desc;
        } else {
            throw new IOException("Unknow partition type");
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, type.name());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        throw new NotImplementedException();
    }
}
