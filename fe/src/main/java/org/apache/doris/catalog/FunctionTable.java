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

package org.apache.doris.catalog;

import org.apache.doris.common.DdlException;
import org.apache.doris.thrift.TFunctionTable;
import org.apache.doris.thrift.TTableDescriptor;
import org.apache.doris.thrift.TTableType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.zip.Adler32;

public class FunctionTable extends Table {
    private static final Logger LOG = LogManager.getLogger(FunctionTable.class);

    private String symbol; // need a symbol

    public FunctionTable() {
        super(TableType.TABLEFUNCTION);
    }

    public FunctionTable(long id, String name, List<Column> schema, Map<String, String> properties) throws DdlException {
        super(id, name, TableType.TABLEFUNCTION, schema);
        //validate
    }


    public TTableDescriptor toThrift() {
        TFunctionTable tFunctionTable = new TFunctionTable();
        TTableDescriptor tTableDescriptor = new TTableDescriptor(getId(), TTableType.FUNCTION_TABLE,
                fullSchema.size(), 0, getName(), "");
        tTableDescriptor.setFnTable(tFunctionTable);
        return tTableDescriptor;
    }


    @Override
    public int getSignature(int signatureVersion) {
        Adler32 adler32 = new Adler32();
        return Math.abs((int) adler32.getValue());
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
    }
}
