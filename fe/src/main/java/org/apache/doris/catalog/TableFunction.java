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

import com.google.common.collect.Maps;
import com.google.gson.Gson;
import org.apache.doris.analysis.CreateFunctionStmt;
import org.apache.doris.analysis.FunctionName;
import org.apache.doris.common.io.Text;
import org.apache.doris.thrift.TFunction;
import org.apache.doris.thrift.TTableFunction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class TableFunction extends Function {

    private static final Logger LOG = LogManager.getLogger(TableFunction.class);

    // The name inside the binary at location_ that contains this particular
    // function. e.g. org.example.MyUdf.class.
    private String symbolName;
    private List<FnTableArgs> tableRetType;

    // Only used for serialization
    protected TableFunction() {
    }

    public TableFunction(FunctionName fnName,  Type[] argTypes, List<FnTableArgs> retType, boolean hasVarArgs) {
        super(fnName, argTypes, Type.NULL, hasVarArgs);
        tableRetType = retType;
    }

    public static TableFunction createUdtf(FunctionName name, Type[] argTypes, List<FnTableArgs> retType, boolean hasVarArgs) {
        TableFunction fn = new TableFunction(name, argTypes, retType, hasVarArgs);
        return fn;
    }

    public String getSymbolName() { return symbolName; }
    public String getTableFnReturnString() {
        return "getTableFnReturnString";
    }

    @Override
    public String toSql(boolean ifNotExists) {
        StringBuilder sb = new StringBuilder("CREATE FUNCTION ");

        sb.append(dbName() + "." + signatureString() + "\n")
                .append(" RETURN TABLE " + getTableFnReturnString() + "\n")
                .append(" LOCATION '" + getLocation() + "'\n")
                .append(" SYMBOL = '" + getSymbolName() + "'\n");
        return sb.toString();
    }

    @Override
    public TFunction toThrift() {
        TFunction fn = super.toThrift();
        fn.setTable_fn(new TTableFunction());
        fn.getTable_fn().setSysmbol(symbolName);
        return fn;
    }

    @Override
    public void write(DataOutput output) throws IOException {
        // 1. type
        FunctionType.TABLE.write(output);
        // 2. parent
        super.writeFields(output);
        // 3. symbols
        Text.writeString(output, symbolName);
        // 4. return table type
        for(int i = 0; i < tableRetType.size(); ++i) {
            Text.writeString(output, tableRetType.get(i).name);
            ColumnType.write(output, tableRetType.get(i).type);
        }
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        super.readFields(input);
        symbolName = Text.readString(input);
    }

    @Override
    public String getProperties() {
        Map<String, String> properties = Maps.newHashMap();
        properties.put(CreateFunctionStmt.OBJECT_FILE_KEY, getLocation().toString());
        properties.put(CreateFunctionStmt.MD5_CHECKSUM, checksum);
        properties.put(CreateFunctionStmt.SYMBOL_KEY, symbolName);
        return new Gson().toJson(properties);
    }
}
