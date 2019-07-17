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

import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.thrift.TTimeLiteral;
import org.apache.doris.thrift.TExprNode;
import org.apache.doris.thrift.TExprNodeType;

import com.google.common.base.Preconditions;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

public class TimeLiteral extends LiteralExpr {
    private static final Logger LOG = LogManager.getLogger(TimeLiteral.class);
    private long time;

    //fixme public -> private
    public TimeLiteral() {
        super();
        this.type = Type.TIME;
    }

    public TimeLiteral(long time) {
        super();
        this.time = time;
        analysisDone();
    }

    protected TimeLiteral(TimeLiteral other) {
        super(other);
        time = other.time;
    }

    @Override
    public Expr clone() {
        return new TimeLiteral(this);
    }

    @Override
    public boolean isMinValue() {
        return  false;
    }

    @Override
    public Object getRealValue() {
        System.out.println("getRealValue");
        return null;
    }

    // Date column and Datetime column's hash value is not same.
    @Override
    public ByteBuffer getHashValue(PrimitiveType type) {
        String value = "";
        ByteBuffer buffer;
        try {
            buffer = ByteBuffer.wrap(value.getBytes("UTF-8"));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return buffer;
    }

    @Override
    public int compareLiteral(LiteralExpr expr) {
        if (expr instanceof NullLiteral) {
            return 1;
        }

        if (expr == MaxLiteral.MAX_VALUE) {
            return -1;
        }
        // date time will not overflow when doing addition and subtraction
        return Long.signum(getLongValue() - expr.getLongValue());
    }

    @Override
    public String toSqlImpl() {
        return "'" + getStringValue() + "'";
    }

    @Override
    public String getStringValue() {
        System.out.println("getStringValue");
        //return new String("2019-08-02");
        //return TimeUtils.format(date, type);
        return "";
    }

    @Override
    public long getLongValue() {
        return 0;
    }

    @Override
    public double getDoubleValue() {
        return 0;
    }

    @Override
    protected void toThrift(TExprNode msg) {
        msg.node_type = TExprNodeType.TIME_LITERAL;
        msg.time_literal = new TTimeLiteral(time);
    }

    @Override
    protected Expr uncheckedCastTo(Type targetType) throws AnalysisException {
        if (targetType.isDateType()) {
            return this;
        } else if (targetType.isStringType()) {
            return new StringLiteral(getStringValue());
        }
        Preconditions.checkState(false);
        return this;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        out.writeLong(time);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        time = in.readLong();
    }

    public static TimeLiteral read(DataInput in) throws IOException {
        TimeLiteral literal = new TimeLiteral();
        literal.readFields(in);
        return literal;
    }
}
