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

import org.apache.doris.catalog.FunctionSet;

public class MVColumnBitmapUnionPatten implements MVColumnPatten {
    @Override
    public boolean match(Expr expr) {
        if (!(expr instanceof FunctionCallExpr)) {
            return false;
        }
        FunctionCallExpr fnExpr = (FunctionCallExpr) expr;
        String fnNameString = fnExpr.getFnName().getFunction();
        if (!fnNameString.equalsIgnoreCase(FunctionSet.BITMAP_UNION)) {
            return false;
        }
        if (!(fnExpr.getChild(0) instanceof FunctionCallExpr)) {
            return false;
        }
        FunctionCallExpr child0FnExpr = (FunctionCallExpr) fnExpr.getChild(0);
        if (!child0FnExpr.getFnName().getFunction().equalsIgnoreCase("to_bitmap")) {
            return false;
        }
        if (child0FnExpr.getChild(0) instanceof SlotRef) {
            return true;
        } else if (child0FnExpr.getChild(0) instanceof CastExpr) {
            CastExpr castExpr = (CastExpr) child0FnExpr.getChild(0);
            if (!(castExpr.getChild(0) instanceof SlotRef)) {
                return false;
            }
            return true;
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        return "bitmap_union(to_bitmap(column)) or bitmap_union(bitmap_column) in agg table";
    }
}
