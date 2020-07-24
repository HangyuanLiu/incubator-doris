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

package org.apache.doris.sql.utframe;

import org.apache.commons.lang.StringUtils;
import org.apache.doris.alter.AlterJobV2;
import org.apache.doris.analysis.*;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.planner.Planner;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QueryState;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TExplainLevel;

import java.io.IOException;
import java.util.Map;
import java.util.stream.Stream;

public class DorisAssert {

    private ConnectContext ctx;

    public DorisAssert() throws IOException {
        this.ctx = UtFrameUtils.createDefaultCtx();
    }

    public DorisAssert withEnableMV() {
        ctx.getSessionVariable().setTestMaterializedView(true);
        Config.enable_materialized_view = true;
        return this;
    }

    public DorisAssert withDatabase(String dbName) throws Exception {
        CreateDbStmt createDbStmt =
                (CreateDbStmt) UtFrameUtils.parseAndAnalyzeStmt("create database " + dbName + ";", ctx);
        Catalog.getCurrentCatalog().createDb(createDbStmt);
        return this;
    }

    public DorisAssert useDatabase(String dbName) {
        ctx.setDatabase(ClusterNamespace.getFullName(SystemInfoService.DEFAULT_CLUSTER, dbName));
        return this;
    }

    public DorisAssert withTable(String sql) throws Exception {
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, ctx);
        Catalog.getCurrentCatalog().createTable(createTableStmt);
        return this;
    }

    public DorisAssert dropTable(String tableName) throws Exception {
        DropTableStmt dropTableStmt =
                (DropTableStmt) UtFrameUtils.parseAndAnalyzeStmt("drop table " + tableName + ";", ctx);
        Catalog.getCurrentCatalog().dropTable(dropTableStmt);
        return this;
    }

    public QueryAssert query(String sql) {
        return new QueryAssert(ctx, sql);
    }

    public class QueryAssert {
        private ConnectContext connectContext;
        private String sql;

        public QueryAssert(ConnectContext connectContext, String sql) {
            this.connectContext = connectContext;
            this.sql = sql;
        }

        public String explainQuery() throws Exception {
            StmtExecutor stmtExecutor = new StmtExecutor(connectContext, "explain " + sql);
            stmtExecutor.execute();
            QueryState queryState = connectContext.getState();
            if (queryState.getStateType() == QueryState.MysqlStateType.ERR) {
                switch (queryState.getErrType()){
                    case ANALYSIS_ERR:
                        throw new AnalysisException(queryState.getErrorMessage());
                    case OTHER_ERR:
                    default:
                        throw new Exception(queryState.getErrorMessage());
                }
            }
            Planner planner = stmtExecutor.planner();
            String explainString = planner.getExplainString(planner.getFragments(), TExplainLevel.VERBOSE);
            System.out.println(explainString);
            return explainString;
        }
    }
}
