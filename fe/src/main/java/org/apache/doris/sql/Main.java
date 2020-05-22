package org.apache.doris.sql;

import com.google.common.collect.Lists;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.apache.doris.analysis.CreateDbStmt;
import org.apache.doris.analysis.CreateTableStmt;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.planner.DistributedPlanner;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.PlanNode;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.sql.analyzer.Analysis;
import org.apache.doris.sql.analyzer.StatementAnalyzer;
import org.apache.doris.sql.metadata.Metadata;
import org.apache.doris.sql.metadata.MetadataManager;
import org.apache.doris.sql.metadata.Session;
import org.apache.doris.sql.parser.AstBuilder;
import org.apache.doris.sql.parser.CaseInsensitiveStream;
import org.apache.doris.sql.parser.ParsingOptions;
import org.apache.doris.sql.parser.SqlBaseLexer;
import org.apache.doris.sql.parser.SqlBaseParser;
import org.apache.doris.sql.planner.LogicalPlanner;
import org.apache.doris.sql.planner.PhysicalPlanner;
import org.apache.doris.sql.planner.Plan;
import org.apache.doris.sql.planner.SimplePlanRewriter;
import org.apache.doris.sql.tree.Expression;
import org.apache.doris.sql.tree.Node;
import org.apache.doris.sql.tree.Statement;
import org.apache.doris.sql.utframe.UtFrameUtils;

import java.util.ArrayList;
import java.util.Optional;
import java.util.UUID;

public class Main {
    private static String runningDir = "./" + UUID.randomUUID().toString() + "/";
    private static ConnectContext connectContext;

    public static void main(String[] args) throws Exception {

        UtFrameUtils.createMinDorisCluster(runningDir);

        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        // create database
        String createDbStmtStr = "create database test;";
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseAndAnalyzeStmt(createDbStmtStr, connectContext);
        Catalog.getCurrentCatalog().createDb(createDbStmt);

        createTable("create table test.test1\n" +
                "(\n" +
                "    query_id varchar(48) comment \"Unique query id\",\n" +
                "    time datetime not null comment \"Query start time\",\n" +
                "    client_ip varchar(32) comment \"Client IP\",\n" +
                "    user varchar(64) comment \"User name\",\n" +
                "    db varchar(96) comment \"Database of this query\",\n" +
                "    state varchar(8) comment \"Query result state. EOF, ERR, OK\",\n" +
                "    query_time bigint comment \"Query execution time in millisecond\",\n" +
                "    scan_bytes bigint comment \"Total scan bytes of this query\",\n" +
                "    scan_rows bigint comment \"Total scan rows of this query\",\n" +
                "    return_rows bigint comment \"Returned rows of this query\",\n" +
                "    stmt_id int comment \"An incremental id of statement\",\n" +
                "    is_query tinyint comment \"Is this statemt a query. 1 or 0\",\n" +
                "    frontend_ip varchar(32) comment \"Frontend ip of executing this statement\",\n" +
                "    stmt varchar(2048) comment \"The original statement, trimed if longer than 2048 bytes\"\n" +
                ")\n" +
                "partition by range(time) ()\n" +
                "distributed by hash(query_id) buckets 1\n" +
                "properties(\n" +
                "    \"dynamic_partition.time_unit\" = \"DAY\",\n" +
                "    \"dynamic_partition.start\" = \"-30\",\n" +
                "    \"dynamic_partition.end\" = \"3\",\n" +
                "    \"dynamic_partition.prefix\" = \"p\",\n" +
                "    \"dynamic_partition.buckets\" = \"1\",\n" +
                "    \"dynamic_partition.enable\" = \"true\",\n" +
                "    \"replication_num\" = \"1\"\n" +
                ");");

        createTable("CREATE TABLE test.bitmap_table (\n" +
                "  `id` int(11) NULL COMMENT \"\",\n" +
                "  `id2` bitmap bitmap_union NULL\n" +
                ") ENGINE=OLAP\n" +
                "AGGREGATE KEY(`id`)\n" +
                "DISTRIBUTED BY HASH(`id`) BUCKETS 1\n" +
                "PROPERTIES (\n" +
                " \"replication_num\" = \"1\"\n" +
                ");");

        createTable("CREATE TABLE test.join1 (\n" +
                "  `dt` int(11) COMMENT \"\",\n" +
                "  `id` int(11) COMMENT \"\",\n" +
                "  `value` varchar(8) COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`dt`, `id`)\n" +
                "PARTITION BY RANGE(`dt`)\n" +
                "(PARTITION p1 VALUES LESS THAN (\"10\"))\n" +
                "DISTRIBUTED BY HASH(`id`) BUCKETS 10\n" +
                "PROPERTIES (\n" +
                "  \"replication_num\" = \"1\"\n" +
                ");");

        createTable("CREATE TABLE test.join2 (\n" +
                "  `dt` int(11) COMMENT \"\",\n" +
                "  `id` int(11) COMMENT \"\",\n" +
                "  `value` varchar(8) COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`dt`, `id`)\n" +
                "PARTITION BY RANGE(`dt`)\n" +
                "(PARTITION p1 VALUES LESS THAN (\"10\"))\n" +
                "DISTRIBUTED BY HASH(`id`) BUCKETS 10\n" +
                "PROPERTIES (\n" +
                "  \"replication_num\" = \"1\"\n" +
                ");");

        createTable("CREATE TABLE test.bitmap_table_2 (\n" +
                "  `id` int(11) NULL COMMENT \"\",\n" +
                "  `id2` bitmap bitmap_union NULL\n" +
                ") ENGINE=OLAP\n" +
                "AGGREGATE KEY(`id`)\n" +
                "DISTRIBUTED BY HASH(`id`) BUCKETS 1\n" +
                "PROPERTIES (\n" +
                " \"replication_num\" = \"1\"\n" +
                ");");

        createTable("CREATE TABLE test.hll_table (\n" +
                "  `id` int(11) NULL COMMENT \"\",\n" +
                "  `id2` hll hll_union NULL\n" +
                ") ENGINE=OLAP\n" +
                "AGGREGATE KEY(`id`)\n" +
                "DISTRIBUTED BY HASH(`id`) BUCKETS 1\n" +
                "PROPERTIES (\n" +
                " \"replication_num\" = \"1\"\n" +
                ");");

        createTable("CREATE TABLE test.`bigtable` (\n" +
                "  `k1` tinyint(4) NULL COMMENT \"\",\n" +
                "  `k2` smallint(6) NULL COMMENT \"\",\n" +
                "  `k3` int(11) NULL COMMENT \"\",\n" +
                "  `k4` bigint(20) NULL COMMENT \"\",\n" +
                "  `k5` decimal(9, 3) NULL COMMENT \"\",\n" +
                "  `k6` char(5) NULL COMMENT \"\",\n" +
                "  `k10` date NULL COMMENT \"\",\n" +
                "  `k11` datetime NULL COMMENT \"\",\n" +
                "  `k7` varchar(20) NULL COMMENT \"\",\n" +
                "  `k8` double MAX NULL COMMENT \"\",\n" +
                "  `k9` float SUM NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "AGGREGATE KEY(`k1`, `k2`, `k3`, `k4`, `k5`, `k6`, `k10`, `k11`, `k7`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`k1`) BUCKETS 5\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ");");

        createTable("CREATE TABLE test.`baseall` (\n" +
                "  `k1` tinyint(4) NULL COMMENT \"\",\n" +
                "  `k2` smallint(6) NULL COMMENT \"\",\n" +
                "  `k3` int(11) NULL COMMENT \"\",\n" +
                "  `k4` bigint(20) NULL COMMENT \"\",\n" +
                "  `k5` decimal(9, 3) NULL COMMENT \"\",\n" +
                "  `k6` char(5) NULL COMMENT \"\",\n" +
                "  `k10` date NULL COMMENT \"\",\n" +
                "  `k11` datetime NULL COMMENT \"\",\n" +
                "  `k7` varchar(20) NULL COMMENT \"\",\n" +
                "  `k8` double MAX NULL COMMENT \"\",\n" +
                "  `k9` float SUM NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "AGGREGATE KEY(`k1`, `k2`, `k3`, `k4`, `k5`, `k6`, `k10`, `k11`, `k7`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`k1`) BUCKETS 5\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ");");

        createTable("CREATE TABLE test.`dynamic_partition` (\n" +
                "  `k1` date NULL COMMENT \"\",\n" +
                "  `k2` smallint(6) NULL COMMENT \"\",\n" +
                "  `k3` int(11) NULL COMMENT \"\",\n" +
                "  `k4` bigint(20) NULL COMMENT \"\",\n" +
                "  `k5` decimal(9, 3) NULL COMMENT \"\",\n" +
                "  `k6` char(5) NULL COMMENT \"\",\n" +
                "  `k10` date NULL COMMENT \"\",\n" +
                "  `k11` datetime NULL COMMENT \"\",\n" +
                "  `k7` varchar(20) NULL COMMENT \"\",\n" +
                "  `k8` double MAX NULL COMMENT \"\",\n" +
                "  `k9` float SUM NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "AGGREGATE KEY(`k1`, `k2`, `k3`, `k4`, `k5`, `k6`, `k10`, `k11`, `k7`)\n" +
                "COMMENT \"OLAP\"\n" +
                "PARTITION BY RANGE (k1)\n" +
                "(\n" +
                "PARTITION p1 VALUES LESS THAN (\"2014-01-01\"),\n" +
                "PARTITION p2 VALUES LESS THAN (\"2014-06-01\"),\n" +
                "PARTITION p3 VALUES LESS THAN (\"2014-12-01\")\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(`k1`) BUCKETS 5\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"dynamic_partition.enable\" = \"true\",\n" +
                "\"dynamic_partition.start\" = \"-3\",\n" +
                "\"dynamic_partition.end\" = \"3\",\n" +
                "\"dynamic_partition.time_unit\" = \"day\",\n" +
                "\"dynamic_partition.prefix\" = \"p\",\n" +
                "\"dynamic_partition.buckets\" = \"1\"\n" +
                ");");

        createTable("CREATE TABLE test.`app_profile` (\n" +
                "  `event_date` date NOT NULL COMMENT \"\",\n" +
                "  `app_name` varchar(64) NOT NULL COMMENT \"\",\n" +
                "  `package_name` varchar(64) NOT NULL COMMENT \"\",\n" +
                "  `age` varchar(32) NOT NULL COMMENT \"\",\n" +
                "  `gender` varchar(32) NOT NULL COMMENT \"\",\n" +
                "  `level` varchar(64) NOT NULL COMMENT \"\",\n" +
                "  `city` varchar(64) NOT NULL COMMENT \"\",\n" +
                "  `model` varchar(64) NOT NULL COMMENT \"\",\n" +
                "  `brand` varchar(64) NOT NULL COMMENT \"\",\n" +
                "  `hours` varchar(16) NOT NULL COMMENT \"\",\n" +
                "  `use_num` int(11) SUM NOT NULL COMMENT \"\",\n" +
                "  `use_time` double SUM NOT NULL COMMENT \"\",\n" +
                "  `start_times` bigint(20) SUM NOT NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "AGGREGATE KEY(`event_date`, `app_name`, `package_name`, `age`, `gender`, `level`, `city`, `model`, `brand`, `hours`)\n"
                +
                "COMMENT \"OLAP\"\n" +
                "PARTITION BY RANGE(`event_date`)\n" +
                "(PARTITION p_20200301 VALUES [('2020-02-27'), ('2020-03-02')),\n" +
                "PARTITION p_20200306 VALUES [('2020-03-02'), ('2020-03-07')))\n" +
                "DISTRIBUTED BY HASH(`event_date`, `app_name`, `package_name`, `age`, `gender`, `level`, `city`, `model`, `brand`, `hours`) BUCKETS 1\n"
                +
                "PROPERTIES (\n" +
                " \"replication_num\" = \"1\"\n" +
                ");");

        createTable("CREATE TABLE test.`pushdown_test` (\n" +
                "  `k1` tinyint(4) NULL COMMENT \"\",\n" +
                "  `k2` smallint(6) NULL COMMENT \"\",\n" +
                "  `k3` int(11) NULL COMMENT \"\",\n" +
                "  `k4` bigint(20) NULL COMMENT \"\",\n" +
                "  `k5` decimal(9, 3) NULL COMMENT \"\",\n" +
                "  `k6` char(5) NULL COMMENT \"\",\n" +
                "  `k10` date NULL COMMENT \"\",\n" +
                "  `k11` datetime NULL COMMENT \"\",\n" +
                "  `k7` varchar(20) NULL COMMENT \"\",\n" +
                "  `k8` double MAX NULL COMMENT \"\",\n" +
                "  `k9` float SUM NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "AGGREGATE KEY(`k1`, `k2`, `k3`, `k4`, `k5`, `k6`, `k10`, `k11`, `k7`)\n" +
                "COMMENT \"OLAP\"\n" +
                "PARTITION BY RANGE(`k1`)\n" +
                "(PARTITION p1 VALUES [(\"-128\"), (\"-64\")),\n" +
                "PARTITION p2 VALUES [(\"-64\"), (\"0\")),\n" +
                "PARTITION p3 VALUES [(\"0\"), (\"64\")))\n" +
                "DISTRIBUTED BY HASH(`k1`) BUCKETS 5\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
                ");");



        String sql = "select k1 from test.baseall";
        //parser
        CaseInsensitiveStream stream = new CaseInsensitiveStream(CharStreams.fromString(sql));
        SqlBaseLexer lexer = new SqlBaseLexer(stream);
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        SqlBaseParser parser = new SqlBaseParser(tokens);
        AstBuilder visitor = new AstBuilder(new ParsingOptions());
        Node stmt = visitor.visit(parser.singleStatement());

        //Session
        Catalog catalog = Catalog.getInstance();
        ConnectContext context = new ConnectContext();
        context.setCatalog(catalog);
        context.setDatabase("test_db");
        Session session = new Session(catalog, context);

        //DorisMetadata
        Metadata metadata = new MetadataManager();

        //analyzer
        ArrayList<Expression> parameters = new ArrayList<>();
        Analysis analysis = new Analysis((Statement) stmt, parameters, true);
        StatementAnalyzer analyzer = new StatementAnalyzer(analysis, metadata, session);
        analyzer.analyze(stmt, Optional.empty());
        System.out.println("Hello world");

        //planner
        LogicalPlanner logicalPlanner = new LogicalPlanner(false, stateMachine.getSession(), planOptimizers, idAllocator, metadata, sqlParser, statsCalculator, costCalculator, stateMachine.getWarningCollector());
        Plan plan = logicalPlanner.plan(analysis);

        PhysicalPlanner physicalPlanner = new PhysicalPlanner();
        PlanNode root = physicalPlanner.createPhysicalPlan(plan);

        DistributedPlanner distributedPlanner = new DistributedPlanner(plannerContext);
        ArrayList<PlanFragment> fragments = Lists.newArrayList();
        fragments = distributedPlanner.createPlanFragments(root);
    }

    private static void createTable(String sql) throws Exception {
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, connectContext);
        Catalog.getCurrentCatalog().createTable(createTableStmt);
    }
}
