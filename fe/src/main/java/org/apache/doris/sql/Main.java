package org.apache.doris.sql;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.apache.doris.sql.analyzer.Analysis;
import org.apache.doris.sql.analyzer.StatementAnalyzer;
import org.apache.doris.sql.parser.AstBuilder;
import org.apache.doris.sql.parser.CaseInsensitiveStream;
import org.apache.doris.sql.parser.ParsingOptions;
import org.apache.doris.sql.parser.SqlBaseLexer;
import org.apache.doris.sql.parser.SqlBaseParser;
import org.apache.doris.sql.planner.LogicalPlanner;
import org.apache.doris.sql.tree.Node;

import java.util.Optional;

public class Main {
    public static void main(String[] args) {
        String sql = "select col from table1 where a = 1";

        //parser
        CaseInsensitiveStream stream = new CaseInsensitiveStream(CharStreams.fromString(sql));
        SqlBaseLexer lexer = new SqlBaseLexer(stream);
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        SqlBaseParser parser = new SqlBaseParser(tokens);
        AstBuilder visitor = new AstBuilder(new ParsingOptions());
        Node stmt = visitor.visit(parser.singleStatement());

        //analyzer
        Analysis analysis = new Analysis(stmt, parameters, isDescribe);
        StatementAnalyzer analyzer = new StatementAnalyzer(analysis, metadata, sqlParser, accessControl, session, warningCollector);
        analyzer.analyze(stmt, Optional.empty());

        //planner
        LogicalPlanner logicalPlanner = new LogicalPlanner(false, stateMachine.getSession(), planOptimizers, idAllocator, metadata, sqlParser, statsCalculator, costCalculator, stateMachine.getWarningCollector());
        Plan plan = logicalPlanner.plan(analysis);
    }
}
