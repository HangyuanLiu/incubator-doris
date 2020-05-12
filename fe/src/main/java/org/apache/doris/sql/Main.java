package org.apache.doris.sql;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.sql.analyzer.Analysis;
import org.apache.doris.sql.analyzer.StatementAnalyzer;
import org.apache.doris.sql.metadata.Session;
import org.apache.doris.sql.parser.AstBuilder;
import org.apache.doris.sql.parser.CaseInsensitiveStream;
import org.apache.doris.sql.parser.ParsingOptions;
import org.apache.doris.sql.parser.SqlBaseLexer;
import org.apache.doris.sql.parser.SqlBaseParser;
import org.apache.doris.sql.tree.Expression;
import org.apache.doris.sql.tree.Node;
import org.apache.doris.sql.tree.Statement;

import java.util.ArrayList;
import java.util.Optional;

public class Main {
    public static void main(String[] args) {
        String sql = "select col from table1";

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

        //analyzer
        ArrayList<Expression> parameters = new ArrayList<>();
        Analysis analysis = new Analysis((Statement) stmt, parameters, true);
        StatementAnalyzer analyzer = new StatementAnalyzer(analysis, session);
        analyzer.analyze(stmt, Optional.empty());
        System.out.println("Hello world");

        //planner
        //LogicalPlanner logicalPlanner = new LogicalPlanner(false, stateMachine.getSession(), planOptimizers, idAllocator, metadata, sqlParser, statsCalculator, costCalculator, stateMachine.getWarningCollector());
        //Plan plan = logicalPlanner.plan(analysis);
    }
}
