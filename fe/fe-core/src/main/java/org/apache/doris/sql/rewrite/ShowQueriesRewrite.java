package org.apache.doris.sql.rewrite;

import org.apache.doris.sql.analyzer.QueryExplainer;
import org.apache.doris.sql.metadata.AccessControl;
import org.apache.doris.sql.metadata.Metadata;
import org.apache.doris.sql.metadata.Session;
import org.apache.doris.sql.metadata.WarningCollector;
import org.apache.doris.sql.parser.SqlParser;
import org.apache.doris.sql.tree.AstVisitor;
import org.apache.doris.sql.tree.Explain;
import org.apache.doris.sql.tree.Expression;
import org.apache.doris.sql.tree.Node;
import org.apache.doris.sql.tree.Statement;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

final class ShowQueriesRewrite
        implements StatementRewrite.Rewrite {
    @Override
    public Statement rewrite(
            Session session,
            Metadata metadata,
            SqlParser parser,
            Optional<QueryExplainer> queryExplainer,
            Statement node,
            List<Expression> parameters,
            AccessControl accessControl,
            WarningCollector warningCollector) {
        return (Statement) new Visitor(metadata, parser, session, parameters, accessControl, queryExplainer, warningCollector).process(node, null);
    }

    private static class Visitor
            extends AstVisitor<Node, Void> {
        private final Metadata metadata;
        private final Session session;
        private final SqlParser sqlParser;
        final List<Expression> parameters;
        private Optional<QueryExplainer> queryExplainer;

        public Visitor(Metadata metadata, SqlParser sqlParser, Session session, List<Expression> parameters, AccessControl accessControl, Optional<QueryExplainer> queryExplainer, WarningCollector warningCollector) {
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
            this.session = requireNonNull(session, "session is null");
            this.parameters = requireNonNull(parameters, "parameters is null");
            this.queryExplainer = requireNonNull(queryExplainer, "queryExplainer is null");
        }

        @Override
        protected Node visitExplain(Explain node, Void context) {
            Statement statement = (Statement) process(node.getStatement(), null);
            return new Explain(
                    node.getLocation().get(),
                    node.isAnalyze(),
                    node.isVerbose(),
                    statement,
                    node.getOptions());
        }

        @Override
        protected Node visitNode(Node node, Void context)
        {
            return node;
        }
    }
}