package org.apache.doris.sql.planner.optimizations;

import com.google.common.collect.ImmutableMap;
import org.apache.doris.common.IdGenerator;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.sql.TypeProvider;
import org.apache.doris.sql.analyzer.ExpressionAnalyzer;
import org.apache.doris.sql.metadata.Metadata;
import org.apache.doris.sql.metadata.Session;
import org.apache.doris.sql.metadata.WarningCollector;
import org.apache.doris.sql.parser.SqlParser;
import org.apache.doris.sql.planner.SimplePlanRewriter;
import org.apache.doris.sql.planner.plan.FilterNode;
import org.apache.doris.sql.planner.plan.LogicalPlanNode;
import org.apache.doris.sql.relation.RowExpression;
import org.apache.doris.sql.relational.SqlToRowExpressionTranslator;
import org.apache.doris.sql.tree.Expression;
import org.apache.doris.sql.tree.NodeRef;
import org.apache.doris.sql.type.Type;

import java.util.Map;

import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;
import static org.apache.doris.sql.relational.OriginalExpressionUtils.castToExpression;

public class TranslateExpressions implements PlanOptimizer {
    Metadata metadata;
    SqlParser sqlParser;
    public TranslateExpressions(Metadata metadata, SqlParser sqlParser)
    {
        this.metadata = metadata;
        this.sqlParser = sqlParser;
    }

    @Override
    public LogicalPlanNode optimize(LogicalPlanNode plan, Session session, TypeProvider types, IdGenerator<PlanNodeId> idAllocator, WarningCollector warningCollector) {
        return SimplePlanRewriter.rewriteWith(new Rewriter(metadata, sqlParser, types), plan, null);
    }

    private static class Rewriter
            extends SimplePlanRewriter<Void> {
        Metadata metadata;
        SqlParser sqlParser;
        TypeProvider typeProvider;

        private Rewriter(Metadata metadata, SqlParser sqlParser, TypeProvider typeProvider)
        {
            this.metadata = metadata;
            this.sqlParser = sqlParser;
            this.typeProvider = typeProvider;
        }

        @Override
        public LogicalPlanNode visitFilter(FilterNode node, RewriteContext<Void> context) {
            Map<NodeRef<Expression>, Type> types = ExpressionAnalyzer.getExpressionTypes(
                    null,
                    metadata,
                    sqlParser,
                    typeProvider,
                    castToExpression(node.getPredicate()),
                    emptyList(),
                    WarningCollector.NOOP);

            RowExpression rewritten = SqlToRowExpressionTranslator.translate(
                            castToExpression(node.getPredicate()),
                            types,
                            ImmutableMap.of(),
                            metadata.getFunctionManager(),
                            metadata.getTypeManager());
            return new FilterNode(node.getId(), node.getSource(), rewritten);
        }
    }
}
