package org.apache.doris.sql.planner.optimizations;

import org.apache.doris.common.IdGenerator;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.sql.TypeProvider;
import org.apache.doris.sql.metadata.Session;
import org.apache.doris.sql.metadata.WarningCollector;
import org.apache.doris.sql.planner.SimplePlanRewriter;
import org.apache.doris.sql.planner.plan.FilterNode;
import org.apache.doris.sql.planner.plan.LogicalPlanNode;
import org.apache.doris.sql.tree.Expression;

import static java.util.Objects.requireNonNull;
import static org.apache.doris.sql.relational.OriginalExpressionUtils.castToExpression;
import static org.apache.doris.sql.tree.BooleanLiteral.TRUE_LITERAL;

public class PredicatePushDown
        implements PlanOptimizer
{
    @Override
    public LogicalPlanNode optimize(LogicalPlanNode plan, Session session, TypeProvider types, IdGenerator<PlanNodeId> idAllocator, WarningCollector warningCollector)
    {
        requireNonNull(plan, "plan is null");
        requireNonNull(session, "session is null");
        requireNonNull(types, "types is null");
        requireNonNull(idAllocator, "idAllocator is null");

        return SimplePlanRewriter.rewriteWith(
                new Rewriter(),
                plan,
                TRUE_LITERAL);
    }

    private static class Rewriter
            extends SimplePlanRewriter<Expression> {
        private Rewriter() {

        }

        @Override
        public LogicalPlanNode visitFilter(FilterNode node, RewriteContext<Expression> context) {
            LogicalPlanNode rewrittenPlan = context.rewrite(node.getSource(), castToExpression(node.getPredicate()));
            if (!(rewrittenPlan instanceof FilterNode)) {
                return rewrittenPlan;
            }
            return null;
        }
    }
}
