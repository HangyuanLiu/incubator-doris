package org.apache.doris.sql.planner;

import com.google.common.collect.Sets;
import org.apache.doris.common.IdGenerator;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.sql.analyzer.Analysis;
import org.apache.doris.sql.metadata.Metadata;
import org.apache.doris.sql.metadata.Session;
import org.apache.doris.sql.relation.VariableReferenceExpression;
import org.apache.doris.sql.tree.Expression;
import org.apache.doris.sql.tree.InPredicate;
import org.apache.doris.sql.tree.Node;
import org.apache.spark.sql.catalyst.expressions.SubqueryExpression;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;

class SubqueryPlanner {
    private final Analysis analysis;
    private final VariableAllocator variableAllocator;
    private final IdGenerator<PlanNodeId> idAllocator;
    private final Metadata metadata;
    private final Session session;

    SubqueryPlanner(
            Analysis analysis,
            VariableAllocator variableAllocator,
            IdGenerator<PlanNodeId> idAllocator,
            Metadata metadata,
            Session session) {
        this.analysis = analysis;
        this.variableAllocator = variableAllocator;
        this.idAllocator = idAllocator;
        this.metadata = metadata;
        this.session = session;
    }

    public PlanBuilder handleSubqueries(PlanBuilder builder, Collection<Expression> expressions, Node node)
    {
        for (Expression expression : expressions) {
            builder = handleSubqueries(builder, expression, node, true);
        }
        return builder;
    }

    public PlanBuilder handleUncorrelatedSubqueries(PlanBuilder builder, Collection<Expression> expressions, Node node)
    {
        for (Expression expression : expressions) {
            builder = handleSubqueries(builder, expression, node, false);
        }
        return builder;
    }

    public PlanBuilder handleSubqueries(PlanBuilder builder, Expression expression, Node node)
    {
        return handleSubqueries(builder, expression, node, true);
    }

    private PlanBuilder handleSubqueries(PlanBuilder builder, Expression expression, Node node, boolean correlationAllowed)
    {
        //builder = appendScalarSubqueryApplyNodes(builder, collectScalarSubqueries(expression, node), correlationAllowed);
        return builder;
    }

    public Set<InPredicate> collectInPredicateSubqueries(Expression expression, Node node)
    {
        /*
        return analysis.getInPredicateSubqueries(node)
                .stream()
                .filter(inPredicate -> nodeContains(expression, inPredicate.getValueList()))
                .collect(toImmutableSet());

         */
        return null;
    }
    /*
    public Set<SubqueryExpression> collectScalarSubqueries(Expression expression, Node node)
    {
        return analysis.getScalarSubqueries(node)
                .stream()
                .filter(subquery -> nodeContains(expression, subquery))
                .collect(toImmutableSet());
    }

    private PlanBuilder appendScalarSubqueryApplyNodes(PlanBuilder builder, Set<SubqueryExpression> scalarSubqueries, boolean correlationAllowed)
    {
        for (SubqueryExpression scalarSubquery : scalarSubqueries) {
            builder = appendScalarSubqueryApplyNode(builder, scalarSubquery, correlationAllowed);
        }
        return builder;
    }

    private PlanBuilder appendScalarSubqueryApplyNode(PlanBuilder subPlan, SubqueryExpression scalarSubquery, boolean correlationAllowed)
    {
        if (subPlan.canTranslate(scalarSubquery)) {
            // given subquery is already appended
            return subPlan;
        }

        List<Expression> coercions = coercionsFor(scalarSubquery);

        SubqueryExpression uncoercedScalarSubquery = uncoercedSubquery(scalarSubquery);
        PlanBuilder subqueryPlan = createPlanBuilder(uncoercedScalarSubquery);
        subqueryPlan = subqueryPlan.withNewRoot(new EnforceSingleRowNode(idAllocator.getNextId(), subqueryPlan.getRoot()));
        subqueryPlan = subqueryPlan.appendProjections(coercions, variableAllocator, idAllocator);

        VariableReferenceExpression uncoercedScalarSubqueryVariable = subqueryPlan.translate(uncoercedScalarSubquery);
        subPlan.getTranslations().put(uncoercedScalarSubquery, uncoercedScalarSubqueryVariable);

        for (Expression coercion : coercions) {
            VariableReferenceExpression coercionVariable = subqueryPlan.translate(coercion);
            subPlan.getTranslations().put(coercion, coercionVariable);
        }

        return appendLateralJoin(subPlan, subqueryPlan, scalarSubquery.getQuery(), correlationAllowed, LateralJoinNode.Type.LEFT);
    }
     */
}