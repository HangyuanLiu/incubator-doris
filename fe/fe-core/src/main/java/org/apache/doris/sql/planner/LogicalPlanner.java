package org.apache.doris.sql.planner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.doris.common.IdGenerator;
import org.apache.doris.common.NotImplementedException;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.sql.TypeProvider;
import org.apache.doris.sql.analyzer.Scope;
import org.apache.doris.sql.metadata.Metadata;
import org.apache.doris.sql.metadata.Session;
import org.apache.doris.sql.metadata.WarningCollector;
import org.apache.doris.sql.planner.cost.CachingCostProvider;
import org.apache.doris.sql.planner.cost.CachingStatsProvider;
import org.apache.doris.sql.planner.cost.CostCalculator;
import org.apache.doris.sql.planner.cost.CostProvider;
import org.apache.doris.sql.planner.cost.StatsAndCosts;
import org.apache.doris.sql.planner.cost.StatsCalculator;
import org.apache.doris.sql.planner.cost.StatsProvider;
import org.apache.doris.sql.planner.optimizations.PlanNodeSearcher;
import org.apache.doris.sql.planner.optimizations.PlanOptimizer;
import org.apache.doris.sql.planner.plan.ExchangeNode;
import org.apache.doris.sql.planner.plan.ExplainAnalyzeNode;
import org.apache.doris.sql.planner.plan.JoinNode;
import org.apache.doris.sql.planner.plan.PlanNodeIdAllocator;
import org.apache.doris.sql.planner.plan.SemiJoinNode;
import org.apache.doris.sql.relation.VariableReferenceExpression;
import org.apache.doris.sql.analyzer.Analysis;
import org.apache.doris.sql.analyzer.Field;
import org.apache.doris.sql.analyzer.RelationType;
import org.apache.doris.sql.planner.plan.OutputNode;
import org.apache.doris.sql.planner.plan.LogicalPlanNode;
import org.apache.doris.sql.tree.Explain;
import org.apache.doris.sql.tree.Query;
import org.apache.doris.sql.tree.Statement;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class LogicalPlanner {
    public enum Stage {
        CREATED, OPTIMIZED, OPTIMIZED_AND_VALIDATED
    }

    private final boolean explain;
    private final Session session;
    PlanNodeIdAllocator idAllocator;
    private final List<PlanOptimizer> planOptimizers;
    private final VariableAllocator variableAllocator = new VariableAllocator();
    private final Metadata metadata;

    private final StatsCalculator statsCalculator;
    private final CostCalculator costCalculator;

    public LogicalPlanner(
            boolean explain,
            Session session,
            List<PlanOptimizer> planOptimizers,
            PlanNodeIdAllocator idAllocator,
            Metadata metadata,
            StatsCalculator statsCalculator,
            CostCalculator costCalculator,
            WarningCollector warningCollector) {
        this.explain = explain;
        this.session = session;
        this.planOptimizers = planOptimizers;
        this.idAllocator = idAllocator;
        this.metadata = metadata;

        this.statsCalculator = requireNonNull(statsCalculator, "statsCalculator is null");
        this.costCalculator = requireNonNull(costCalculator, "costCalculator is null");
    }

    public Plan plan(Analysis analysis) throws Exception
    {
        return plan(analysis, Stage.OPTIMIZED_AND_VALIDATED);
    }

    public Plan plan(Analysis analysis, Stage stage) throws Exception
    {
        LogicalPlanNode root = planStatement(analysis, analysis.getStatement());

        //planSanityChecker.validateIntermediatePlan(root, session, metadata, sqlParser, variableAllocator.getTypes(), warningCollector);

        if (stage.ordinal() >= Stage.OPTIMIZED.ordinal()) {
            for (PlanOptimizer optimizer : planOptimizers) {
                root = optimizer.optimize(root, session, variableAllocator.getTypes(), variableAllocator, idAllocator, null);
                requireNonNull(root, format("%s returned a null plan", optimizer.getClass().getName()));
            }
        }
        /*
        if (stage.ordinal() >= Stage.OPTIMIZED_AND_VALIDATED.ordinal()) {
            // make sure we produce a valid plan after optimizations run. This is mainly to catch programming errors
            planSanityChecker.validateFinalPlan(root, session, metadata, sqlParser, variableAllocator.getTypes(), warningCollector);
        }
        */
        TypeProvider types = variableAllocator.getTypes();
        return new Plan(root, types, computeStats(root, types));
    }

    private StatsAndCosts computeStats(LogicalPlanNode root, TypeProvider types)
    {
        /*
        if (explain || isPrintStatsForNonJoinQuery(session) ||
                PlanNodeSearcher.searchFrom(root).where(node ->
                        (node instanceof JoinNode) || (node instanceof SemiJoinNode)).matches()) {

         */
        if (true) {
            StatsProvider statsProvider = new CachingStatsProvider(statsCalculator, session, types);
            CostProvider costProvider = new CachingCostProvider(costCalculator, statsProvider, Optional.empty(), session);
            return StatsAndCosts.create(root, statsProvider, costProvider);
        }
        return StatsAndCosts.empty();
    }

    public LogicalPlanNode planStatement(Analysis analysis, Statement statement) throws Exception
    {
        return createOutputPlan(planStatementWithoutOutput(analysis, statement), analysis);
    }

    private RelationPlan planStatementWithoutOutput(Analysis analysis, Statement statement) throws Exception
    {
        if (statement instanceof Query) {
            return createRelationPlan(analysis, (Query) statement);
        } else if (statement instanceof Explain && ((Explain) statement).isAnalyze()) {
            return createExplainAnalyzePlan(analysis, (Explain) statement);
        }
        else {
            throw new NotImplementedException("Not implement");
        }
    }

    private RelationPlan createExplainAnalyzePlan(Analysis analysis, Explain statement) throws Exception
    {
        RelationPlan underlyingPlan = planStatementWithoutOutput(analysis, statement.getStatement());
        LogicalPlanNode root = underlyingPlan.getRoot();
        Scope scope = analysis.getScope(statement);
        VariableReferenceExpression outputVariable = variableAllocator.newVariable(scope.getRelationType().getFieldByIndex(0));
        root = new ExplainAnalyzeNode(idAllocator.getNextId(), root, outputVariable, statement.isVerbose());
        return new RelationPlan(root, scope, ImmutableList.of(outputVariable));
    }

    private LogicalPlanNode createOutputPlan(RelationPlan plan, Analysis analysis)
    {
        /*
        ImmutableList.Builder<VariableReferenceExpression> outputs = ImmutableList.builder();
        ImmutableList.Builder<String> names = ImmutableList.builder();

        int columnNumber = 0;
        RelationType outputDescriptor = analysis.getOutputDescriptor();
        for (Field field : outputDescriptor.getVisibleFields()) {
            String name = field.getName().orElse("_col" + columnNumber);
            names.add(name);

            int fieldIndex = outputDescriptor.indexOf(field);
            VariableReferenceExpression variable = plan.getVariable(fieldIndex);
            outputs.add(variable);

            columnNumber++;
        }

         */

        return new OutputNode(idAllocator.getNextId(), plan.getRoot(), new ArrayList<>(), plan.getRoot().getOutputVariables());
    }

    private RelationPlan createRelationPlan(Analysis analysis, Query query)
    {
        return new RelationPlanner(analysis, variableAllocator, idAllocator, metadata, session)
                .process(query, null);
    }
}