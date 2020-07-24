package org.apache.doris.sql.planner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.doris.common.IdGenerator;
import org.apache.doris.common.NotImplementedException;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.sql.TypeProvider;
import org.apache.doris.sql.metadata.Metadata;
import org.apache.doris.sql.metadata.Session;
import org.apache.doris.sql.planner.optimizations.PlanOptimizer;
import org.apache.doris.sql.planner.plan.ExchangeNode;
import org.apache.doris.sql.relation.VariableReferenceExpression;
import org.apache.doris.sql.analyzer.Analysis;
import org.apache.doris.sql.analyzer.Field;
import org.apache.doris.sql.analyzer.RelationType;
import org.apache.doris.sql.planner.plan.OutputNode;
import org.apache.doris.sql.planner.plan.LogicalPlanNode;
import org.apache.doris.sql.tree.Query;
import org.apache.doris.sql.tree.Statement;

import java.util.ArrayList;
import java.util.List;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class LogicalPlanner {
    public enum Stage {
        CREATED, OPTIMIZED, OPTIMIZED_AND_VALIDATED
    }

    private final Session session;
    IdGenerator<PlanNodeId> idAllocator;
    private final List<PlanOptimizer> planOptimizers;
    private final VariableAllocator variableAllocator = new VariableAllocator();
    private final Metadata metadata;

    public LogicalPlanner(
            Session session,
            List<PlanOptimizer> planOptimizers,
            IdGenerator<PlanNodeId> idAllocator,
            Metadata metadata) {
        this.session = session;
        this.planOptimizers = planOptimizers;
        this.idAllocator = idAllocator;
        this.metadata = metadata;
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
        return new Plan(root, types);
    }

    public LogicalPlanNode planStatement(Analysis analysis, Statement statement) throws Exception
    {
        return createOutputPlan(planStatementWithoutOutput(analysis, statement), analysis);
    }

    private RelationPlan planStatementWithoutOutput(Analysis analysis, Statement statement) throws Exception
    {
        if (statement instanceof Query) {
            return createRelationPlan(analysis, (Query) statement);
        }
        else {
            throw new NotImplementedException("Not implement");
        }
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