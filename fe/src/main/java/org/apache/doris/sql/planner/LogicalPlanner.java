package org.apache.doris.sql.planner;

import com.google.common.collect.ImmutableList;
import org.apache.doris.common.IdGenerator;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.sql.relation.VariableReferenceExpression;
import org.apache.doris.sql.analyzer.Analysis;
import org.apache.doris.sql.analyzer.Field;
import org.apache.doris.sql.analyzer.RelationType;
import org.apache.doris.sql.planner.plan.OutputNode;
import org.apache.doris.sql.planner.plan.LogicalPlanNode;
import org.apache.doris.sql.tree.Query;
import org.apache.doris.sql.tree.Statement;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class LogicalPlanner {
    public enum Stage {
        CREATED, OPTIMIZED, OPTIMIZED_AND_VALIDATED
    }

    IdGenerator<PlanNodeId> idAllocator;
    private final VariableAllocator variableAllocator = new VariableAllocator();

    public LogicalPlanner(IdGenerator<PlanNodeId> idAllocator) {
        this.idAllocator = idAllocator;
    }

    public Plan plan(Analysis analysis)
    {
        return plan(analysis, Stage.OPTIMIZED_AND_VALIDATED);
    }

    public Plan plan(Analysis analysis, Stage stage)
    {
        LogicalPlanNode root = planStatement(analysis, analysis.getStatement());
        /*
        planSanityChecker.validateIntermediatePlan(root, session, metadata, sqlParser, variableAllocator.getTypes(), warningCollector);

        if (stage.ordinal() >= Stage.OPTIMIZED.ordinal()) {
            for (PlanOptimizer optimizer : planOptimizers) {
                root = optimizer.optimize(root, session, variableAllocator.getTypes(), variableAllocator, idAllocator, warningCollector);
                requireNonNull(root, format("%s returned a null plan", optimizer.getClass().getName()));
            }
        }

        if (stage.ordinal() >= Stage.OPTIMIZED_AND_VALIDATED.ordinal()) {
            // make sure we produce a valid plan after optimizations run. This is mainly to catch programming errors
            planSanityChecker.validateFinalPlan(root, session, metadata, sqlParser, variableAllocator.getTypes(), warningCollector);
        }
        */
        //TypeProvider types = variableAllocator.getTypes();
        return new Plan(root, null);
    }

    public LogicalPlanNode planStatement(Analysis analysis, Statement statement)
    {
        return createOutputPlan(planStatementWithoutOutput(analysis, statement), analysis);
    }

    private RelationPlan planStatementWithoutOutput(Analysis analysis, Statement statement)
    {
        if (statement instanceof Query) {
            return createRelationPlan(analysis, (Query) statement);
        }
        else {
            //throw new Exception(NOT_SUPPORTED, "Unsupported statement type " + statement.getClass().getSimpleName());
        }
        return null;
    }

    private LogicalPlanNode createOutputPlan(RelationPlan plan, Analysis analysis)
    {
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
        return new OutputNode(idAllocator.getNextId(), plan.getRoot(), names.build(), outputs.build());
    }

    private RelationPlan createRelationPlan(Analysis analysis, Query query)
    {
        return new RelationPlanner(analysis, variableAllocator, idAllocator)
                .process(query, null);
    }
}