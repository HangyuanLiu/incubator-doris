package org.apache.doris.sql.planner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.doris.common.IdGenerator;
import org.apache.doris.planner.PlanNode;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.sql.planner.plan.Assignments;
import org.apache.doris.sql.planner.plan.LogicalPlanNode;
import org.apache.doris.sql.planner.plan.ProjectNode;
import org.apache.doris.sql.planner.plan.TableScanNode;
import org.apache.doris.sql.relation.VariableReferenceExpression;
import org.apache.doris.sql.analyzer.Analysis;
import org.apache.doris.sql.analyzer.Field;
import org.apache.doris.sql.analyzer.Scope;
import org.apache.doris.sql.metadata.ColumnHandle;
import org.apache.doris.sql.metadata.TableHandle;
import org.apache.doris.sql.tree.*;
import org.apache.doris.sql.type.Type;

import java.util.List;

import static org.apache.doris.sql.relational.OriginalExpressionUtils.asSymbolReference;
import static org.apache.doris.sql.relational.OriginalExpressionUtils.castToRowExpression;

class RelationPlanner
        extends DefaultTraversalVisitor<RelationPlan, Void> {
    private final Analysis analysis;
    private final VariableAllocator variableAllocator;
    private final IdGenerator<PlanNodeId> idAllocator;

    RelationPlanner(Analysis analysis, VariableAllocator variableAllocator, IdGenerator<PlanNodeId> idAllocator) {
        this.analysis = analysis;
        this.variableAllocator = variableAllocator;
        this.idAllocator = idAllocator;
    }

    @Override
    protected RelationPlan visitTable(Table node, Void context)
    {
        Scope scope = analysis.getScope(node);
        TableHandle handle = analysis.getTableHandle(node);

        ImmutableList.Builder<VariableReferenceExpression> outputVariablesBuilder = ImmutableList.builder();
        ImmutableMap.Builder<VariableReferenceExpression, ColumnHandle> columns = ImmutableMap.builder();
        for (Field field : scope.getRelationType().getAllFields()) {
            VariableReferenceExpression variable = variableAllocator.newVariable(field.getName().get(), field.getType());
            outputVariablesBuilder.add(variable);
            columns.put(variable, analysis.getColumn(field));
        }

        List<VariableReferenceExpression> outputVariables = outputVariablesBuilder.build();
        LogicalPlanNode root = new TableScanNode(idAllocator.getNextId(), handle, outputVariables, columns.build());
        return new RelationPlan(root, scope, outputVariables);
    }

    @Override
    protected RelationPlan visitAliasedRelation(AliasedRelation node, Void context)
    {
        RelationPlan subPlan = process(node.getRelation(), context);

        LogicalPlanNode root = subPlan.getRoot();
        List<VariableReferenceExpression> mappings = subPlan.getFieldMappings();

        if (node.getColumnNames() != null) {
            ImmutableList.Builder<VariableReferenceExpression> newMappings = ImmutableList.builder();
            Assignments.Builder assignments = Assignments.builder();

            // project only the visible columns from the underlying relation
            for (int i = 0; i < subPlan.getDescriptor().getAllFieldCount(); i++) {
                Field field = subPlan.getDescriptor().getFieldByIndex(i);
                if (!field.isHidden()) {
                    VariableReferenceExpression aliasedColumn = variableAllocator.newVariable(field);
                    assignments.put(aliasedColumn, castToRowExpression(asSymbolReference(subPlan.getFieldMappings().get(i))));
                    newMappings.add(aliasedColumn);
                }
            }

            root = new ProjectNode(idAllocator.getNextId(), subPlan.getRoot(), assignments.build());
            mappings = newMappings.build();
        }

        return new RelationPlan(root, analysis.getScope(node), mappings);
    }

    @Override
    protected RelationPlan visitQuery(Query node, Void context)
    {
        return new QueryPlanner(analysis, variableAllocator, idAllocator)
                .plan(node);
    }

    @Override
    protected RelationPlan visitQuerySpecification(QuerySpecification node, Void context)
    {
        return new QueryPlanner(analysis, variableAllocator, idAllocator)
                .plan(node);
    }
}
