package org.apache.doris.sql.planner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.doris.planner.PlanNode;
import org.apache.doris.sql.planner.plan.Assignments;
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

    RelationPlanner(Analysis analysis) {
        this.analysis = analysis;
    }

    @Override
    protected RelationPlan visitTable(Table node, Void context)
    {
        Query namedQuery = analysis.getNamedQuery(node);
        Scope scope = analysis.getScope(node);

        if (namedQuery != null) {
            RelationPlan subPlan = process(namedQuery, null);

            // Add implicit coercions if view query produces types that don't match the declared output types
            // of the view (e.g., if the underlying tables referenced by the view changed)
            Type[] types = scope.getRelationType().getAllFields().stream().map(Field::getType).toArray(Type[]::new);
            RelationPlan withCoercions = addCoercions(subPlan, types);
            return new RelationPlan(withCoercions.getRoot(), scope, withCoercions.getFieldMappings());
        }

        TableHandle handle = analysis.getTableHandle(node);

        ImmutableList.Builder<VariableReferenceExpression> outputVariablesBuilder = ImmutableList.builder();
        ImmutableMap.Builder<VariableReferenceExpression, ColumnHandle> columns = ImmutableMap.builder();
        for (Field field : scope.getRelationType().getAllFields()) {
            VariableReferenceExpression variable = variableAllocator.newVariable(field.getName().get(), field.getType());
            outputVariablesBuilder.add(variable);
            columns.put(variable, analysis.getColumn(field));
        }

        List<VariableReferenceExpression> outputVariables = outputVariablesBuilder.build();
        PlanNode root = new TableScanNode(idAllocator.getNextId(), handle, outputVariables, columns.build(), TupleDomain.all(), TupleDomain.all());
        return new RelationPlan(root, scope, outputVariables);
    }

    @Override
    protected RelationPlan visitAliasedRelation(AliasedRelation node, Void context)
    {
        RelationPlan subPlan = process(node.getRelation(), context);

        PlanNode root = subPlan.getRoot();
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
    protected RelationPlan visitSampledRelation(SampledRelation node, Void context)
    {
        RelationPlan subPlan = process(node.getRelation(), context);

        double ratio = analysis.getSampleRatio(node);
        PlanNode planNode = new SampleNode(idAllocator.getNextId(),
                subPlan.getRoot(),
                ratio,
                SampleNodeUtil.fromType(node.getType()));
        return new RelationPlan(planNode, analysis.getScope(node), subPlan.getFieldMappings());
    }

    @Override
    protected RelationPlan visitQuery(Query node, Void context)
    {
        return new QueryPlanner(analysis, variableAllocator, idAllocator, lambdaDeclarationToVariableMap, metadata, session)
                .plan(node);
    }

    @Override
    protected RelationPlan visitQuerySpecification(QuerySpecification node, Void context)
    {
        return new QueryPlanner(analysis, variableAllocator, idAllocator, lambdaDeclarationToVariableMap, metadata, session)
                .plan(node);
    }
}
