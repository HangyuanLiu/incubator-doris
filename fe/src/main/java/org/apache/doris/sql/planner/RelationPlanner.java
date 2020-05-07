/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.doris.sql.planner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import org.apache.doris.sql.analyzer.*;
import org.apache.doris.sql.metadata.Metadata;
import org.apache.doris.sql.metadata.Session;
import org.apache.doris.sql.metadata.Type;
import org.apache.doris.sql.planner.plan.Assignments;
import org.apache.doris.sql.planner.plan.PlanNode;
import org.apache.doris.sql.planner.plan.PlanNodeIdAllocator;
import org.apache.doris.sql.planner.plan.ProjectNode;
import org.apache.doris.sql.relation.VariableReferenceExpression;
import org.apache.doris.sql.tree.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;
import static org.apache.doris.sql.relational.OriginalExpressionUtils.asSymbolReference;
import static org.apache.doris.sql.relational.OriginalExpressionUtils.castToRowExpression;

class RelationPlanner
        extends DefaultTraversalVisitor<RelationPlan, Void>
{
    private final Analysis analysis;
    private final PlanVariableAllocator variableAllocator;
    private final PlanNodeIdAllocator idAllocator;
    private final Metadata metadata;
    private final Session session;
    //private final SubqueryPlanner subqueryPlanner;

    RelationPlanner(
            Analysis analysis,
            PlanVariableAllocator variableAllocator,
            PlanNodeIdAllocator idAllocator,
            Metadata metadata,
            Session session)
    {
        requireNonNull(analysis, "analysis is null");
        requireNonNull(variableAllocator, "variableAllocator is null");
        requireNonNull(idAllocator, "idAllocator is null");
        requireNonNull(metadata, "metadata is null");
        requireNonNull(session, "session is null");

        this.analysis = analysis;
        this.variableAllocator = variableAllocator;
        this.idAllocator = idAllocator;
        this.metadata = metadata;
        this.session = session;
        //this.subqueryPlanner = new SubqueryPlanner(analysis, variableAllocator, idAllocator, lambdaDeclarationToVariableMap, metadata, session);
    }

    @Override
    protected RelationPlan visitTable(Table node, Void context)
    {
        Query namedQuery = analysis.getNamedQuery(node);
        Scope scope = analysis.getScope(node);
        /*
        if (namedQuery != null) {
            RelationPlan subPlan = process(namedQuery, null);

            // Add implicit coercions if view query produces types that don't match the declared output types
            // of the view (e.g., if the underlying tables referenced by the view changed)
            Type[] types = scope.getRelationType().getAllFields().stream().map(Field::getType).toArray(Type[]::new);
            RelationPlan withCoercions = addCoercions(subPlan, types);
            return new RelationPlan(withCoercions.getRoot(), scope, withCoercions.getFieldMappings());
        }
        */
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
