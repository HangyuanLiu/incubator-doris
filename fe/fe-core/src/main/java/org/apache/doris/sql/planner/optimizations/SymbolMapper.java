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
package org.apache.doris.sql.planner.optimizations;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.doris.common.IdGenerator;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.sql.TypeProvider;
import org.apache.doris.sql.expressions.RowExpressionRewriter;
import org.apache.doris.sql.expressions.RowExpressionTreeRewriter;
import org.apache.doris.sql.planner.Symbol;
import org.apache.doris.sql.planner.plan.AggregationNode;
import org.apache.doris.sql.planner.plan.AggregationNode.Aggregation;
import org.apache.doris.sql.planner.plan.LogicalPlanNode;
import org.apache.doris.sql.planner.plan.Ordering;
import org.apache.doris.sql.planner.plan.OrderingScheme;
import org.apache.doris.sql.planner.plan.SortOrder;
import org.apache.doris.sql.planner.plan.TopNNode;
import org.apache.doris.sql.relation.CallExpression;
import org.apache.doris.sql.relation.RowExpression;
import org.apache.doris.sql.relation.VariableReferenceExpression;
import org.apache.doris.sql.tree.Expression;
import org.apache.doris.sql.tree.ExpressionRewriter;
import org.apache.doris.sql.tree.ExpressionTreeRewriter;
import org.apache.doris.sql.tree.SymbolReference;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;
import static org.apache.doris.sql.planner.plan.AggregationNode.groupingSets;
import static org.apache.doris.sql.relational.OriginalExpressionUtils.castToExpression;
import static org.apache.doris.sql.relational.OriginalExpressionUtils.castToRowExpression;
import static org.apache.doris.sql.relational.OriginalExpressionUtils.isExpression;

public class SymbolMapper
{
    private final Map<String, String> mapping;
    private final TypeProvider types;

    public SymbolMapper(Map<VariableReferenceExpression, VariableReferenceExpression> mapping)
    {
        requireNonNull(mapping, "mapping is null");
        this.mapping = mapping.entrySet().stream().collect(toImmutableMap(entry -> entry.getKey().getName(), entry -> entry.getValue().getName()));
        ImmutableSet.Builder<VariableReferenceExpression> variables = ImmutableSet.builder();
        mapping.entrySet().forEach(entry -> {
            variables.add(entry.getKey());
            variables.add(entry.getValue());
        });
        this.types = TypeProvider.fromVariables(variables.build());
    }

    public SymbolMapper(Map<String, String> mapping, TypeProvider types)
    {
        requireNonNull(mapping, "mapping is null");
        this.mapping = ImmutableMap.copyOf(mapping);
        this.types = requireNonNull(types, "types is null");
    }

    public Symbol map(Symbol symbol)
    {
        String canonical = symbol.getName();
        while (mapping.containsKey(canonical) && !mapping.get(canonical).equals(canonical)) {
            canonical = mapping.get(canonical);
        }
        return new Symbol(canonical);
    }

    public VariableReferenceExpression map(VariableReferenceExpression variable)
    {
        String canonical = variable.getName();
        while (mapping.containsKey(canonical) && !mapping.get(canonical).equals(canonical)) {
            canonical = mapping.get(canonical);
        }
        if (canonical.equals(variable.getName())) {
            return variable;
        }
        return new VariableReferenceExpression(canonical, types.get(new SymbolReference(canonical)));
    }

    public Expression map(Expression value)
    {
        return ExpressionTreeRewriter.rewriteWith(new ExpressionRewriter<Void>()
        {
            @Override
            public Expression rewriteSymbolReference(SymbolReference node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                Symbol canonical = map(Symbol.from(node));
                return canonical.toSymbolReference();
            }
        }, value);
    }

    public RowExpression map(RowExpression value)
    {
        if (isExpression(value)) {
            return castToRowExpression(map(castToExpression(value)));
        }
        return RowExpressionTreeRewriter.rewriteWith(new RowExpressionRewriter<Void>()
        {
            @Override
            public RowExpression rewriteVariableReference(VariableReferenceExpression variable, Void context, RowExpressionTreeRewriter<Void> treeRewriter)
            {
                return map(variable);
            }
        }, value);
    }

    public OrderingScheme map(OrderingScheme orderingScheme)
    {
        // SymbolMapper inlines symbol with multiple level reference (SymbolInliner only inline single level).
        ImmutableList.Builder<VariableReferenceExpression> orderBy = ImmutableList.builder();
        ImmutableMap.Builder<VariableReferenceExpression, SortOrder> ordering = ImmutableMap.builder();
        for (VariableReferenceExpression variable : orderingScheme.getOrderByVariables()) {
            VariableReferenceExpression translated = map(variable);
            orderBy.add(translated);
            ordering.put(translated, orderingScheme.getOrdering(variable));
        }

        ImmutableMap<VariableReferenceExpression, SortOrder> orderingMap = ordering.build();
        return new OrderingScheme(orderBy.build().stream().map(variable -> new Ordering(variable, orderingMap.get(variable))).collect(Collectors.toList()));
    }

    public AggregationNode map(AggregationNode node, LogicalPlanNode source)
    {
        return map(node, source, node.getId());
    }

    public AggregationNode map(AggregationNode node, LogicalPlanNode source, IdGenerator<PlanNodeId> idAllocator)
    {
        return map(node, source, idAllocator.getNextId());
    }

    private AggregationNode map(AggregationNode node, LogicalPlanNode source, PlanNodeId newNodeId)
    {
        ImmutableMap.Builder<VariableReferenceExpression, Aggregation> aggregations = ImmutableMap.builder();
        for (Entry<VariableReferenceExpression, Aggregation> entry : node.getAggregations().entrySet()) {
            aggregations.put(map(entry.getKey()), map(entry.getValue()));
        }

        return new AggregationNode(
                newNodeId,
                source,
                aggregations.build(),
                groupingSets(
                        mapAndDistinctVariable(node.getGroupingKeys()),
                        node.getGroupingSetCount(),
                        node.getGlobalGroupingSets()),
                ImmutableList.of(),
                node.getStep(),
                node.getHashVariable().map(this::map),
                node.getGroupIdVariable().map(this::map));
    }

    private Aggregation map(Aggregation aggregation)
    {
        return new Aggregation(
                new CallExpression(
                        aggregation.getCall().getDisplayName(),
                        aggregation.getCall().getFunctionHandle(),
                        aggregation.getCall().getType(),
                        aggregation.getArguments().stream().map(this::map).collect(Collectors.toList())),
                aggregation.getFilter().map(this::map),
                aggregation.getOrderBy().map(this::map),
                aggregation.isDistinct(),
                aggregation.getMask().map(this::map));
    }

    public TopNNode map(TopNNode node, LogicalPlanNode source, PlanNodeId newNodeId)
    {
        ImmutableList.Builder<VariableReferenceExpression> variables = ImmutableList.builder();
        ImmutableMap.Builder<VariableReferenceExpression, SortOrder> orderings = ImmutableMap.builder();
        Set<VariableReferenceExpression> seenCanonicals = new HashSet<>(node.getOrderingScheme().getOrderByVariables().size());
        for (VariableReferenceExpression variable : node.getOrderingScheme().getOrderByVariables()) {
            VariableReferenceExpression canonical = map(variable);
            if (seenCanonicals.add(canonical)) {
                seenCanonicals.add(canonical);
                variables.add(canonical);
                orderings.put(canonical, node.getOrderingScheme().getOrdering(variable));
            }
        }

        ImmutableMap<VariableReferenceExpression, SortOrder> orderingMap = orderings.build();
        return new TopNNode(
                newNodeId,
                source,
                node.getCount(),
                new OrderingScheme(variables.build().stream().map(variable -> new Ordering(variable, orderingMap.get(variable))).collect(Collectors.toList())),
                node.getStep());
    }

    private List<Symbol> mapAndDistinctSymbol(List<Symbol> outputs)
    {
        Set<Symbol> added = new HashSet<>();
        ImmutableList.Builder<Symbol> builder = ImmutableList.builder();
        for (Symbol symbol : outputs) {
            Symbol canonical = map(symbol);
            if (added.add(canonical)) {
                builder.add(canonical);
            }
        }
        return builder.build();
    }

    private List<VariableReferenceExpression> mapAndDistinctVariable(List<VariableReferenceExpression> outputs)
    {
        Set<VariableReferenceExpression> added = new HashSet<>();
        ImmutableList.Builder<VariableReferenceExpression> builder = ImmutableList.builder();
        for (VariableReferenceExpression variable : outputs) {
            VariableReferenceExpression canonical = map(variable);
            if (added.add(canonical)) {
                builder.add(canonical);
            }
        }
        return builder.build();
    }

    public static SymbolMapper.Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private final ImmutableMap.Builder<VariableReferenceExpression, VariableReferenceExpression> mappingsBuilder = ImmutableMap.builder();

        public SymbolMapper build()
        {
            return new SymbolMapper(mappingsBuilder.build());
        }

        public void put(VariableReferenceExpression from, VariableReferenceExpression to)
        {
            mappingsBuilder.put(from, to);
        }
    }
}
