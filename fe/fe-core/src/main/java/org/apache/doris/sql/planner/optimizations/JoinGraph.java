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

import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.sql.planner.iterative.GroupReference;
import org.apache.doris.sql.planner.iterative.Lookup;
import  org.apache.doris.sql.planner.plan.FilterNode;
import org.apache.doris.sql.planner.plan.JoinNode;
import org.apache.doris.sql.planner.plan.LogicalPlanNode;
import org.apache.doris.sql.planner.plan.PlanVisitor;
import  org.apache.doris.sql.planner.plan.ProjectNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import org.apache.doris.sql.relation.VariableReferenceExpression;
import org.apache.doris.sql.relational.OriginalExpressionUtils;
import org.apache.doris.sql.tree.Expression;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Maps.transformValues;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.apache.doris.sql.planner.plan.JoinNode.Type.INNER;
import static org.apache.doris.sql.relational.OriginalExpressionUtils.castToExpression;
import static org.apache.doris.sql.relational.ProjectNodeUtils.isIdentity;

/**
 * JoinGraph represents sequence of Joins, where nodes in the graph
 * are PlanNodes that are being joined and edges are all equality join
 * conditions between pair of nodes.
 */
public class JoinGraph
{
    private final Optional<Map<VariableReferenceExpression, Expression>> assignments;
    private final List<Expression> filters;
    private final List<LogicalPlanNode> nodes; // nodes in order of their appearance in tree plan (left, right, parent)
    private final Multimap<PlanNodeId, Edge> edges;
    private final PlanNodeId rootId;

    /**
     * Builds all (distinct) {@link JoinGraph}-es whole plan tree.
     */
    public static List<JoinGraph> buildFrom(LogicalPlanNode plan)
    {
        return buildFrom(plan, Lookup.noLookup());
    }

    /**
     * Builds {@link JoinGraph} containing {@code plan} node.
     */
    public static JoinGraph buildShallowFrom(LogicalPlanNode plan, Lookup lookup)
    {
        JoinGraph graph = plan.accept(new Builder(true, lookup), new Context());
        return graph;
    }

    private static List<JoinGraph> buildFrom(LogicalPlanNode plan, Lookup lookup)
    {
        Context context = new Context();
        JoinGraph graph = plan.accept(new Builder(false, lookup), context);
        if (graph.size() > 1) {
            context.addSubGraph(graph);
        }
        return context.getGraphs();
    }

    public JoinGraph(LogicalPlanNode node)
    {
        this(ImmutableList.of(node), ImmutableMultimap.of(), node.getId(), ImmutableList.of(), Optional.empty());
    }

    public JoinGraph(
            List<LogicalPlanNode> nodes,
            Multimap<PlanNodeId, Edge> edges,
            PlanNodeId rootId,
            List<Expression> filters,
            Optional<Map<VariableReferenceExpression, Expression>> assignments)
    {
        this.nodes = nodes;
        this.edges = edges;
        this.rootId = rootId;
        this.filters = filters;
        this.assignments = assignments;
    }

    public JoinGraph withAssignments(Map<VariableReferenceExpression, Expression> assignments)
    {
        return new JoinGraph(nodes, edges, rootId, filters, Optional.of(assignments));
    }

    public Optional<Map<VariableReferenceExpression, Expression>> getAssignments()
    {
        return assignments;
    }

    public JoinGraph withFilter(Expression expression)
    {
        ImmutableList.Builder<Expression> filters = ImmutableList.builder();
        filters.addAll(this.filters);
        filters.add(expression);

        return new JoinGraph(nodes, edges, rootId, filters.build(), assignments);
    }

    public List<Expression> getFilters()
    {
        return filters;
    }

    public PlanNodeId getRootId()
    {
        return rootId;
    }

    public JoinGraph withRootId(PlanNodeId rootId)
    {
        return new JoinGraph(nodes, edges, rootId, filters, assignments);
    }

    public boolean isEmpty()
    {
        return nodes.isEmpty();
    }

    public int size()
    {
        return nodes.size();
    }

    public LogicalPlanNode getNode(int index)
    {
        return nodes.get(index);
    }

    public List<LogicalPlanNode> getNodes()
    {
        return nodes;
    }

    public Collection<Edge> getEdges(LogicalPlanNode node)
    {
        return ImmutableList.copyOf(edges.get(node.getId()));
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();

        for (LogicalPlanNode nodeFrom : nodes) {
            builder.append(nodeFrom.getId())
                    .append(" = ")
                    .append(nodeFrom.toString())
                    .append("\n");
        }
        for (LogicalPlanNode nodeFrom : nodes) {
            builder.append(nodeFrom.getId())
                    .append(":");
            for (Edge nodeTo : edges.get(nodeFrom.getId())) {
                builder.append(" ").append(nodeTo.getTargetNode().getId());
            }
            builder.append("\n");
        }

        return builder.toString();
    }

    private JoinGraph joinWith(JoinGraph other, List<JoinNode.EquiJoinClause> joinClauses, Context context, PlanNodeId newRoot)
    {
        for (LogicalPlanNode node : other.nodes) {
            checkState(!edges.containsKey(node.getId()), format("Node [%s] appeared in two JoinGraphs", node));
        }

        List<LogicalPlanNode> nodes = ImmutableList.<LogicalPlanNode>builder()
                .addAll(this.nodes)
                .addAll(other.nodes)
                .build();

        ImmutableMultimap.Builder<PlanNodeId, Edge> edges = ImmutableMultimap.<PlanNodeId, Edge>builder()
                .putAll(this.edges)
                .putAll(other.edges);

        List<Expression> joinedFilters = ImmutableList.<Expression>builder()
                .addAll(this.filters)
                .addAll(other.filters)
                .build();

        for (JoinNode.EquiJoinClause edge : joinClauses) {
            VariableReferenceExpression leftVariable = edge.getLeft();
            VariableReferenceExpression rightVariable = edge.getRight();
            checkState(context.containsVariable(leftVariable));
            checkState(context.containsVariable(rightVariable));

            LogicalPlanNode left = context.getVariableSource(leftVariable);
            LogicalPlanNode right = context.getVariableSource(rightVariable);
            edges.put(left.getId(), new Edge(right, leftVariable, rightVariable));
            edges.put(right.getId(), new Edge(left, rightVariable, leftVariable));
        }

        return new JoinGraph(nodes, edges.build(), newRoot, joinedFilters, Optional.empty());
    }

    private static class Builder
            extends PlanVisitor<JoinGraph, Context>
    {
        // TODO When com.facebook.presto.sql.planner.optimizations.EliminateCrossJoins is removed, remove 'shallow' flag
        private final boolean shallow;
        private final Lookup lookup;

        private Builder(boolean shallow, Lookup lookup)
        {
            this.shallow = shallow;
            this.lookup = requireNonNull(lookup, "lookup cannot be null");
        }

        @Override
        public JoinGraph visitPlan(LogicalPlanNode node, Context context)
        {
            if (!shallow) {
                for (LogicalPlanNode child : node.getSources()) {
                    JoinGraph graph = child.accept(this, context);
                    if (graph.size() < 2) {
                        continue;
                    }
                    context.addSubGraph(graph.withRootId(child.getId()));
                }
            }

            for (VariableReferenceExpression variable : node.getOutputVariables()) {
                context.setVariableSource(variable, node);
            }
            return new JoinGraph(node);
        }

        @Override
        public JoinGraph visitFilter(FilterNode node, Context context)
        {
            JoinGraph graph = node.getSource().accept(this, context);
            return graph.withFilter(castToExpression(node.getPredicate()));
        }

        @Override
        public JoinGraph visitJoin(JoinNode node, Context context)
        {
            //TODO: add support for non inner joins
            if (node.getType() != INNER) {
                return visitPlan(node, context);
            }

            JoinGraph left = node.getLeft().accept(this, context);
            JoinGraph right = node.getRight().accept(this, context);

            JoinGraph graph = left.joinWith(right, node.getCriteria(), context, node.getId());

            if (node.getFilter().isPresent()) {
                return graph.withFilter(castToExpression(node.getFilter().get()));
            }
            return graph;
        }

        @Override
        public JoinGraph visitProject(ProjectNode node, Context context)
        {
            if (isIdentity(node)) {
                JoinGraph graph = node.getSource().accept(this, context);
                return graph.withAssignments(transformValues(node.getAssignments().getMap(), OriginalExpressionUtils::castToExpression));
            }
            return visitPlan(node, context);
        }

        @Override
        public JoinGraph visitGroupReference(GroupReference node, Context context)
        {
            LogicalPlanNode dereferenced = lookup.resolve(node);
            JoinGraph graph = dereferenced.accept(this, context);
            if (isTrivialGraph(graph)) {
                return replacementGraph(dereferenced, node, context);
            }
            return graph;
        }

        private boolean isTrivialGraph(JoinGraph graph)
        {
            return graph.nodes.size() < 2 && graph.edges.isEmpty() && graph.filters.isEmpty() && !graph.assignments.isPresent();
        }

        private JoinGraph replacementGraph(LogicalPlanNode oldNode, LogicalPlanNode newNode, Context context)
        {
            // TODO optimize when idea is generally approved
            List<VariableReferenceExpression> variables = context.variableSources.entrySet().stream()
                    .filter(entry -> entry.getValue() == oldNode)
                    .map(Map.Entry::getKey)
                    .collect(toImmutableList());
            variables.forEach(variable -> context.variableSources.put(variable, newNode));

            return new JoinGraph(newNode);
        }
    }

    public static class Edge
    {
        private final LogicalPlanNode targetNode;
        private final VariableReferenceExpression sourceVariable;
        private final VariableReferenceExpression targetVariable;

        public Edge(LogicalPlanNode targetNode, VariableReferenceExpression sourceVariable, VariableReferenceExpression targetVariable)
        {
            this.targetNode = requireNonNull(targetNode, "targetNode is null");
            this.sourceVariable = requireNonNull(sourceVariable, "sourceVariable is null");
            this.targetVariable = requireNonNull(targetVariable, "targetVariable is null");
        }

        public LogicalPlanNode getTargetNode()
        {
            return targetNode;
        }

        public VariableReferenceExpression getSourceVariable()
        {
            return sourceVariable;
        }

        public VariableReferenceExpression getTargetVariable()
        {
            return targetVariable;
        }
    }

    private static class Context
    {
        private final Map<VariableReferenceExpression, LogicalPlanNode> variableSources = new HashMap<>();

        // TODO When com.facebook.presto.sql.planner.optimizations.EliminateCrossJoins is removed, remove 'joinGraphs'
        private final List<JoinGraph> joinGraphs = new ArrayList<>();

        public void setVariableSource(VariableReferenceExpression variable, LogicalPlanNode node)
        {
            variableSources.put(variable, node);
        }

        public void addSubGraph(JoinGraph graph)
        {
            joinGraphs.add(graph);
        }

        public boolean containsVariable(VariableReferenceExpression variable)
        {
            return variableSources.containsKey(variable);
        }

        public LogicalPlanNode getVariableSource(VariableReferenceExpression variable)
        {
            checkState(containsVariable(variable));
            return variableSources.get(variable);
        }

        public List<JoinGraph> getGraphs()
        {
            return joinGraphs;
        }
    }
}
