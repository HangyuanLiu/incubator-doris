package org.apache.doris.sql.planner.planPrinter;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.sql.TypeProvider;
import org.apache.doris.sql.metadata.ConnectorSession;
import org.apache.doris.sql.metadata.FunctionManager;
import org.apache.doris.sql.metadata.Session;
import org.apache.doris.sql.metadata.TableHandle;
import org.apache.doris.sql.planner.cost.PlanCostEstimate;
import org.apache.doris.sql.planner.cost.PlanNodeStatsEstimate;
import org.apache.doris.sql.planner.cost.StatsAndCosts;
import org.apache.doris.sql.planner.optimizations.JoinNodeUtils;
import org.apache.doris.sql.planner.plan.AggregationNode;
import org.apache.doris.sql.planner.plan.Assignments;
import org.apache.doris.sql.planner.plan.FilterNode;
import org.apache.doris.sql.planner.plan.JoinNode;
import org.apache.doris.sql.planner.plan.LimitNode;
import org.apache.doris.sql.planner.plan.LogicalPlanNode;
import org.apache.doris.sql.planner.plan.OutputNode;
import org.apache.doris.sql.planner.plan.PlanVisitor;
import org.apache.doris.sql.planner.plan.ProjectNode;
import org.apache.doris.sql.planner.plan.SemiJoinNode;
import org.apache.doris.sql.planner.plan.SortNode;
import org.apache.doris.sql.planner.plan.TableScanNode;
import org.apache.doris.sql.planner.plan.TopNNode;
import org.apache.doris.sql.planner.plan.ValuesNode;
import org.apache.doris.sql.relation.RowExpression;
import org.apache.doris.sql.relation.VariableReferenceExpression;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.util.Arrays.stream;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class PlanPrinter {
    private final PlanRepresentation representation;
    private final FunctionManager functionManager;
    private final Function<RowExpression, String> formatter;

    private PlanPrinter(
            LogicalPlanNode planRoot,
            TypeProvider types,
            FunctionManager functionManager,
            StatsAndCosts estimatedStatsAndCosts,
            Session session,
            Optional<Map<PlanNodeId, PlanNodeStats>> stats)
    {
        this.functionManager = functionManager;
        this.representation = new PlanRepresentation(planRoot);

        RowExpressionFormatter rowExpressionFormatter = new RowExpressionFormatter(functionManager);
        ConnectorSession connectorSession = requireNonNull(session, "session is null").toConnectorSession();
        this.formatter = rowExpression -> rowExpressionFormatter.formatRowExpression(connectorSession, rowExpression);

        Visitor visitor = new Visitor(estimatedStatsAndCosts, stats);
        planRoot.accept(visitor, null);
    }

    public String toText(boolean verbose, int level)
    {
        return new TextRenderer(verbose, level).render(representation);
    }

    public static String textLogicalPlan(LogicalPlanNode plan, TypeProvider types, FunctionManager functionManager, StatsAndCosts estimatedStatsAndCosts, Session session, int level) {
        return new PlanPrinter(plan, types, functionManager, estimatedStatsAndCosts, session, Optional.empty()).toText(false, level);
    }

    public static String textLogicalPlan(
            LogicalPlanNode plan,
            TypeProvider types,
            FunctionManager functionManager,
            StatsAndCosts estimatedStatsAndCosts,
            Session session,
            int level,
            boolean verbose) {
        return textLogicalPlan(plan, types, functionManager, estimatedStatsAndCosts, session, Optional.empty(),level, verbose);
    }

    public static String textLogicalPlan(
            LogicalPlanNode plan,
            TypeProvider types,
            FunctionManager functionManager,
            StatsAndCosts estimatedStatsAndCosts,
            Session session,
            Optional<Map<PlanNodeId, PlanNodeStats>> stats,
            int level,
            boolean verbose)
    {
        return new PlanPrinter(plan, types, functionManager, estimatedStatsAndCosts, session, stats).toText(verbose, level);
    }

    private class Visitor
            extends PlanVisitor<Void, Void>
    {
        private final StatsAndCosts estimatedStatsAndCosts;
        private final Optional<Map<PlanNodeId, PlanNodeStats>> stats;
        public Visitor(StatsAndCosts estimatedStatsAndCosts, Optional<Map<PlanNodeId, PlanNodeStats>> stats) {
            this.estimatedStatsAndCosts = requireNonNull(estimatedStatsAndCosts, "estimatedStatsAndCosts is null");
            this.stats = requireNonNull(stats, "stats is null");
        }

        @Override
        public Void visitTableScan(TableScanNode node, Void context)
        {
            TableHandle table = node.getTable();
            NodeRepresentation nodeOutput;
            nodeOutput = addNode(node, "TableScan", format("[%s]", table));
            PlanNodeStats nodeStats = stats.map(s -> s.get(node.getId())).orElse(null);
            //printTableScanInfo(nodeOutput, node, nodeStats);
            return null;
        }

        @Override
        public Void visitJoin(JoinNode node, Void context)
        {
            List<String> joinExpressions = new ArrayList<>();
            for (JoinNode.EquiJoinClause clause : node.getCriteria()) {
                joinExpressions.add(JoinNodeUtils.toExpression(clause).toString());
            }
            node.getFilter().map(formatter::apply).ifPresent(joinExpressions::add);

            NodeRepresentation nodeOutput;
            if (node.isCrossJoin()) {
                checkState(joinExpressions.isEmpty());
                nodeOutput = addNode(node, "CrossJoin");
            }
            else {
                nodeOutput = addNode(node,
                        node.getType().getJoinLabel(),
                        format("[%s]%s", Joiner.on(" AND ").join(joinExpressions), formatHash(node.getLeftHashVariable(), node.getRightHashVariable())));
            }

            node.getDistributionType().ifPresent(distributionType -> nodeOutput.appendDetails("Distribution: %s", distributionType));
            node.getSortExpressionContext(functionManager)
                    .ifPresent(sortContext -> nodeOutput.appendDetails("SortExpression[%s]", formatter.apply(sortContext.getSortExpression())));
            node.getLeft().accept(this, context);
            node.getRight().accept(this, context);

            return null;
        }

        @Override
        public Void visitSemiJoin(SemiJoinNode node, Void context)
        {
            NodeRepresentation nodeOutput = addNode(node,
                    "SemiJoin",
                    format("[%s = %s]%s",
                            node.getSourceJoinVariable(),
                            node.getFilteringSourceJoinVariable(),
                            formatHash(node.getSourceHashVariable(), node.getFilteringSourceHashVariable())));
            node.getDistributionType().ifPresent(distributionType -> nodeOutput.appendDetailsLine("Distribution: %s", distributionType));
            node.getSource().accept(this, context);
            node.getFilteringSource().accept(this, context);

            return null;
        }

        @Override
        public Void visitLimit(LimitNode node, Void context)
        {
            addNode(node,
                    format("Limit%s", node.isPartial() ? "Partial" : ""),
                    format("[%s]", node.getCount()));
            return processChildren(node, context);
        }


        @Override
        public Void visitAggregation(AggregationNode node, Void context)
        {
            String type = "";
            if (node.getStep() != AggregationNode.Step.SINGLE) {
                type = format("(%s)", node.getStep().toString());
            }
            if (node.isStreamable()) {
                type = format("%s(STREAMING)", type);
            }
            String key = "";
            if (!node.getGroupingKeys().isEmpty()) {
                key = node.getGroupingKeys().toString();
            }

            NodeRepresentation nodeOutput = addNode(node,
                    format("Aggregate%s%s%s", type, key, formatHash(node.getHashVariable())));

            for (Map.Entry<VariableReferenceExpression, AggregationNode.Aggregation> entry : node.getAggregations().entrySet()) {
                nodeOutput.appendDetailsLine("%s := %s", entry.getKey(), formatAggregation(entry.getValue()));
            }

            return processChildren(node, context);
        }

        private String formatAggregation(AggregationNode.Aggregation aggregation)
        {
            StringBuilder builder = new StringBuilder();
            builder.append("\"");
            builder.append(functionManager.getFunctionMetadata(aggregation.getFunctionHandle()).getName());
            builder.append("\"");
            builder.append("(");
            if (aggregation.isDistinct()) {
                builder.append("DISTINCT ");
            }
            if (aggregation.getArguments().isEmpty()) {
                builder.append("*");
            }
            else {
                builder.append("(" + Joiner.on(",").join(aggregation.getArguments().stream().map(formatter::apply).collect(toImmutableList())) + ")");
            }
            builder.append(")");
            aggregation.getFilter().ifPresent(filter -> builder.append(" WHERE " + formatter.apply(filter)));
            aggregation.getOrderBy().ifPresent(orderingScheme -> builder.append(" ORDER BY " + orderingScheme.toString()));
            aggregation.getMask().ifPresent(mask -> builder.append(" (mask = " + mask + ")"));
            return builder.toString();
        }

        @Override
        public Void visitValues(ValuesNode node, Void context)
        {
            NodeRepresentation nodeOutput = addNode(node, "Values");
            for (List<RowExpression> row : node.getRows()) {
                nodeOutput.appendDetailsLine("(" + row.stream().map(formatter::apply).collect(Collectors.joining(", ")) + ")");
            }
            return null;
        }

        @Override
        public Void visitFilter(FilterNode node, Void context)
        {
            return visitScanFilterAndProjectInfo(node, Optional.of(node), Optional.empty(), context);
        }

        @Override
        public Void visitProject(ProjectNode node, Void context)
        {
            if (node.getSource() instanceof FilterNode) {
                return visitScanFilterAndProjectInfo(node, Optional.of((FilterNode) node.getSource()), Optional.of(node), context);
            }

            return visitScanFilterAndProjectInfo(node, Optional.empty(), Optional.of(node), context);
        }

        private Void visitScanFilterAndProjectInfo(
                LogicalPlanNode node,
                Optional<FilterNode> filterNode,
                Optional<ProjectNode> projectNode,
                Void context)
        {
            checkState(projectNode.isPresent() || filterNode.isPresent());

            LogicalPlanNode sourceNode;
            if (filterNode.isPresent()) {
                sourceNode = filterNode.get().getSource();
            }
            else {
                sourceNode = projectNode.get().getSource();
            }

            Optional<TableScanNode> scanNode;
            if (sourceNode instanceof TableScanNode) {
                scanNode = Optional.of((TableScanNode) sourceNode);
            }
            else {
                scanNode = Optional.empty();
            }

            String formatString = "[";
            String operatorName = "";
            List<Object> arguments = new LinkedList<>();

            if (scanNode.isPresent()) {
                operatorName += "Scan";
                formatString += "table = %s, ";
                TableHandle table = scanNode.get().getTable();
                arguments.add(table);
                /*
                if (stageExecutionStrategy.isPresent()) {
                    formatString += "grouped = %s, ";
                    arguments.add(stageExecutionStrategy.get().isScanGroupedExecution(scanNode.get().getId()));
                }
                 */
            }

            if (filterNode.isPresent()) {
                operatorName += "Filter";
                formatString += "filterPredicate = %s, ";
                arguments.add(formatter.apply(filterNode.get().getPredicate()));
            }

            if (formatString.length() > 1) {
                formatString = formatString.substring(0, formatString.length() - 2);
            }
            formatString += "]";

            if (projectNode.isPresent()) {
                operatorName += "Project";
            }

            List<PlanNodeId> allNodes = Stream.of(scanNode, filterNode, projectNode)
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .map(LogicalPlanNode::getId)
                    .collect(toList());

            NodeRepresentation nodeOutput = addNode(
                    node,
                    operatorName,
                    format(formatString, arguments.toArray(new Object[0])),
                    allNodes,
                    ImmutableList.of(sourceNode));

            if (projectNode.isPresent()) {
                printAssignments(nodeOutput, projectNode.get().getAssignments());
            }

            if (scanNode.isPresent()) {
                PlanNodeStats nodeStats = stats.map(s -> s.get(node.getId())).orElse(null);
                //printTableScanInfo(nodeOutput, scanNode.get(), nodeStats);
                return null;
            }

            sourceNode.accept(this, context);
            return null;
        }


        @Override
        public Void visitOutput(OutputNode node, Void context)
        {
            NodeRepresentation nodeOutput = addNode(node, "Output", format("[%s]", Joiner.on(", ").join(node.getColumnNames())));
            for (int i = 0; i < node.getColumnNames().size(); i++) {
                String name = node.getColumnNames().get(i);
                VariableReferenceExpression variable = node.getOutputVariables().get(i);
                if (!name.equals(variable.toString())) {
                    nodeOutput.appendDetailsLine("%s := %s", name, variable);
                }
            }
            return processChildren(node, context);
        }

        @Override
        public Void visitTopN(TopNNode node, Void context)
        {
            Iterable<String> keys = Iterables.transform(node.getOrderingScheme().getOrderByVariables(), input -> input + " " + node.getOrderingScheme().getOrdering(input));

            addNode(node,
                    format("TopN%s", node.getStep() == TopNNode.Step.PARTIAL ? "Partial" : ""),
                    format("[%s by (%s)]", node.getCount(), Joiner.on(", ").join(keys)));
            return processChildren(node, context);
        }

        @Override
        public Void visitSort(SortNode node, Void context)
        {
            Iterable<String> keys = Iterables.transform(node.getOrderingScheme().getOrderByVariables(), input -> input + " " + node.getOrderingScheme().getOrdering(input));

            addNode(node,
                    format("%sSort", node.isPartial() ? "Partial" : ""),
                    format("[%s]", Joiner.on(", ").join(keys)));

            return processChildren(node, context);
        }

        @Override
        public Void visitPlan(LogicalPlanNode node, Void context)
        {
            throw new UnsupportedOperationException("not yet implemented: " + node.getClass().getName());
        }

        private Void processChildren(LogicalPlanNode node, Void context)
        {
            for (LogicalPlanNode child : node.getSources()) {
                child.accept(this, context);
            }

            return null;
        }

        private void printAssignments(NodeRepresentation nodeOutput, Assignments assignments)
        {
            for (Map.Entry<VariableReferenceExpression, RowExpression> entry : assignments.getMap().entrySet()) {
                if (entry.getValue() instanceof VariableReferenceExpression && ((VariableReferenceExpression) entry.getValue()).getName().equals(entry.getKey().getName())) {
                    // skip identity assignments
                    continue;
                }
                nodeOutput.appendDetailsLine("%s := %s", entry.getKey(), formatter.apply(entry.getValue()));
            }
        }

        public NodeRepresentation addNode(LogicalPlanNode node, String name)
        {
            return addNode(node, name, "");
        }

        public NodeRepresentation addNode(LogicalPlanNode node, String name, String identifier)
        {
            return addNode(node, name, identifier, node.getSources());
        }

        public NodeRepresentation addNode(LogicalPlanNode node, String name, String identifier, List<LogicalPlanNode> children)
        {
            return addNode(node, name, identifier, ImmutableList.of(node.getId()), children);
        }

        public NodeRepresentation addNode(LogicalPlanNode rootNode, String name, String identifier, List<PlanNodeId> allNodes, List<LogicalPlanNode> children)
        {
            List<PlanNodeId> childrenIds = children.stream().map(LogicalPlanNode::getId).collect(toImmutableList());
            List<PlanNodeStatsEstimate> estimatedStats = allNodes.stream()
                    .map(nodeId -> estimatedStatsAndCosts.getStats().getOrDefault(nodeId, PlanNodeStatsEstimate.unknown()))
                    .collect(toList());
            List<PlanCostEstimate> estimatedCosts = allNodes.stream()
                    .map(nodeId -> estimatedStatsAndCosts.getCosts().getOrDefault(nodeId, PlanCostEstimate.unknown()))
                    .collect(toList());

            NodeRepresentation nodeOutput = new NodeRepresentation(
                    rootNode.getId(),
                    name,
                    rootNode.getClass().getSimpleName(),
                    identifier,
                    rootNode.getOutputVariables(),
                    stats.map(s -> s.get(rootNode.getId())),
                    estimatedStats,
                    estimatedCosts,
                    childrenIds);

            representation.addNode(nodeOutput);
            return nodeOutput;
        }

    }

    private static String formatHash(Optional<VariableReferenceExpression>... hashes)
    {
        List<VariableReferenceExpression> variables = stream(hashes)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(toList());

        if (variables.isEmpty()) {
            return "";
        }

        return "[" + Joiner.on(", ").join(variables) + "]";
    }
}