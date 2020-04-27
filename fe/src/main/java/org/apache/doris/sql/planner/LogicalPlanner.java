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
import org.apache.doris.sql.analyzer.Analysis;
import org.apache.doris.sql.analyzer.Field;
import org.apache.doris.sql.analyzer.RelationType;
import org.apache.doris.sql.metadata.Metadata;
import org.apache.doris.sql.metadata.Session;
import org.apache.doris.sql.metadata.WarningCollector;
import org.apache.doris.sql.parser.SqlParser;
import org.apache.doris.sql.planner.plan.LimitNode;
import org.apache.doris.sql.planner.plan.PlanNode;
import org.apache.doris.sql.tree.Query;
import org.apache.doris.sql.tree.Statement;

import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class LogicalPlanner
{
    public enum Stage
    {
        CREATED, OPTIMIZED, OPTIMIZED_AND_VALIDATED
    }

    private final boolean explain;
    private final PlanNodeIdAllocator idAllocator;
    private final Session session;
    private final List<PlanOptimizer> planOptimizers;
    private final PlanSanityChecker planSanityChecker;
    private final PlanVariableAllocator variableAllocator = new PlanVariableAllocator();
    private final Metadata metadata;
    private final SqlParser sqlParser;
    private final StatisticsAggregationPlanner statisticsAggregationPlanner;
    private final StatsCalculator statsCalculator;
    private final CostCalculator costCalculator;
    private final WarningCollector warningCollector;

    public LogicalPlanner(
            boolean explain,
            Session session,
            List<PlanOptimizer> planOptimizers,
            PlanNodeIdAllocator idAllocator,
            Metadata metadata,
            SqlParser sqlParser,
            StatsCalculator statsCalculator,
            CostCalculator costCalculator,
            WarningCollector warningCollector)
    {
        this(explain, session, planOptimizers, DISTRIBUTED_PLAN_SANITY_CHECKER, idAllocator, metadata, sqlParser, statsCalculator, costCalculator, warningCollector);
    }

    public LogicalPlanner(
            boolean explain,
            Session session,
            List<PlanOptimizer> planOptimizers,
            PlanSanityChecker planSanityChecker,
            PlanNodeIdAllocator idAllocator,
            Metadata metadata,
            SqlParser sqlParser,
            StatsCalculator statsCalculator,
            CostCalculator costCalculator,
            WarningCollector warningCollector)
    {
        this.explain = explain;
        this.session = requireNonNull(session, "session is null");
        this.planOptimizers = requireNonNull(planOptimizers, "planOptimizers is null");
        this.planSanityChecker = requireNonNull(planSanityChecker, "planSanityChecker is null");
        this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
        this.statisticsAggregationPlanner = new StatisticsAggregationPlanner(variableAllocator, metadata);
        this.statsCalculator = requireNonNull(statsCalculator, "statsCalculator is null");
        this.costCalculator = requireNonNull(costCalculator, "costCalculator is null");
        this.warningCollector = requireNonNull(warningCollector, "warningCollector is null");
    }

    public Plan plan(Analysis analysis)
    {
        return plan(analysis, Stage.OPTIMIZED_AND_VALIDATED);
    }

    public Plan plan(Analysis analysis, Stage stage)
    {
        PlanNode root = planStatement(analysis, analysis.getStatement());

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

        TypeProvider types = variableAllocator.getTypes();
        return new Plan(root, types, computeStats(root, types));
    }

    private StatsAndCosts computeStats(PlanNode root, TypeProvider types)
    {
        if (explain || isPrintStatsForNonJoinQuery(session) ||
                PlanNodeSearcher.searchFrom(root).where(node ->
                    (node instanceof JoinNode) || (node instanceof SemiJoinNode)).matches()) {
            StatsProvider statsProvider = new CachingStatsProvider(statsCalculator, session, types);
            CostProvider costProvider = new CachingCostProvider(costCalculator, statsProvider, Optional.empty(), session);
            return StatsAndCosts.create(root, statsProvider, costProvider);
        }
        return StatsAndCosts.empty();
    }

    public PlanNode planStatement(Analysis analysis, Statement statement)
    {
        if (statement instanceof CreateTableAsSelect && analysis.isCreateTableAsSelectNoOp()) {
            checkState(analysis.getCreateTableDestination().isPresent(), "Table destination is missing");
            VariableReferenceExpression variable = variableAllocator.newVariable("rows", BIGINT);
            PlanNode source = new ValuesNode(
                    idAllocator.getNextId(),
                    ImmutableList.of(variable),
                    ImmutableList.of(ImmutableList.of(constant(0L, BIGINT))));
            return new OutputNode(idAllocator.getNextId(), source, ImmutableList.of("rows"), ImmutableList.of(variable));
        }
        return createOutputPlan(planStatementWithoutOutput(analysis, statement), analysis);
    }

    private RelationPlan planStatementWithoutOutput(Analysis analysis, Statement statement)
    {
        if (statement instanceof CreateTableAsSelect) {
            if (analysis.isCreateTableAsSelectNoOp()) {
                throw new PrestoException(NOT_SUPPORTED, "CREATE TABLE IF NOT EXISTS is not supported in this context " + statement.getClass().getSimpleName());
            }
            return createTableCreationPlan(analysis, ((CreateTableAsSelect) statement).getQuery());
        }
        else if (statement instanceof Analyze) {
            return createAnalyzePlan(analysis, (Analyze) statement);
        }
        else if (statement instanceof Insert) {
            checkState(analysis.getInsert().isPresent(), "Insert handle is missing");
            return createInsertPlan(analysis, (Insert) statement);
        }
        else if (statement instanceof Delete) {
            return createDeletePlan(analysis, (Delete) statement);
        }
        else if (statement instanceof Query) {
            return createRelationPlan(analysis, (Query) statement);
        }
        else if (statement instanceof Explain && ((Explain) statement).isAnalyze()) {
            return createExplainAnalyzePlan(analysis, (Explain) statement);
        }
        else {
            throw new PrestoException(NOT_SUPPORTED, "Unsupported statement type " + statement.getClass().getSimpleName());
        }
    }

    private RelationPlan createExplainAnalyzePlan(Analysis analysis, Explain statement)
    {
        RelationPlan underlyingPlan = planStatementWithoutOutput(analysis, statement.getStatement());
        PlanNode root = underlyingPlan.getRoot();
        Scope scope = analysis.getScope(statement);
        VariableReferenceExpression outputVariable = variableAllocator.newVariable(scope.getRelationType().getFieldByIndex(0));
        root = new ExplainAnalyzeNode(idAllocator.getNextId(), root, outputVariable, statement.isVerbose());
        return new RelationPlan(root, scope, ImmutableList.of(outputVariable));
    }

    private RelationPlan createAnalyzePlan(Analysis analysis, Analyze analyzeStatement)
    {
        TableHandle targetTable = analysis.getAnalyzeTarget().get();

        // Plan table scan
        Map<String, ColumnHandle> columnHandles = metadata.getColumnHandles(session, targetTable);
        ImmutableList.Builder<VariableReferenceExpression> tableScanOutputsBuilder = ImmutableList.builder();
        ImmutableMap.Builder<VariableReferenceExpression, ColumnHandle> variableToColumnHandle = ImmutableMap.builder();
        ImmutableMap.Builder<String, VariableReferenceExpression> columnNameToVariable = ImmutableMap.builder();
        TableMetadata tableMetadata = metadata.getTableMetadata(session, targetTable);
        for (ColumnMetadata column : tableMetadata.getColumns()) {
            VariableReferenceExpression variable = variableAllocator.newVariable(column.getName(), column.getType());
            tableScanOutputsBuilder.add(variable);
            variableToColumnHandle.put(variable, columnHandles.get(column.getName()));
            columnNameToVariable.put(column.getName(), variable);
        }

        List<VariableReferenceExpression> tableScanOutputs = tableScanOutputsBuilder.build();
        TableStatisticsMetadata tableStatisticsMetadata = metadata.getStatisticsCollectionMetadata(
                session,
                targetTable.getConnectorId().getCatalogName(),
                tableMetadata.getMetadata());

        TableStatisticAggregation tableStatisticAggregation = statisticsAggregationPlanner.createStatisticsAggregation(tableStatisticsMetadata, columnNameToVariable.build());
        StatisticAggregations statisticAggregations = tableStatisticAggregation.getAggregations();

        PlanNode planNode = new StatisticsWriterNode(
                idAllocator.getNextId(),
                new AggregationNode(
                        idAllocator.getNextId(),
                        new TableScanNode(idAllocator.getNextId(), targetTable, tableScanOutputs, variableToColumnHandle.build(), TupleDomain.all(), TupleDomain.all()),
                        statisticAggregations.getAggregations(),
                        singleGroupingSet(statisticAggregations.getGroupingVariables()),
                        ImmutableList.of(),
                        AggregationNode.Step.SINGLE,
                        Optional.empty(),
                        Optional.empty()),
                targetTable,
                variableAllocator.newVariable("rows", BIGINT),
                tableStatisticsMetadata.getTableStatistics().contains(ROW_COUNT),
                tableStatisticAggregation.getDescriptor());
        return new RelationPlan(planNode, analysis.getScope(analyzeStatement), planNode.getOutputVariables());
    }

    private RelationPlan createTableCreationPlan(Analysis analysis, Query query)
    {
        QualifiedObjectName destination = analysis.getCreateTableDestination().get();

        RelationPlan plan = createRelationPlan(analysis, query);

        ConnectorTableMetadata tableMetadata = createTableMetadata(
                destination,
                getOutputTableColumns(plan, analysis.getColumnAliases()),
                analysis.getCreateTableProperties(),
                analysis.getParameters(),
                analysis.getCreateTableComment());
        Optional<NewTableLayout> newTableLayout = metadata.getNewTableLayout(session, destination.getCatalogName(), tableMetadata);
        Optional<NewTableLayout> preferredShuffleLayout = metadata.getPreferredShuffleLayoutForNewTable(session, destination.getCatalogName(), tableMetadata);

        List<String> columnNames = tableMetadata.getColumns().stream()
                .filter(column -> !column.isHidden())
                .map(ColumnMetadata::getName)
                .collect(toImmutableList());

        TableStatisticsMetadata statisticsMetadata = metadata.getStatisticsCollectionMetadataForWrite(session, destination.getCatalogName(), tableMetadata);

        return createTableWriterPlan(
                analysis,
                plan,
                new CreateName(new ConnectorId(destination.getCatalogName()), tableMetadata, newTableLayout),
                columnNames,
                newTableLayout,
                preferredShuffleLayout,
                statisticsMetadata);
    }

    private RelationPlan createInsertPlan(Analysis analysis, Insert insertStatement)
    {
        Analysis.Insert insert = analysis.getInsert().get();

        TableMetadata tableMetadata = metadata.getTableMetadata(session, insert.getTarget());

        List<ColumnMetadata> visibleTableColumns = tableMetadata.getColumns().stream()
                .filter(column -> !column.isHidden())
                .collect(toImmutableList());
        List<String> visibleTableColumnNames = visibleTableColumns.stream()
                .map(ColumnMetadata::getName)
                .collect(toImmutableList());

        RelationPlan plan = createRelationPlan(analysis, insertStatement.getQuery());

        Map<String, ColumnHandle> columns = metadata.getColumnHandles(session, insert.getTarget());
        Assignments.Builder assignments = Assignments.builder();
        for (ColumnMetadata column : tableMetadata.getColumns()) {
            if (column.isHidden()) {
                continue;
            }
            VariableReferenceExpression output = variableAllocator.newVariable(column.getName(), column.getType());
            int index = insert.getColumns().indexOf(columns.get(column.getName()));
            if (index < 0) {
                Expression cast = new Cast(new NullLiteral(), column.getType().getTypeSignature().toString());
                assignments.put(output, castToRowExpression(cast));
            }
            else {
                VariableReferenceExpression input = plan.getVariable(index);
                Type tableType = column.getType();
                Type queryType = input.getType();

                if (queryType.equals(tableType) || metadata.getTypeManager().isTypeOnlyCoercion(queryType, tableType)) {
                    assignments.put(output, castToRowExpression(new SymbolReference(input.getName())));
                }
                else {
                    Expression cast = new Cast(new SymbolReference(input.getName()), tableType.getTypeSignature().toString());
                    assignments.put(output, castToRowExpression(cast));
                }
            }
        }
        ProjectNode projectNode = new ProjectNode(idAllocator.getNextId(), plan.getRoot(), assignments.build());

        List<Field> fields = visibleTableColumns.stream()
                .map(column -> Field.newUnqualified(column.getName(), column.getType()))
                .collect(toImmutableList());
        Scope scope = Scope.builder().withRelationType(RelationId.anonymous(), new RelationType(fields)).build();

        plan = new RelationPlan(projectNode, scope, projectNode.getOutputVariables());

        Optional<NewTableLayout> newTableLayout = metadata.getInsertLayout(session, insert.getTarget());
        Optional<NewTableLayout> preferredShuffleLayout = metadata.getPreferredShuffleLayoutForInsert(session, insert.getTarget());

        String catalogName = insert.getTarget().getConnectorId().getCatalogName();
        TableStatisticsMetadata statisticsMetadata = metadata.getStatisticsCollectionMetadataForWrite(session, catalogName, tableMetadata.getMetadata());

        return createTableWriterPlan(
                analysis,
                plan,
                new InsertReference(insert.getTarget(), metadata.getTableMetadata(session, insert.getTarget()).getTable()),
                visibleTableColumnNames,
                newTableLayout,
                preferredShuffleLayout,
                statisticsMetadata);
    }

    private RelationPlan createTableWriterPlan(
            Analysis analysis,
            RelationPlan plan,
            WriterTarget target,
            List<String> columnNames,
            Optional<NewTableLayout> writeTableLayout,
            Optional<NewTableLayout> preferredShuffleLayout,
            TableStatisticsMetadata statisticsMetadata)
    {
        verify(!(writeTableLayout.isPresent() && preferredShuffleLayout.isPresent()), "writeTableLayout and preferredShuffleLayout cannot both exist");

        PlanNode source = plan.getRoot();

        if (!analysis.isCreateTableAsSelectWithData()) {
            source = new LimitNode(idAllocator.getNextId(), source, 0L, FINAL);
        }

        List<VariableReferenceExpression> variables = plan.getFieldMappings();
        Optional<PartitioningScheme> tablePartitioningScheme = getPartitioningSchemeForTableWrite(writeTableLayout, columnNames, variables);
        Optional<PartitioningScheme> preferredShufflePartitioningScheme = getPartitioningSchemeForTableWrite(preferredShuffleLayout, columnNames, variables);

        if (!statisticsMetadata.isEmpty()) {
            verify(columnNames.size() == variables.size(), "columnNames.size() != variables.size(): %s and %s", columnNames, variables);
            Map<String, VariableReferenceExpression> columnToVariableMap = zip(columnNames.stream(), plan.getFieldMappings().stream(), SimpleImmutableEntry::new)
                    .collect(toImmutableMap(Entry::getKey, Entry::getValue));

            TableStatisticAggregation result = statisticsAggregationPlanner.createStatisticsAggregation(statisticsMetadata, columnToVariableMap);

            StatisticAggregations.Parts aggregations = result.getAggregations().splitIntoPartialAndFinal(variableAllocator, metadata.getFunctionManager());

            TableFinishNode commitNode = new TableFinishNode(
                    idAllocator.getNextId(),
                    new TableWriterNode(
                            idAllocator.getNextId(),
                            source,
                            Optional.of(target),
                            variableAllocator.newVariable("rows", BIGINT),
                            variableAllocator.newVariable("fragments", VARBINARY),
                            variableAllocator.newVariable("commitcontext", VARBINARY),
                            plan.getFieldMappings(),
                            columnNames,
                            tablePartitioningScheme,
                            preferredShufflePartitioningScheme,
                            // partial aggregation is run within the TableWriteOperator to calculate the statistics for
                            // the data consumed by the TableWriteOperator
                            Optional.of(aggregations.getPartialAggregation())),
                    Optional.of(target),
                    variableAllocator.newVariable("rows", BIGINT),
                    // final aggregation is run within the TableFinishOperator to summarize collected statistics
                    // by the partial aggregation from all of the writer nodes
                    Optional.of(aggregations.getFinalAggregation()),
                    Optional.of(result.getDescriptor()));

            return new RelationPlan(commitNode, analysis.getRootScope(), commitNode.getOutputVariables());
        }

        TableFinishNode commitNode = new TableFinishNode(
                idAllocator.getNextId(),
                new TableWriterNode(
                        idAllocator.getNextId(),
                        source,
                        Optional.of(target),
                        variableAllocator.newVariable("rows", BIGINT),
                        variableAllocator.newVariable("fragments", VARBINARY),
                        variableAllocator.newVariable("commitcontext", VARBINARY),
                        plan.getFieldMappings(),
                        columnNames,
                        tablePartitioningScheme,
                        preferredShufflePartitioningScheme,
                        Optional.empty()),
                Optional.of(target),
                variableAllocator.newVariable("rows", BIGINT),
                Optional.empty(),
                Optional.empty());
        return new RelationPlan(commitNode, analysis.getRootScope(), commitNode.getOutputVariables());
    }

    private RelationPlan createDeletePlan(Analysis analysis, Delete node)
    {
        DeleteNode deleteNode = new QueryPlanner(analysis, variableAllocator, idAllocator, buildLambdaDeclarationToVariableMap(analysis, variableAllocator), metadata, session)
                .plan(node);

        TableHandle handle = analysis.getTableHandle(node.getTable());
        DeleteHandle deleteHandle = new DeleteHandle(handle, metadata.getTableMetadata(session, handle).getTable());
        TableFinishNode commitNode = new TableFinishNode(
                idAllocator.getNextId(),
                deleteNode,
                Optional.of(deleteHandle),
                variableAllocator.newVariable("rows", BIGINT),
                Optional.empty(),
                Optional.empty());

        return new RelationPlan(commitNode, analysis.getScope(node), commitNode.getOutputVariables());
    }

    private PlanNode createOutputPlan(RelationPlan plan, Analysis analysis)
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
        return new RelationPlanner(analysis, variableAllocator, idAllocator, buildLambdaDeclarationToVariableMap(analysis, variableAllocator), metadata, session)
                .process(query, null);
    }

    private ConnectorTableMetadata createTableMetadata(QualifiedObjectName table, List<ColumnMetadata> columns, Map<String, Expression> propertyExpressions, List<Expression> parameters, Optional<String> comment)
    {
        ConnectorId connectorId = metadata.getCatalogHandle(session, table.getCatalogName())
                .orElseThrow(() -> new PrestoException(NOT_FOUND, "Catalog does not exist: " + table.getCatalogName()));

        Map<String, Object> properties = metadata.getTablePropertyManager().getProperties(
                connectorId,
                table.getCatalogName(),
                propertyExpressions,
                session,
                metadata,
                parameters);

        return new ConnectorTableMetadata(table.asSchemaTableName(), columns, properties, comment);
    }

    private static List<ColumnMetadata> getOutputTableColumns(RelationPlan plan, Optional<List<Identifier>> columnAliases)
    {
        ImmutableList.Builder<ColumnMetadata> columns = ImmutableList.builder();
        int aliasPosition = 0;
        for (Field field : plan.getDescriptor().getVisibleFields()) {
            String columnName = columnAliases.isPresent() ? columnAliases.get().get(aliasPosition).getValue() : field.getName().get();
            columns.add(new ColumnMetadata(columnName, field.getType()));
            aliasPosition++;
        }
        return columns.build();
    }

    private static Map<NodeRef<LambdaArgumentDeclaration>, VariableReferenceExpression> buildLambdaDeclarationToVariableMap(Analysis analysis, PlanVariableAllocator variableAllocator)
    {
        Map<NodeRef<LambdaArgumentDeclaration>, VariableReferenceExpression> resultMap = new LinkedHashMap<>();
        for (Entry<NodeRef<Expression>, Type> entry : analysis.getTypes().entrySet()) {
            if (!(entry.getKey().getNode() instanceof LambdaArgumentDeclaration)) {
                continue;
            }
            NodeRef<LambdaArgumentDeclaration> lambdaArgumentDeclaration = NodeRef.of((LambdaArgumentDeclaration) entry.getKey().getNode());
            if (resultMap.containsKey(lambdaArgumentDeclaration)) {
                continue;
            }
            resultMap.put(lambdaArgumentDeclaration, variableAllocator.newVariable(lambdaArgumentDeclaration.getNode(), entry.getValue()));
        }
        return resultMap;
    }

    private static Optional<PartitioningScheme> getPartitioningSchemeForTableWrite(Optional<NewTableLayout> tableLayout, List<String> columnNames, List<VariableReferenceExpression> variables)
    {
        // todo this should be checked in analysis
        tableLayout.ifPresent(layout -> {
            if (!ImmutableSet.copyOf(columnNames).containsAll(layout.getPartitionColumns())) {
                throw new PrestoException(NOT_SUPPORTED, "INSERT must write all distribution columns: " + layout.getPartitionColumns());
            }
        });

        Optional<PartitioningScheme> partitioningScheme = Optional.empty();
        if (tableLayout.isPresent()) {
            List<VariableReferenceExpression> partitionFunctionArguments = new ArrayList<>();
            tableLayout.get().getPartitionColumns().stream()
                    .mapToInt(columnNames::indexOf)
                    .mapToObj(variables::get)
                    .forEach(partitionFunctionArguments::add);

            List<VariableReferenceExpression> outputLayout = new ArrayList<>(variables);

            partitioningScheme = Optional.of(new PartitioningScheme(
                    Partitioning.create(tableLayout.get().getPartitioning(), partitionFunctionArguments),
                    outputLayout));
        }
        return partitioningScheme;
    }
}
