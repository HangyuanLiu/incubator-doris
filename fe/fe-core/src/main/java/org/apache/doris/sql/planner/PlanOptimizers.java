package org.apache.doris.sql.planner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.doris.sql.metadata.Metadata;
import org.apache.doris.sql.parser.SqlParser;
import org.apache.doris.sql.planner.cost.CostCalculator;
import org.apache.doris.sql.planner.cost.CostCalculatorWithEstimatedExchanges;
import org.apache.doris.sql.planner.cost.CostComparator;
import org.apache.doris.sql.planner.cost.StatsCalculator;
import org.apache.doris.sql.planner.cost.TaskCountEstimator;
import org.apache.doris.sql.planner.iterative.IterativeOptimizer;
import org.apache.doris.sql.planner.iterative.Rule;
import org.apache.doris.sql.planner.iterative.rule.CanonicalizeExpressions;
import org.apache.doris.sql.planner.iterative.rule.DetermineJoinDistributionType;
import org.apache.doris.sql.planner.iterative.rule.DetermineSemiJoinDistributionType;
import org.apache.doris.sql.planner.iterative.rule.EvaluateZeroLimit;
import org.apache.doris.sql.planner.iterative.rule.InlineProjections;
import org.apache.doris.sql.planner.iterative.rule.MergeFilters;
import org.apache.doris.sql.planner.iterative.rule.MergeLimitWithSort;
import org.apache.doris.sql.planner.iterative.rule.MergeLimitWithTopN;
import org.apache.doris.sql.planner.iterative.rule.MergeLimits;
import org.apache.doris.sql.planner.iterative.rule.PruneAggregationColumns;
import org.apache.doris.sql.planner.iterative.rule.PruneAggregationSourceColumns;
import org.apache.doris.sql.planner.iterative.rule.PruneCountAggregationOverScalar;
import org.apache.doris.sql.planner.iterative.rule.PruneCrossJoinColumns;
import org.apache.doris.sql.planner.iterative.rule.PruneFilterColumns;
import org.apache.doris.sql.planner.iterative.rule.PruneJoinChildrenColumns;
import org.apache.doris.sql.planner.iterative.rule.PruneJoinColumns;
import org.apache.doris.sql.planner.iterative.rule.PruneLimitColumns;
import org.apache.doris.sql.planner.iterative.rule.PruneProjectColumns;
import org.apache.doris.sql.planner.iterative.rule.PruneSemiJoinColumns;
import org.apache.doris.sql.planner.iterative.rule.PruneSemiJoinFilteringSourceColumns;
import org.apache.doris.sql.planner.iterative.rule.PruneTableScanColumns;
import org.apache.doris.sql.planner.iterative.rule.PruneTopNColumns;
import org.apache.doris.sql.planner.iterative.rule.PushAggregationThroughOuterJoin;
import org.apache.doris.sql.planner.iterative.rule.PushLimitThroughOuterJoin;
import org.apache.doris.sql.planner.iterative.rule.PushLimitThroughProject;
import org.apache.doris.sql.planner.iterative.rule.RemoveRedundantIdentityProjections;
import org.apache.doris.sql.planner.iterative.rule.RemoveTrivialFilters;
import org.apache.doris.sql.planner.iterative.rule.RemoveUnreferencedScalarApplyNodes;
import org.apache.doris.sql.planner.iterative.rule.RemoveUnreferencedScalarLateralNodes;
import org.apache.doris.sql.planner.iterative.rule.SimplifyCountOverConstant;
import org.apache.doris.sql.planner.iterative.rule.SimplifyExpressions;
import org.apache.doris.sql.planner.iterative.rule.SimplifyRowExpressions;
import org.apache.doris.sql.planner.iterative.rule.SingleDistinctAggregationToGroupBy;
import org.apache.doris.sql.planner.iterative.rule.TransformCorrelatedInPredicateToJoin;
import org.apache.doris.sql.planner.iterative.rule.TransformCorrelatedLateralJoinToJoin;
import org.apache.doris.sql.planner.iterative.rule.TransformCorrelatedScalarAggregationToJoin;
import org.apache.doris.sql.planner.iterative.rule.TransformCorrelatedScalarSubquery;
import org.apache.doris.sql.planner.iterative.rule.TransformCorrelatedSingleRowSubqueryToProject;
import org.apache.doris.sql.planner.iterative.rule.TransformExistsApplyToLateralNode;
import org.apache.doris.sql.planner.iterative.rule.TransformUncorrelatedInPredicateSubqueryToSemiJoin;
import org.apache.doris.sql.planner.iterative.rule.TransformUncorrelatedLateralToJoin;
import org.apache.doris.sql.planner.optimizations.AddExchanges;
import org.apache.doris.sql.planner.optimizations.CheckSubqueryNodesAreRewritten;
import org.apache.doris.sql.planner.optimizations.LimitPushDown;
import org.apache.doris.sql.planner.optimizations.PlanOptimizer;
import org.apache.doris.sql.planner.iterative.rule.TranslateExpressions;
import org.apache.doris.sql.planner.optimizations.PredicatePushDown;
import org.apache.doris.sql.planner.optimizations.PruneUnreferencedOutputs;
import org.apache.doris.sql.planner.optimizations.RowExpressionPredicatePushDown;
import org.apache.doris.sql.planner.optimizations.TransformQuantifiedComparisonApplyToLateralJoin;
import org.apache.doris.sql.planner.optimizations.UnaliasSymbolReferences;

import java.util.List;
import java.util.Set;

public class PlanOptimizers {
    private final List<PlanOptimizer> optimizers;
    private final RuleStatsRecorder ruleStats = new RuleStatsRecorder();

    public PlanOptimizers(Metadata metadata,
                          SqlParser sqlParser,
                          StatsCalculator statsCalculator,
                          CostCalculator estimatedExchangesCostCalculator,
                          CostComparator costComparator,
                          TaskCountEstimator taskCountEstimator) {

        Set<Rule<?>> predicatePushDownRules = ImmutableSet.of(
                new MergeFilters());

        //列裁剪
        Set<Rule<?>> columnPruningRules = ImmutableSet.of(
                new PruneAggregationColumns(),
                new PruneAggregationSourceColumns(),
                new PruneCrossJoinColumns(),
                new PruneFilterColumns(),
                new PruneJoinChildrenColumns(),
                new PruneJoinColumns(),
                //new PruneMarkDistinctColumns(),
                //new PruneOutputColumns(),
                new PruneProjectColumns(),
                new PruneSemiJoinColumns(),
                new PruneSemiJoinFilteringSourceColumns(),
                new PruneTopNColumns(),
                new PruneLimitColumns(),
                new PruneTableScanColumns()
        );

        //统一裁剪冗余project
        IterativeOptimizer inlineProjections = new IterativeOptimizer(
                ruleStats,
                statsCalculator,
                estimatedExchangesCostCalculator,
                ImmutableSet.of(
                        new InlineProjections(metadata.getFunctionManager()),
                        new RemoveRedundantIdentityProjections()));

        //表达式优化
        IterativeOptimizer simplifyOptimizer = new IterativeOptimizer(
                ruleStats,
                statsCalculator,
                estimatedExchangesCostCalculator,
                new SimplifyExpressions(metadata, sqlParser).rules());

        IterativeOptimizer simplifyRowExpressionOptimizer = new IterativeOptimizer(
                ruleStats,
                statsCalculator,
                estimatedExchangesCostCalculator,
                new SimplifyRowExpressions(metadata).rules());

        //谓词下推
        PlanOptimizer predicatePushDown = new PredicatePushDown(metadata, sqlParser);
        PlanOptimizer rowExpressionPredicatePushDown = new RowExpressionPredicatePushDown(metadata, sqlParser);


        ImmutableList.Builder<PlanOptimizer> builder = ImmutableList.builder();
        builder.add(
                // Clean up all the sugar in expressions, e.g. AtTimeZone, must be run before all the other optimizers
                /*
                new IterativeOptimizer(
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.<Rule<?>>builder()
                                .addAll(new DesugarLambdaExpression().rules())
                                .addAll(new DesugarAtTimeZone(metadata, sqlParser).rules())
                                .addAll(new DesugarCurrentUser().rules())
                                .addAll(new DesugarTryExpression().rules())
                                .addAll(new DesugarRowSubscript(metadata, sqlParser).rules())
                                .build()),
                 */
                new IterativeOptimizer(
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        new CanonicalizeExpressions().rules()),
                new IterativeOptimizer(
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.of(new EvaluateZeroLimit())),
                new IterativeOptimizer(
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.<Rule<?>>builder()
                                .addAll(predicatePushDownRules)
                                .addAll(columnPruningRules)
                                .addAll(ImmutableSet.of(
                                        new RemoveRedundantIdentityProjections(),
                                        new PushLimitThroughProject(),
                                        new MergeLimits(),
                                        new MergeLimitWithSort(),
                                        new MergeLimitWithTopN(),
                                        //new PushLimitThroughMarkDistinct(),
                                        new PushLimitThroughOuterJoin(),
                                        //new PushLimitThroughSemiJoin(),
                                        new RemoveTrivialFilters(),
                                        //new ImplementFilteredAggregations(),
                                        new SingleDistinctAggregationToGroupBy(),
                                        //new MultipleDistinctAggregationToMarkDistinct(),
                                        //new MergeLimitWithDistinct(),
                                        new PruneCountAggregationOverScalar(metadata.getFunctionManager())
                                        //new PruneOrderByInAggregation(metadata.getFunctionManager())
                                    )
                                )
                                .build()),
                simplifyOptimizer,
                new UnaliasSymbolReferences(metadata.getFunctionManager()),
                new IterativeOptimizer(
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.of(new RemoveRedundantIdentityProjections())),
                //new SetFlatteningOptimizer(),
                //new ImplementIntersectAndExceptAsUnion(metadata.getFunctionManager()),
                new LimitPushDown(), // Run the LimitPushDown after flattening set operators to make it easier to do the set flattening
                new PruneUnreferencedOutputs(),
                inlineProjections,
                new IterativeOptimizer(
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        columnPruningRules),
                new IterativeOptimizer(
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.of(new TransformExistsApplyToLateralNode(metadata.getFunctionManager()))),
                new TransformQuantifiedComparisonApplyToLateralJoin(metadata.getFunctionManager()),
                new IterativeOptimizer(
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.of(
                                new RemoveUnreferencedScalarLateralNodes(),
                                new TransformUncorrelatedLateralToJoin(),
                                new TransformUncorrelatedInPredicateSubqueryToSemiJoin(),
                                new TransformCorrelatedScalarAggregationToJoin(metadata.getFunctionManager()),
                                new TransformCorrelatedLateralJoinToJoin())),
                new IterativeOptimizer(
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.of(
                            new RemoveUnreferencedScalarApplyNodes(),
                            new TransformCorrelatedInPredicateToJoin(metadata.getFunctionManager()), // must be run after PruneUnreferencedOutputs
                            new TransformCorrelatedScalarSubquery(), // must be run after TransformCorrelatedScalarAggregationToJoin
                            new TransformCorrelatedLateralJoinToJoin())),
                new IterativeOptimizer(ruleStats, statsCalculator, estimatedExchangesCostCalculator,
                     ImmutableSet.of(
                        new InlineProjections(metadata.getFunctionManager()),
                        new RemoveRedundantIdentityProjections(),
                        new TransformCorrelatedSingleRowSubqueryToProject()
                     )),
                new CheckSubqueryNodesAreRewritten(),
                predicatePushDown,
                /*
                new IterativeOptimizer(
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        new PickTableLayout(metadata, sqlParser).rules()),
                 */
                new PruneUnreferencedOutputs(),
                new IterativeOptimizer(
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.of(
                                new RemoveRedundantIdentityProjections(),
                                new PushAggregationThroughOuterJoin(metadata.getFunctionManager()))),
                inlineProjections,
                simplifyOptimizer, // Re-run the SimplifyExpressions to simplify any recomposed expressions from other optimizations
                //projectionPushDown,
                new UnaliasSymbolReferences(metadata.getFunctionManager()), // Run again because predicate pushdown and projection pushdown might add more projections
                new PruneUnreferencedOutputs(), // Make sure to run this before index join. Filtered projections may not have all the columns.
                //new IndexJoinOptimizer(metadata), // Run this after projections and filters have been fully simplified and pushed down
                new IterativeOptimizer(
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.of(new SimplifyCountOverConstant(metadata.getFunctionManager()))),
                new LimitPushDown(), // Run LimitPushDown before WindowFilterPushDown
                /*
                new WindowFilterPushDown(metadata), // This must run after PredicatePushDown and LimitPushDown so that it squashes any successive filter nodes and limits
                new IterativeOptimizer(
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.<Rule<?>>builder()
                                // add UnaliasSymbolReferences when it's ported
                                .add(new RemoveRedundantIdentityProjections())
                                .addAll(GatherAndMergeWindows.rules())
                                .build()),
                 */
                inlineProjections,
                new PruneUnreferencedOutputs(), // Make sure to run this at the end to help clean the plan for logging/execution and not remove info that other optimizers might need at an earlier point
                new IterativeOptimizer(
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.of(new RemoveRedundantIdentityProjections())),
                /*
                new IterativeOptimizer(
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.of(new EliminateCrossJoins())), // This can pull up Filter and Project nodes from between Joins, so we need to push them down again
                 */
                predicatePushDown,
                simplifyOptimizer); // Should be always run after PredicatePushDown
                /*
                new IterativeOptimizer(
                    ruleStats,
                    statsCalculator,
                    estimatedExchangesCostCalculator,
                    new PickTableLayout(metadata, sqlParser).rules()));
                 */

        // TODO: move this before optimization if possible!!
        // Replace all expressions with row expressions
        builder.add(new IterativeOptimizer(
                ruleStats,
                statsCalculator,
                estimatedExchangesCostCalculator,
                new TranslateExpressions(metadata, sqlParser).rules()));
        // After this point, all planNodes should not contain OriginalExpression

        // Pass a supplier so that we pickup connector optimizers that are installed later
        builder.add(
                //projectionPushDown,
                new PruneUnreferencedOutputs());
        /*
        builder.add(new IterativeOptimizer(
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.of(new RemoveRedundantIdentityProjections())),
                new PushdownSubfields(metadata));
        */
        builder.add(rowExpressionPredicatePushDown); // Run predicate push down one more time in case we can leverage new information from layouts' effective predicate
        builder.add(simplifyRowExpressionOptimizer); // Should be always run after PredicatePushDown
        /*
        builder.add(new IterativeOptimizer(
                // Because ReorderJoins runs only once,
                // PredicatePushDown, PruneUnreferenedOutputpus and RemoveRedundantIdentityProjections
                // need to run beforehand in order to produce an optimal join order
                // It also needs to run after EliminateCrossJoins so that its chosen order doesn't get undone.
                ruleStats,
                statsCalculator,
                estimatedExchangesCostCalculator,
                ImmutableSet.of(new ReorderJoins(costComparator, metadata))));
        */
        //builder.add(new OptimizeMixedDistinctAggregations(metadata));
        /*
        builder.add(new IterativeOptimizer(
                ruleStats,
                statsCalculator,
                estimatedExchangesCostCalculator,
                ImmutableSet.of(
                        new CreatePartialTopN(),
                        new PushTopNThroughUnion())));
        */

        //判断JOIN类型
        builder.add((new IterativeOptimizer(
                ruleStats,
                statsCalculator,
                estimatedExchangesCostCalculator,
                ImmutableSet.of(
                        new DetermineJoinDistributionType(costComparator, taskCountEstimator), // Must run before AddExchanges
                        // Must run before AddExchanges and after ReplicateSemiJoinInDelete
                        // to avoid temporarily having an invalid plan
                        new DetermineSemiJoinDistributionType(costComparator, taskCountEstimator)))));

        //builder.add(new AddExchanges(metadata));
        this.optimizers = builder.build();
    }
    public List<PlanOptimizer> get()
    {
        return optimizers;
    }
}
